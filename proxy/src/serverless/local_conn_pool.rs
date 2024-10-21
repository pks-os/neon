//! Manages the pool of connections between local_proxy and postgres.
//!
//! The pool is keyed by database and role_name, and can contain multiple connections
//! shared between users.
//!
//! The pool manages the pg_session_jwt extension used for authorizing
//! requests in the db.
//!
//! The first time a db/role pair is seen, local_proxy attempts to install the extension
//! and grant usage to the role on the given schema.

use std::collections::HashMap;
use std::pin::pin;
use std::sync::{Arc, Weak};
use std::task::{ready, Poll};
use std::time::Duration;

use futures::future::poll_fn;
use futures::Future;
use indexmap::IndexMap;
use jose_jwk::jose_b64::base64ct::{Base64UrlUnpadded, Encoding};
use p256::ecdsa::{Signature, SigningKey};
use parking_lot::RwLock;
use serde_json::value::RawValue;
use signature::Signer;
use tokio::time::Instant;
use tokio_postgres::tls::NoTlsStream;
use tokio_postgres::types::ToSql;
use tokio_postgres::{AsyncMessage, ReadyForQueryStatus, Socket};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, info_span, warn, Instrument, Span};

use super::backend::HttpConnError;
use super::conn_pool_lib::{ClientInnerExt, ConnInfo};
use crate::context::RequestMonitoring;
use crate::control_plane::messages::{ColdStartInfo, MetricsAuxInfo};
use crate::metrics::Metrics;
use crate::usage_metrics::{Ids, MetricCounter, USAGE_METRICS};
use crate::{DbName, RoleName};

pub(crate) const EXT_NAME: &str = "pg_session_jwt";
pub(crate) const EXT_VERSION: &str = "0.1.2";
pub(crate) const EXT_SCHEMA: &str = "auth";

struct ConnPoolEntry<C: ClientInnerExt> {
    conn: ClientInner<C>,
    _last_access: std::time::Instant,
}

// Per-endpoint connection pool, (dbname, username) -> DbUserConnPool
// Number of open connections is limited by the `max_conns_per_endpoint`.
pub(crate) struct EndpointConnPool<C: ClientInnerExt> {
    pools: HashMap<(DbName, RoleName), DbUserConnPool<C>>,
    total_conns: usize,
    max_conns: usize,
    global_pool_size_max_conns: usize,
}

impl<C: ClientInnerExt> EndpointConnPool<C> {
    fn get_conn_entry(&mut self, db_user: (DbName, RoleName)) -> Option<ConnPoolEntry<C>> {
        let Self {
            pools, total_conns, ..
        } = self;
        pools
            .get_mut(&db_user)
            .and_then(|pool_entries| pool_entries.get_conn_entry(total_conns))
    }

    fn remove_client(&mut self, db_user: (DbName, RoleName), conn_id: uuid::Uuid) -> bool {
        let Self {
            pools, total_conns, ..
        } = self;
        if let Some(pool) = pools.get_mut(&db_user) {
            let old_len = pool.conns.len();
            pool.conns.retain(|conn| conn.conn.conn_id != conn_id);
            let new_len = pool.conns.len();
            let removed = old_len - new_len;
            if removed > 0 {
                Metrics::get()
                    .proxy
                    .http_pool_opened_connections
                    .get_metric()
                    .dec_by(removed as i64);
            }
            *total_conns -= removed;
            removed > 0
        } else {
            false
        }
    }

    fn put(pool: &RwLock<Self>, conn_info: &ConnInfo, client: ClientInner<C>) {
        let conn_id = client.conn_id;

        if client.is_closed() {
            info!(%conn_id, "local_pool: throwing away connection '{conn_info}' because connection is closed");
            return;
        }
        let global_max_conn = pool.read().global_pool_size_max_conns;
        if pool.read().total_conns >= global_max_conn {
            info!(%conn_id, "local_pool: throwing away connection '{conn_info}' because pool is full");
            return;
        }

        // return connection to the pool
        let mut returned = false;
        let mut per_db_size = 0;
        let total_conns = {
            let mut pool = pool.write();

            if pool.total_conns < pool.max_conns {
                let pool_entries = pool.pools.entry(conn_info.db_and_user()).or_default();
                pool_entries.conns.push(ConnPoolEntry {
                    conn: client,
                    _last_access: std::time::Instant::now(),
                });

                returned = true;
                per_db_size = pool_entries.conns.len();

                pool.total_conns += 1;
                Metrics::get()
                    .proxy
                    .http_pool_opened_connections
                    .get_metric()
                    .inc();
            }

            pool.total_conns
        };

        // do logging outside of the mutex
        if returned {
            info!(%conn_id, "local_pool: returning connection '{conn_info}' back to the pool, total_conns={total_conns}, for this (db, user)={per_db_size}");
        } else {
            info!(%conn_id, "local_pool: throwing away connection '{conn_info}' because pool is full, total_conns={total_conns}");
        }
    }
}

impl<C: ClientInnerExt> Drop for EndpointConnPool<C> {
    fn drop(&mut self) {
        if self.total_conns > 0 {
            Metrics::get()
                .proxy
                .http_pool_opened_connections
                .get_metric()
                .dec_by(self.total_conns as i64);
        }
    }
}

pub(crate) struct DbUserConnPool<C: ClientInnerExt> {
    conns: Vec<ConnPoolEntry<C>>,

    // true if we have definitely installed the extension and
    // granted the role access to the auth schema.
    initialized: bool,
}

impl<C: ClientInnerExt> Default for DbUserConnPool<C> {
    fn default() -> Self {
        Self {
            conns: Vec::new(),
            initialized: false,
        }
    }
}

impl<C: ClientInnerExt> DbUserConnPool<C> {
    fn clear_closed_clients(&mut self, conns: &mut usize) -> usize {
        let old_len = self.conns.len();

        self.conns.retain(|conn| !conn.conn.is_closed());

        let new_len = self.conns.len();
        let removed = old_len - new_len;
        *conns -= removed;
        removed
    }

    fn get_conn_entry(&mut self, conns: &mut usize) -> Option<ConnPoolEntry<C>> {
        let mut removed = self.clear_closed_clients(conns);
        let conn = self.conns.pop();
        if conn.is_some() {
            *conns -= 1;
            removed += 1;
        }
        Metrics::get()
            .proxy
            .http_pool_opened_connections
            .get_metric()
            .dec_by(removed as i64);
        conn
    }
}

pub(crate) struct LocalConnPool<C: ClientInnerExt> {
    global_pool: RwLock<EndpointConnPool<C>>,

    config: &'static crate::config::HttpConfig,
}

impl<C: ClientInnerExt> LocalConnPool<C> {
    pub(crate) fn new(config: &'static crate::config::HttpConfig) -> Arc<Self> {
        Arc::new(Self {
            global_pool: RwLock::new(EndpointConnPool {
                pools: HashMap::new(),
                total_conns: 0,
                max_conns: config.pool_options.max_conns_per_endpoint,
                global_pool_size_max_conns: config.pool_options.max_total_conns,
            }),
            config,
        })
    }

    pub(crate) fn get_idle_timeout(&self) -> Duration {
        self.config.pool_options.idle_timeout
    }

    pub(crate) fn get(
        self: &Arc<Self>,
        ctx: &RequestMonitoring,
        conn_info: &ConnInfo,
    ) -> Result<Option<LocalClient<C>>, HttpConnError> {
        let client = self
            .global_pool
            .write()
            .get_conn_entry(conn_info.db_and_user())
            .map(|entry| entry.conn);

        // ok return cached connection if found and establish a new one otherwise
        if let Some(client) = client {
            if client.is_closed() {
                info!("local_pool: cached connection '{conn_info}' is closed, opening a new one");
                return Ok(None);
            }
            tracing::Span::current().record("conn_id", tracing::field::display(client.conn_id));
            tracing::Span::current().record(
                "pid",
                tracing::field::display(client.inner.get_process_id()),
            );
            info!(
                cold_start_info = ColdStartInfo::HttpPoolHit.as_str(),
                "local_pool: reusing connection '{conn_info}'"
            );
            client.session.send(ctx.session_id())?;
            ctx.set_cold_start_info(ColdStartInfo::HttpPoolHit);
            ctx.success();
            return Ok(Some(LocalClient::new(
                client,
                conn_info.clone(),
                Arc::downgrade(self),
            )));
        }
        Ok(None)
    }

    pub(crate) fn initialized(self: &Arc<Self>, conn_info: &ConnInfo) -> bool {
        self.global_pool
            .read()
            .pools
            .get(&conn_info.db_and_user())
            .map_or(false, |pool| pool.initialized)
    }

    pub(crate) fn set_initialized(self: &Arc<Self>, conn_info: &ConnInfo) {
        self.global_pool
            .write()
            .pools
            .entry(conn_info.db_and_user())
            .or_default()
            .initialized = true;
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn poll_client(
    global_pool: Arc<LocalConnPool<tokio_postgres::Client>>,
    ctx: &RequestMonitoring,
    conn_info: ConnInfo,
    client: tokio_postgres::Client,
    mut connection: tokio_postgres::Connection<Socket, NoTlsStream>,
    key: SigningKey,
    conn_id: uuid::Uuid,
    aux: MetricsAuxInfo,
) -> LocalClient<tokio_postgres::Client> {
    let conn_gauge = Metrics::get().proxy.db_connections.guard(ctx.protocol());
    let mut session_id = ctx.session_id();
    let (tx, mut rx) = tokio::sync::watch::channel(session_id);

    let span = info_span!(parent: None, "connection", %conn_id);
    let cold_start_info = ctx.cold_start_info();
    span.in_scope(|| {
        info!(cold_start_info = cold_start_info.as_str(), %conn_info, %session_id, "new connection");
    });
    let pool = Arc::downgrade(&global_pool);
    let pool_clone = pool.clone();

    let db_user = conn_info.db_and_user();
    let idle = global_pool.get_idle_timeout();
    let cancel = CancellationToken::new();
    let cancelled = cancel.clone().cancelled_owned();

    tokio::spawn(
    async move {
        let _conn_gauge = conn_gauge;
        let mut idle_timeout = pin!(tokio::time::sleep(idle));
        let mut cancelled = pin!(cancelled);

        poll_fn(move |cx| {
            if cancelled.as_mut().poll(cx).is_ready() {
                info!("connection dropped");
                return Poll::Ready(())
            }

            match rx.has_changed() {
                Ok(true) => {
                    session_id = *rx.borrow_and_update();
                    info!(%session_id, "changed session");
                    idle_timeout.as_mut().reset(Instant::now() + idle);
                }
                Err(_) => {
                    info!("connection dropped");
                    return Poll::Ready(())
                }
                _ => {}
            }

            // 5 minute idle connection timeout
            if idle_timeout.as_mut().poll(cx).is_ready() {
                idle_timeout.as_mut().reset(Instant::now() + idle);
                info!("connection idle");
                if let Some(pool) = pool.clone().upgrade() {
                    // remove client from pool - should close the connection if it's idle.
                    // does nothing if the client is currently checked-out and in-use
                    if pool.global_pool.write().remove_client(db_user.clone(), conn_id) {
                        info!("idle connection removed");
                    }
                }
            }

            loop {
                let message = ready!(connection.poll_message(cx));

                match message {
                    Some(Ok(AsyncMessage::Notice(notice))) => {
                        info!(%session_id, "notice: {}", notice);
                    }
                    Some(Ok(AsyncMessage::Notification(notif))) => {
                        warn!(%session_id, pid = notif.process_id(), channel = notif.channel(), "notification received");
                    }
                    Some(Ok(_)) => {
                        warn!(%session_id, "unknown message");
                    }
                    Some(Err(e)) => {
                        error!(%session_id, "connection error: {}", e);
                        break
                    }
                    None => {
                        info!("connection closed");
                        break
                    }
                }
            }

            // remove from connection pool
            if let Some(pool) = pool.clone().upgrade() {
                if pool.global_pool.write().remove_client(db_user.clone(), conn_id) {
                    info!("closed connection removed");
                }
            }

            Poll::Ready(())
        }).await;

    }
    .instrument(span));

    let inner = ClientInner {
        inner: client,
        session: tx,
        cancel,
        aux,
        conn_id,
        key,
        jti: 0,
    };
    LocalClient::new(inner, conn_info, pool_clone)
}

pub(crate) struct ClientInner<C: ClientInnerExt> {
    inner: C,
    session: tokio::sync::watch::Sender<uuid::Uuid>,
    cancel: CancellationToken,
    aux: MetricsAuxInfo,
    conn_id: uuid::Uuid,

    // needed for pg_session_jwt state
    key: SigningKey,
    jti: u64,
}

impl<C: ClientInnerExt> Drop for ClientInner<C> {
    fn drop(&mut self) {
        // on client drop, tell the conn to shut down
        self.cancel.cancel();
    }
}

impl<C: ClientInnerExt> ClientInner<C> {
    pub(crate) fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }
}

impl ClientInner<tokio_postgres::Client> {
    pub(crate) async fn set_jwt_session(&mut self, payload: &[u8]) -> Result<(), HttpConnError> {
        self.jti += 1;
        let token = resign_jwt(&self.key, payload, self.jti)?;

        // initiates the auth session
        self.inner.simple_query("discard all").await?;
        self.inner
            .query(
                "select auth.jwt_session_init($1)",
                &[&token as &(dyn ToSql + Sync)],
            )
            .await?;

        let pid = self.inner.get_process_id();
        info!(pid, jti = self.jti, "user session state init");

        Ok(())
    }
}

pub(crate) struct LocalClient<C: ClientInnerExt> {
    span: Span,
    inner: Option<ClientInner<C>>,
    conn_info: ConnInfo,
    pool: Weak<LocalConnPool<C>>,
}

pub(crate) struct Discard<'a, C: ClientInnerExt> {
    conn_info: &'a ConnInfo,
    pool: &'a mut Weak<LocalConnPool<C>>,
}

impl<C: ClientInnerExt> LocalClient<C> {
    pub(self) fn new(
        inner: ClientInner<C>,
        conn_info: ConnInfo,
        pool: Weak<LocalConnPool<C>>,
    ) -> Self {
        Self {
            inner: Some(inner),
            span: Span::current(),
            conn_info,
            pool,
        }
    }

    pub(crate) fn client_inner(&mut self) -> (&mut ClientInner<C>, Discard<'_, C>) {
        let Self {
            inner,
            pool,
            conn_info,
            span: _,
        } = self;
        let inner_m = inner.as_mut().expect("client inner should not be removed");
        (inner_m, Discard { conn_info, pool })
    }

    pub(crate) fn inner(&mut self) -> (&mut C, Discard<'_, C>) {
        let Self {
            inner,
            pool,
            conn_info,
            span: _,
        } = self;
        let inner = inner.as_mut().expect("client inner should not be removed");
        (&mut inner.inner, Discard { conn_info, pool })
    }
}

/// implements relatively efficient in-place json object key upserting
///
/// only supports top-level keys
fn upsert_json_object(
    payload: &[u8],
    key: &str,
    value: &RawValue,
) -> Result<String, serde_json::Error> {
    let mut payload = serde_json::from_slice::<IndexMap<&str, &RawValue>>(payload)?;
    payload.insert(key, value);
    serde_json::to_string(&payload)
}

fn resign_jwt(sk: &SigningKey, payload: &[u8], jti: u64) -> Result<String, HttpConnError> {
    let mut buffer = itoa::Buffer::new();

    // encode the jti integer to a json rawvalue
    let jti = serde_json::from_str::<&RawValue>(buffer.format(jti)).unwrap();

    // update the jti in-place
    let payload =
        upsert_json_object(payload, "jti", jti).map_err(HttpConnError::JwtPayloadError)?;

    // sign the jwt
    let token = sign_jwt(sk, payload.as_bytes());

    Ok(token)
}

fn sign_jwt(sk: &SigningKey, payload: &[u8]) -> String {
    let header_len = 20;
    let payload_len = Base64UrlUnpadded::encoded_len(payload);
    let signature_len = Base64UrlUnpadded::encoded_len(&[0; 64]);
    let total_len = header_len + payload_len + signature_len + 2;

    let mut jwt = String::with_capacity(total_len);
    let cap = jwt.capacity();

    // we only need an empty header with the alg specified.
    // base64url(r#"{"alg":"ES256"}"#) == "eyJhbGciOiJFUzI1NiJ9"
    jwt.push_str("eyJhbGciOiJFUzI1NiJ9.");

    // encode the jwt payload in-place
    base64::encode_config_buf(payload, base64::URL_SAFE_NO_PAD, &mut jwt);

    // create the signature from the encoded header || payload
    let sig: Signature = sk.sign(jwt.as_bytes());

    jwt.push('.');

    // encode the jwt signature in-place
    base64::encode_config_buf(sig.to_bytes(), base64::URL_SAFE_NO_PAD, &mut jwt);

    debug_assert_eq!(
        jwt.len(),
        total_len,
        "the jwt len should match our expected len"
    );
    debug_assert_eq!(jwt.capacity(), cap, "the jwt capacity should not change");

    jwt
}

impl<C: ClientInnerExt> LocalClient<C> {
    pub(crate) fn metrics(&self) -> Arc<MetricCounter> {
        let aux = &self.inner.as_ref().unwrap().aux;
        USAGE_METRICS.register(Ids {
            endpoint_id: aux.endpoint_id,
            branch_id: aux.branch_id,
        })
    }

    fn do_drop(&mut self) -> Option<impl FnOnce() + use<C>> {
        let conn_info = self.conn_info.clone();
        let client = self
            .inner
            .take()
            .expect("client inner should not be removed");
        if let Some(conn_pool) = std::mem::take(&mut self.pool).upgrade() {
            let current_span = self.span.clone();
            // return connection to the pool
            return Some(move || {
                let _span = current_span.enter();
                EndpointConnPool::put(&conn_pool.global_pool, &conn_info, client);
            });
        }
        None
    }
}

impl<C: ClientInnerExt> Drop for LocalClient<C> {
    fn drop(&mut self) {
        if let Some(drop) = self.do_drop() {
            tokio::task::spawn_blocking(drop);
        }
    }
}

impl<C: ClientInnerExt> Discard<'_, C> {
    pub(crate) fn check_idle(&mut self, status: ReadyForQueryStatus) {
        let conn_info = &self.conn_info;
        if status != ReadyForQueryStatus::Idle && std::mem::take(self.pool).strong_count() > 0 {
            info!(
                "local_pool: throwing away connection '{conn_info}' because connection is not idle"
            );
        }
    }
    pub(crate) fn discard(&mut self) {
        let conn_info = &self.conn_info;
        if std::mem::take(self.pool).strong_count() > 0 {
            info!("local_pool: throwing away connection '{conn_info}' because connection is potentially in a broken state");
        }
    }
}

#[cfg(test)]
mod tests {
    use p256::ecdsa::SigningKey;
    use typed_json::json;

    use super::resign_jwt;

    #[test]
    fn jwt_token_snapshot() {
        let key = SigningKey::from_bytes(&[1; 32].into()).unwrap();
        let data =
            json!({"foo":"bar","jti":"foo\nbar","nested":{"jti":"tricky nesting"}}).to_string();

        let jwt = resign_jwt(&key, data.as_bytes(), 2).unwrap();

        // To validate the JWT, copy the JWT string and paste it into https://jwt.io/.
        // In the public-key box, paste the following jwk public key
        // `{"kty":"EC","crv":"P-256","x":"b_A7lJJBzh2t1DUZ5pYOCoW0GmmgXDKBA6orzhWUyhY","y":"PE91OlW_AdxT9sCwx-7ni0DG_30lqW4igrmJzvccFEo"}`

        // let pub_key = p256::ecdsa::VerifyingKey::from(&key);
        // let pub_key = p256::PublicKey::from(pub_key);
        // println!("{}", pub_key.to_jwk_string());

        assert_eq!(jwt, "eyJhbGciOiJFUzI1NiJ9.eyJmb28iOiJiYXIiLCJqdGkiOjIsIm5lc3RlZCI6eyJqdGkiOiJ0cmlja3kgbmVzdGluZyJ9fQ.pYf0LxoJ8sDgpmsYOgrbNecOSipnPBEGwnZzB-JhW2cONrKlqRsgXwK8_cOsyolGy-hTTe8GXbWTl_UdpF5RyA");
    }
}
