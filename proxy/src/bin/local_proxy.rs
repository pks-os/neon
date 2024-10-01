use std::{net::SocketAddr, pin::pin, str::FromStr, sync::Arc, time::Duration};

use anyhow::{bail, ensure, Context};
use camino::{Utf8Path, Utf8PathBuf};
use compute_api::spec::LocalProxySpec;
use dashmap::DashMap;
use futures::future::Either;
use proxy::{
    auth::backend::{
        jwt::JwkCache,
        local::{LocalBackend, JWKS_ROLE_MAP},
    },
    cancellation::CancellationHandlerMain,
    config::{self, AuthenticationConfig, HttpConfig, ProxyConfig, RetryConfig},
    console::{
        locks::ApiLocks,
        messages::{EndpointJwksResponse, JwksSettings},
    },
    http::health_server::AppMetrics,
    intern::RoleNameInt,
    metrics::{Metrics, ThreadPoolMetrics},
    rate_limiter::{BucketRateLimiter, EndpointRateLimiter, LeakyBucketConfig, RateBucketInfo},
    scram::threadpool::ThreadPool,
    serverless::{self, cancel_set::CancelSet, GlobalConnPoolOptions},
    RoleName,
};

project_git_version!(GIT_VERSION);
project_build_tag!(BUILD_TAG);

use clap::Parser;
use tokio::{net::TcpListener, sync::Notify, task::JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use utils::{pid_file, project_build_tag, project_git_version, sentry_init::init_sentry};

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// Neon proxy/router
#[derive(Parser)]
#[command(version = GIT_VERSION, about)]
struct LocalProxyCliArgs {
    /// listen for incoming metrics connections on ip:port
    #[clap(long, default_value = "127.0.0.1:7001")]
    metrics: String,
    /// listen for incoming http connections on ip:port
    #[clap(long)]
    http: String,
    /// timeout for the TLS handshake
    #[clap(long, default_value = "15s", value_parser = humantime::parse_duration)]
    handshake_timeout: tokio::time::Duration,
    /// lock for `connect_compute` api method. example: "shards=32,permits=4,epoch=10m,timeout=1s". (use `permits=0` to disable).
    #[clap(long, default_value = config::ConcurrencyLockOptions::DEFAULT_OPTIONS_CONNECT_COMPUTE_LOCK)]
    connect_compute_lock: String,
    #[clap(flatten)]
    sql_over_http: SqlOverHttpArgs,
    /// User rate limiter max number of requests per second.
    ///
    /// Provided in the form `<Requests Per Second>@<Bucket Duration Size>`.
    /// Can be given multiple times for different bucket sizes.
    #[clap(long, default_values_t = RateBucketInfo::DEFAULT_ENDPOINT_SET)]
    user_rps_limit: Vec<RateBucketInfo>,
    /// Whether the auth rate limiter actually takes effect (for testing)
    #[clap(long, default_value_t = false, value_parser = clap::builder::BoolishValueParser::new(), action = clap::ArgAction::Set)]
    auth_rate_limit_enabled: bool,
    /// Authentication rate limiter max number of hashes per second.
    #[clap(long, default_values_t = RateBucketInfo::DEFAULT_AUTH_SET)]
    auth_rate_limit: Vec<RateBucketInfo>,
    /// The IP subnet to use when considering whether two IP addresses are considered the same.
    #[clap(long, default_value_t = 64)]
    auth_rate_limit_ip_subnet: u8,
    /// Whether to retry the connection to the compute node
    #[clap(long, default_value = config::RetryConfig::CONNECT_TO_COMPUTE_DEFAULT_VALUES)]
    connect_to_compute_retry: String,
    /// Address of the postgres server
    #[clap(long, default_value = "127.0.0.1:5432")]
    compute: SocketAddr,
    /// Path of the local proxy config file
    #[clap(long, default_value = "./localproxy.json")]
    config_path: Utf8PathBuf,
    /// Path of the local proxy PID file
    #[clap(long, default_value = "./localproxy.pid")]
    pid_path: Utf8PathBuf,
}

#[derive(clap::Args, Clone, Copy, Debug)]
struct SqlOverHttpArgs {
    /// How many connections to pool for each endpoint. Excess connections are discarded
    #[clap(long, default_value_t = 200)]
    sql_over_http_pool_max_total_conns: usize,

    /// How long pooled connections should remain idle for before closing
    #[clap(long, default_value = "5m", value_parser = humantime::parse_duration)]
    sql_over_http_idle_timeout: tokio::time::Duration,

    #[clap(long, default_value_t = 100)]
    sql_over_http_client_conn_threshold: u64,

    #[clap(long, default_value_t = 16)]
    sql_over_http_cancel_set_shards: usize,

    #[clap(long, default_value_t = 10 * 1024 * 1024)] // 10 MiB
    sql_over_http_max_request_size_bytes: u64,

    #[clap(long, default_value_t = 10 * 1024 * 1024)] // 10 MiB
    sql_over_http_max_response_size_bytes: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _logging_guard = proxy::logging::init().await?;
    let _panic_hook_guard = utils::logging::replace_panic_hook_with_tracing_panic_hook();
    let _sentry_guard = init_sentry(Some(GIT_VERSION.into()), &[]);

    Metrics::install(Arc::new(ThreadPoolMetrics::new(0)));

    info!("Version: {GIT_VERSION}");
    info!("Build_tag: {BUILD_TAG}");
    let neon_metrics = ::metrics::NeonMetrics::new(::metrics::BuildInfo {
        revision: GIT_VERSION,
        build_tag: BUILD_TAG,
    });

    let jemalloc = match proxy::jemalloc::MetricRecorder::new() {
        Ok(t) => Some(t),
        Err(e) => {
            tracing::error!(error = ?e, "could not start jemalloc metrics loop");
            None
        }
    };

    let args = LocalProxyCliArgs::parse();
    let config = build_config(&args)?;

    // before we bind to any ports, write the process ID to a file
    // so that compute-ctl can find our process later
    // in order to trigger the appropriate SIGHUP on config change.
    //
    // This also claims a "lock" that makes sure only one instance
    // of local-proxy runs at a time.
    let _process_guard = loop {
        match pid_file::claim_for_current_process(&args.pid_path) {
            Ok(guard) => break guard,
            Err(e) => {
                // compute-ctl might have tried to read the pid-file to let us
                // know about some config change. We should try again.
                error!(path=?args.pid_path, "could not claim PID file guard: {e:?}");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    };

    let metrics_listener = TcpListener::bind(args.metrics).await?.into_std()?;
    let http_listener = TcpListener::bind(args.http).await?;
    let shutdown = CancellationToken::new();

    // todo: should scale with CU
    let endpoint_rate_limiter = Arc::new(EndpointRateLimiter::new_with_shards(
        LeakyBucketConfig {
            rps: 10.0,
            max: 100.0,
        },
        16,
    ));

    // write the process ID to a file so that compute-ctl can find our process later
    // in order to trigger the appropriate SIGHUP on config change.
    let pid = std::process::id();
    info!("process running in PID {pid}");
    std::fs::write(args.pid_path, format!("{pid}\n")).context("writing PID to file")?;

    let mut maintenance_tasks = JoinSet::new();

    let refresh_config_notify = Arc::new(Notify::new());
    maintenance_tasks.spawn(proxy::handle_signals(shutdown.clone(), {
        let refresh_config_notify = Arc::clone(&refresh_config_notify);
        move || {
            refresh_config_notify.notify_one();
        }
    }));

    // trigger the first config load **after** setting up the signal hook
    // to avoid the race condition where:
    // 1. No config file registered when local-proxy starts up
    // 2. The config file is written but the signal hook is not yet received
    // 3. local-proxy completes startup but has no config loaded, despite there being a registerd config.
    refresh_config_notify.notify_one();
    tokio::spawn(refresh_config_loop(args.config_path, refresh_config_notify));

    maintenance_tasks.spawn(proxy::http::health_server::task_main(
        metrics_listener,
        AppMetrics {
            jemalloc,
            neon_metrics,
            proxy: proxy::metrics::Metrics::get(),
        },
    ));

    let task = serverless::task_main(
        config,
        http_listener,
        shutdown.clone(),
        Arc::new(CancellationHandlerMain::new(
            Arc::new(DashMap::new()),
            None,
            proxy::metrics::CancellationSource::Local,
        )),
        endpoint_rate_limiter,
    );

    match futures::future::select(pin!(maintenance_tasks.join_next()), pin!(task)).await {
        // exit immediately on maintenance task completion
        Either::Left((Some(res), _)) => match proxy::flatten_err(res)? {},
        // exit with error immediately if all maintenance tasks have ceased (should be caught by branch above)
        Either::Left((None, _)) => bail!("no maintenance tasks running. invalid state"),
        // exit immediately on client task error
        Either::Right((res, _)) => res?,
    }

    Ok(())
}

/// ProxyConfig is created at proxy startup, and lives forever.
fn build_config(args: &LocalProxyCliArgs) -> anyhow::Result<&'static ProxyConfig> {
    let config::ConcurrencyLockOptions {
        shards,
        limiter,
        epoch,
        timeout,
    } = args.connect_compute_lock.parse()?;
    info!(
        ?limiter,
        shards,
        ?epoch,
        "Using NodeLocks (connect_compute)"
    );
    let connect_compute_locks = ApiLocks::new(
        "connect_compute_lock",
        limiter,
        shards,
        timeout,
        epoch,
        &Metrics::get().proxy.connect_compute_lock,
    )?;

    let http_config = HttpConfig {
        accept_websockets: false,
        pool_options: GlobalConnPoolOptions {
            gc_epoch: Duration::from_secs(60),
            pool_shards: 2,
            idle_timeout: args.sql_over_http.sql_over_http_idle_timeout,
            opt_in: false,

            max_conns_per_endpoint: args.sql_over_http.sql_over_http_pool_max_total_conns,
            max_total_conns: args.sql_over_http.sql_over_http_pool_max_total_conns,
        },
        cancel_set: CancelSet::new(args.sql_over_http.sql_over_http_cancel_set_shards),
        client_conn_threshold: args.sql_over_http.sql_over_http_client_conn_threshold,
        max_request_size_bytes: args.sql_over_http.sql_over_http_max_request_size_bytes,
        max_response_size_bytes: args.sql_over_http.sql_over_http_max_response_size_bytes,
    };

    Ok(Box::leak(Box::new(ProxyConfig {
        tls_config: None,
        auth_backend: proxy::auth::Backend::Local(proxy::auth::backend::MaybeOwned::Owned(
            LocalBackend::new(args.compute),
        )),
        metric_collection: None,
        allow_self_signed_compute: false,
        http_config,
        authentication_config: AuthenticationConfig {
            jwks_cache: JwkCache::default(),
            thread_pool: ThreadPool::new(0),
            scram_protocol_timeout: Duration::from_secs(10),
            rate_limiter_enabled: false,
            rate_limiter: BucketRateLimiter::new(vec![]),
            rate_limit_ip_subnet: 64,
            ip_allowlist_check_enabled: true,
            is_auth_broker: false,
            accept_jwts: true,
        },
        proxy_protocol_v2: config::ProxyProtocolV2::Rejected,
        handshake_timeout: Duration::from_secs(10),
        region: "local".into(),
        wake_compute_retry_config: RetryConfig::parse(RetryConfig::WAKE_COMPUTE_DEFAULT_VALUES)?,
        connect_compute_locks,
        connect_to_compute_retry_config: RetryConfig::parse(
            RetryConfig::CONNECT_TO_COMPUTE_DEFAULT_VALUES,
        )?,
    })))
}

async fn refresh_config_loop(path: Utf8PathBuf, rx: Arc<Notify>) {
    loop {
        rx.notified().await;

        match refresh_config_inner(&path).await {
            Ok(()) => {}
            Err(e) => {
                error!(error=?e, ?path, "could not read config file");
            }
        }
    }
}

async fn refresh_config_inner(path: &Utf8Path) -> anyhow::Result<()> {
    let bytes = tokio::fs::read(&path).await?;
    let data: LocalProxySpec = serde_json::from_slice(&bytes)?;

    let mut jwks_set = vec![];

    for jwks in data.jwks {
        let mut jwks_url = url::Url::from_str(&jwks.jwks_url).context("parsing JWKS url")?;

        ensure!(
            jwks_url.has_authority()
                && (jwks_url.scheme() == "http" || jwks_url.scheme() == "https"),
            "Invalid JWKS url. Must be HTTP",
        );

        ensure!(
            jwks_url.host().is_some_and(|h| h != url::Host::Domain("")),
            "Invalid JWKS url. No domain listed",
        );

        // clear username, password and ports
        jwks_url
            .set_username("")
            .expect("url can be a base and has a valid host and is not a file. should not error");
        jwks_url
            .set_password(None)
            .expect("url can be a base and has a valid host and is not a file. should not error");
        // local testing is hard if we need to have a specific restricted port
        if cfg!(not(feature = "testing")) {
            jwks_url.set_port(None).expect(
                "url can be a base and has a valid host and is not a file. should not error",
            );
        }

        // clear query params
        jwks_url.set_fragment(None);
        jwks_url.query_pairs_mut().clear().finish();

        if jwks_url.scheme() != "https" {
            // local testing is hard if we need to set up https support.
            if cfg!(not(feature = "testing")) {
                jwks_url
                    .set_scheme("https")
                    .expect("should not error to set the scheme to https if it was http");
            } else {
                warn!(scheme = jwks_url.scheme(), "JWKS url is not HTTPS");
            }
        }

        jwks_set.push(JwksSettings {
            id: jwks.id,
            jwks_url,
            provider_name: jwks.provider_name,
            jwt_audience: jwks.jwt_audience,
            role_names: jwks
                .role_names
                .into_iter()
                .map(RoleName::from)
                .map(|s| RoleNameInt::from(&s))
                .collect(),
        })
    }

    info!("successfully loaded new config");
    JWKS_ROLE_MAP.store(Some(Arc::new(EndpointJwksResponse { jwks: jwks_set })));

    Ok(())
}
