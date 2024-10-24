#[cfg(any(test, feature = "testing"))]
pub mod mock;
pub mod neon;

use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use tokio::time::Instant;
use tracing::info;

use super::messages::{ControlPlaneError, MetricsAuxInfo};
use crate::auth::backend::jwt::{AuthRule, FetchAuthRules, FetchAuthRulesError};
use crate::auth::backend::{ComputeCredentialKeys, ComputeUserInfo};
use crate::auth::IpPattern;
use crate::cache::endpoints::EndpointsCache;
use crate::cache::project_info::ProjectInfoCacheImpl;
use crate::cache::{Cached, TimedLru};
use crate::config::{CacheOptions, EndpointCacheConfig, ProjectInfoCacheOptions};
use crate::context::RequestMonitoring;
use crate::error::ReportableError;
use crate::intern::ProjectIdInt;
use crate::metrics::ApiLockMetrics;
use crate::rate_limiter::{DynamicLimiter, Outcome, RateLimiterConfig, Token};
use crate::types::{EndpointCacheKey, EndpointId};
use crate::{compute, scram};

pub(crate) mod errors {
    use thiserror::Error;

    use super::ApiLockError;
    use crate::control_plane::messages::{self, ControlPlaneError, Reason};
    use crate::error::{io_error, ErrorKind, ReportableError, UserFacingError};
    use crate::proxy::retry::CouldRetry;

    /// A go-to error message which doesn't leak any detail.
    pub(crate) const REQUEST_FAILED: &str = "Console request failed";

    /// Common console API error.
    #[derive(Debug, Error)]
    pub(crate) enum ApiError {
        /// Error returned by the console itself.
        #[error("{REQUEST_FAILED} with {0}")]
        ControlPlane(Box<ControlPlaneError>),

        /// Various IO errors like broken pipe or malformed payload.
        #[error("{REQUEST_FAILED}: {0}")]
        Transport(#[from] std::io::Error),
    }

    impl ApiError {
        /// Returns HTTP status code if it's the reason for failure.
        pub(crate) fn get_reason(&self) -> messages::Reason {
            match self {
                ApiError::ControlPlane(e) => e.get_reason(),
                ApiError::Transport(_) => messages::Reason::Unknown,
            }
        }
    }

    impl UserFacingError for ApiError {
        fn to_string_client(&self) -> String {
            match self {
                // To minimize risks, only select errors are forwarded to users.
                ApiError::ControlPlane(c) => c.get_user_facing_message(),
                ApiError::Transport(_) => REQUEST_FAILED.to_owned(),
            }
        }
    }

    impl ReportableError for ApiError {
        fn get_error_kind(&self) -> crate::error::ErrorKind {
            match self {
                ApiError::ControlPlane(e) => match e.get_reason() {
                    Reason::RoleProtected => ErrorKind::User,
                    Reason::ResourceNotFound => ErrorKind::User,
                    Reason::ProjectNotFound => ErrorKind::User,
                    Reason::EndpointNotFound => ErrorKind::User,
                    Reason::BranchNotFound => ErrorKind::User,
                    Reason::RateLimitExceeded => ErrorKind::ServiceRateLimit,
                    Reason::NonDefaultBranchComputeTimeExceeded => ErrorKind::Quota,
                    Reason::ActiveTimeQuotaExceeded => ErrorKind::Quota,
                    Reason::ComputeTimeQuotaExceeded => ErrorKind::Quota,
                    Reason::WrittenDataQuotaExceeded => ErrorKind::Quota,
                    Reason::DataTransferQuotaExceeded => ErrorKind::Quota,
                    Reason::LogicalSizeQuotaExceeded => ErrorKind::Quota,
                    Reason::ConcurrencyLimitReached => ErrorKind::ControlPlane,
                    Reason::LockAlreadyTaken => ErrorKind::ControlPlane,
                    Reason::RunningOperations => ErrorKind::ControlPlane,
                    Reason::ActiveEndpointsLimitExceeded => ErrorKind::ControlPlane,
                    Reason::Unknown => ErrorKind::ControlPlane,
                },
                ApiError::Transport(_) => crate::error::ErrorKind::ControlPlane,
            }
        }
    }

    impl CouldRetry for ApiError {
        fn could_retry(&self) -> bool {
            match self {
                // retry some transport errors
                Self::Transport(io) => io.could_retry(),
                Self::ControlPlane(e) => e.could_retry(),
            }
        }
    }

    impl From<reqwest::Error> for ApiError {
        fn from(e: reqwest::Error) -> Self {
            io_error(e).into()
        }
    }

    impl From<reqwest_middleware::Error> for ApiError {
        fn from(e: reqwest_middleware::Error) -> Self {
            io_error(e).into()
        }
    }

    #[derive(Debug, Error)]
    pub(crate) enum GetAuthInfoError {
        // We shouldn't include the actual secret here.
        #[error("Console responded with a malformed auth secret")]
        BadSecret,

        #[error(transparent)]
        ApiError(ApiError),
    }

    // This allows more useful interactions than `#[from]`.
    impl<E: Into<ApiError>> From<E> for GetAuthInfoError {
        fn from(e: E) -> Self {
            Self::ApiError(e.into())
        }
    }

    impl UserFacingError for GetAuthInfoError {
        fn to_string_client(&self) -> String {
            match self {
                // We absolutely should not leak any secrets!
                Self::BadSecret => REQUEST_FAILED.to_owned(),
                // However, API might return a meaningful error.
                Self::ApiError(e) => e.to_string_client(),
            }
        }
    }

    impl ReportableError for GetAuthInfoError {
        fn get_error_kind(&self) -> crate::error::ErrorKind {
            match self {
                Self::BadSecret => crate::error::ErrorKind::ControlPlane,
                Self::ApiError(_) => crate::error::ErrorKind::ControlPlane,
            }
        }
    }

    #[derive(Debug, Error)]
    pub(crate) enum WakeComputeError {
        #[error("Console responded with a malformed compute address: {0}")]
        BadComputeAddress(Box<str>),

        #[error(transparent)]
        ApiError(ApiError),

        #[error("Too many connections attempts")]
        TooManyConnections,

        #[error("error acquiring resource permit: {0}")]
        TooManyConnectionAttempts(#[from] ApiLockError),
    }

    // This allows more useful interactions than `#[from]`.
    impl<E: Into<ApiError>> From<E> for WakeComputeError {
        fn from(e: E) -> Self {
            Self::ApiError(e.into())
        }
    }

    impl UserFacingError for WakeComputeError {
        fn to_string_client(&self) -> String {
            match self {
                // We shouldn't show user the address even if it's broken.
                // Besides, user is unlikely to care about this detail.
                Self::BadComputeAddress(_) => REQUEST_FAILED.to_owned(),
                // However, API might return a meaningful error.
                Self::ApiError(e) => e.to_string_client(),

                Self::TooManyConnections => self.to_string(),

                Self::TooManyConnectionAttempts(_) => {
                    "Failed to acquire permit to connect to the database. Too many database connection attempts are currently ongoing.".to_owned()
                }
            }
        }
    }

    impl ReportableError for WakeComputeError {
        fn get_error_kind(&self) -> crate::error::ErrorKind {
            match self {
                Self::BadComputeAddress(_) => crate::error::ErrorKind::ControlPlane,
                Self::ApiError(e) => e.get_error_kind(),
                Self::TooManyConnections => crate::error::ErrorKind::RateLimit,
                Self::TooManyConnectionAttempts(e) => e.get_error_kind(),
            }
        }
    }

    impl CouldRetry for WakeComputeError {
        fn could_retry(&self) -> bool {
            match self {
                Self::BadComputeAddress(_) => false,
                Self::ApiError(e) => e.could_retry(),
                Self::TooManyConnections => false,
                Self::TooManyConnectionAttempts(_) => false,
            }
        }
    }

    #[derive(Debug, Error)]
    pub enum GetEndpointJwksError {
        #[error("endpoint not found")]
        EndpointNotFound,

        #[error("failed to build control plane request: {0}")]
        RequestBuild(#[source] reqwest::Error),

        #[error("failed to send control plane request: {0}")]
        RequestExecute(#[source] reqwest_middleware::Error),

        #[error(transparent)]
        ControlPlane(#[from] ApiError),

        #[cfg(any(test, feature = "testing"))]
        #[error(transparent)]
        TokioPostgres(#[from] tokio_postgres::Error),

        #[cfg(any(test, feature = "testing"))]
        #[error(transparent)]
        ParseUrl(#[from] url::ParseError),

        #[cfg(any(test, feature = "testing"))]
        #[error(transparent)]
        TaskJoin(#[from] tokio::task::JoinError),
    }
}

/// Auth secret which is managed by the cloud.
#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) enum AuthSecret {
    #[cfg(any(test, feature = "testing"))]
    /// Md5 hash of user's password.
    Md5([u8; 16]),

    /// [SCRAM](crate::scram) authentication info.
    Scram(scram::ServerSecret),
}

#[derive(Default)]
pub(crate) struct AuthInfo {
    pub(crate) secret: Option<AuthSecret>,
    /// List of IP addresses allowed for the autorization.
    pub(crate) allowed_ips: Vec<IpPattern>,
    /// Project ID. This is used for cache invalidation.
    pub(crate) project_id: Option<ProjectIdInt>,
}

/// Info for establishing a connection to a compute node.
/// This is what we get after auth succeeded, but not before!
#[derive(Clone)]
pub(crate) struct NodeInfo {
    /// Compute node connection params.
    /// It's sad that we have to clone this, but this will improve
    /// once we migrate to a bespoke connection logic.
    pub(crate) config: compute::ConnCfg,

    /// Labels for proxy's metrics.
    pub(crate) aux: MetricsAuxInfo,

    /// Whether we should accept self-signed certificates (for testing)
    pub(crate) allow_self_signed_compute: bool,
}

impl NodeInfo {
    pub(crate) async fn connect(
        &self,
        ctx: &RequestMonitoring,
        timeout: Duration,
    ) -> Result<compute::PostgresConnection, compute::ConnectionError> {
        self.config
            .connect(
                ctx,
                self.allow_self_signed_compute,
                self.aux.clone(),
                timeout,
            )
            .await
    }
    pub(crate) fn reuse_settings(&mut self, other: Self) {
        self.allow_self_signed_compute = other.allow_self_signed_compute;
        self.config.reuse_password(other.config);
    }

    pub(crate) fn set_keys(&mut self, keys: &ComputeCredentialKeys) {
        match keys {
            #[cfg(any(test, feature = "testing"))]
            ComputeCredentialKeys::Password(password) => self.config.password(password),
            ComputeCredentialKeys::AuthKeys(auth_keys) => self.config.auth_keys(*auth_keys),
            ComputeCredentialKeys::JwtPayload(_) | ComputeCredentialKeys::None => &mut self.config,
        };
    }
}

pub(crate) type NodeInfoCache =
    TimedLru<EndpointCacheKey, Result<NodeInfo, Box<ControlPlaneError>>>;
pub(crate) type CachedNodeInfo = Cached<&'static NodeInfoCache, NodeInfo>;
pub(crate) type CachedRoleSecret = Cached<&'static ProjectInfoCacheImpl, Option<AuthSecret>>;
pub(crate) type CachedAllowedIps = Cached<&'static ProjectInfoCacheImpl, Arc<Vec<IpPattern>>>;

/// This will allocate per each call, but the http requests alone
/// already require a few allocations, so it should be fine.
pub(crate) trait Api {
    /// Get the client's auth secret for authentication.
    /// Returns option because user not found situation is special.
    /// We still have to mock the scram to avoid leaking information that user doesn't exist.
    async fn get_role_secret(
        &self,
        ctx: &RequestMonitoring,
        user_info: &ComputeUserInfo,
    ) -> Result<CachedRoleSecret, errors::GetAuthInfoError>;

    async fn get_allowed_ips_and_secret(
        &self,
        ctx: &RequestMonitoring,
        user_info: &ComputeUserInfo,
    ) -> Result<(CachedAllowedIps, Option<CachedRoleSecret>), errors::GetAuthInfoError>;

    async fn get_endpoint_jwks(
        &self,
        ctx: &RequestMonitoring,
        endpoint: EndpointId,
    ) -> Result<Vec<AuthRule>, errors::GetEndpointJwksError>;

    /// Wake up the compute node and return the corresponding connection info.
    async fn wake_compute(
        &self,
        ctx: &RequestMonitoring,
        user_info: &ComputeUserInfo,
    ) -> Result<CachedNodeInfo, errors::WakeComputeError>;
}

#[non_exhaustive]
#[derive(Clone)]
pub enum ControlPlaneBackend {
    /// Current Management API (V2).
    Management(neon::Api),
    /// Local mock control plane.
    #[cfg(any(test, feature = "testing"))]
    PostgresMock(mock::Api),
    /// Internal testing
    #[cfg(test)]
    #[allow(private_interfaces)]
    Test(Box<dyn crate::auth::backend::TestBackend>),
}

impl Api for ControlPlaneBackend {
    async fn get_role_secret(
        &self,
        ctx: &RequestMonitoring,
        user_info: &ComputeUserInfo,
    ) -> Result<CachedRoleSecret, errors::GetAuthInfoError> {
        match self {
            Self::Management(api) => api.get_role_secret(ctx, user_info).await,
            #[cfg(any(test, feature = "testing"))]
            Self::PostgresMock(api) => api.get_role_secret(ctx, user_info).await,
            #[cfg(test)]
            Self::Test(_) => {
                unreachable!("this function should never be called in the test backend")
            }
        }
    }

    async fn get_allowed_ips_and_secret(
        &self,
        ctx: &RequestMonitoring,
        user_info: &ComputeUserInfo,
    ) -> Result<(CachedAllowedIps, Option<CachedRoleSecret>), errors::GetAuthInfoError> {
        match self {
            Self::Management(api) => api.get_allowed_ips_and_secret(ctx, user_info).await,
            #[cfg(any(test, feature = "testing"))]
            Self::PostgresMock(api) => api.get_allowed_ips_and_secret(ctx, user_info).await,
            #[cfg(test)]
            Self::Test(api) => api.get_allowed_ips_and_secret(),
        }
    }

    async fn get_endpoint_jwks(
        &self,
        ctx: &RequestMonitoring,
        endpoint: EndpointId,
    ) -> Result<Vec<AuthRule>, errors::GetEndpointJwksError> {
        match self {
            Self::Management(api) => api.get_endpoint_jwks(ctx, endpoint).await,
            #[cfg(any(test, feature = "testing"))]
            Self::PostgresMock(api) => api.get_endpoint_jwks(ctx, endpoint).await,
            #[cfg(test)]
            Self::Test(_api) => Ok(vec![]),
        }
    }

    async fn wake_compute(
        &self,
        ctx: &RequestMonitoring,
        user_info: &ComputeUserInfo,
    ) -> Result<CachedNodeInfo, errors::WakeComputeError> {
        match self {
            Self::Management(api) => api.wake_compute(ctx, user_info).await,
            #[cfg(any(test, feature = "testing"))]
            Self::PostgresMock(api) => api.wake_compute(ctx, user_info).await,
            #[cfg(test)]
            Self::Test(api) => api.wake_compute(),
        }
    }
}

/// Various caches for [`control_plane`](super).
pub struct ApiCaches {
    /// Cache for the `wake_compute` API method.
    pub(crate) node_info: NodeInfoCache,
    /// Cache which stores project_id -> endpoint_ids mapping.
    pub project_info: Arc<ProjectInfoCacheImpl>,
    /// List of all valid endpoints.
    pub endpoints_cache: Arc<EndpointsCache>,
}

impl ApiCaches {
    pub fn new(
        wake_compute_cache_config: CacheOptions,
        project_info_cache_config: ProjectInfoCacheOptions,
        endpoint_cache_config: EndpointCacheConfig,
    ) -> Self {
        Self {
            node_info: NodeInfoCache::new(
                "node_info_cache",
                wake_compute_cache_config.size,
                wake_compute_cache_config.ttl,
                true,
            ),
            project_info: Arc::new(ProjectInfoCacheImpl::new(project_info_cache_config)),
            endpoints_cache: Arc::new(EndpointsCache::new(endpoint_cache_config)),
        }
    }
}

/// Various caches for [`control_plane`](super).
pub struct ApiLocks<K> {
    name: &'static str,
    node_locks: DashMap<K, Arc<DynamicLimiter>>,
    config: RateLimiterConfig,
    timeout: Duration,
    epoch: std::time::Duration,
    metrics: &'static ApiLockMetrics,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ApiLockError {
    #[error("timeout acquiring resource permit")]
    TimeoutError(#[from] tokio::time::error::Elapsed),
}

impl ReportableError for ApiLockError {
    fn get_error_kind(&self) -> crate::error::ErrorKind {
        match self {
            ApiLockError::TimeoutError(_) => crate::error::ErrorKind::RateLimit,
        }
    }
}

impl<K: Hash + Eq + Clone> ApiLocks<K> {
    pub fn new(
        name: &'static str,
        config: RateLimiterConfig,
        shards: usize,
        timeout: Duration,
        epoch: std::time::Duration,
        metrics: &'static ApiLockMetrics,
    ) -> prometheus::Result<Self> {
        Ok(Self {
            name,
            node_locks: DashMap::with_shard_amount(shards),
            config,
            timeout,
            epoch,
            metrics,
        })
    }

    pub(crate) async fn get_permit(&self, key: &K) -> Result<WakeComputePermit, ApiLockError> {
        if self.config.initial_limit == 0 {
            return Ok(WakeComputePermit {
                permit: Token::disabled(),
            });
        }
        let now = Instant::now();
        let semaphore = {
            // get fast path
            if let Some(semaphore) = self.node_locks.get(key) {
                semaphore.clone()
            } else {
                self.node_locks
                    .entry(key.clone())
                    .or_insert_with(|| {
                        self.metrics.semaphores_registered.inc();
                        DynamicLimiter::new(self.config)
                    })
                    .clone()
            }
        };
        let permit = semaphore.acquire_timeout(self.timeout).await;

        self.metrics
            .semaphore_acquire_seconds
            .observe(now.elapsed().as_secs_f64());
        info!("acquired permit {:?}", now.elapsed().as_secs_f64());
        Ok(WakeComputePermit { permit: permit? })
    }

    pub async fn garbage_collect_worker(&self) {
        if self.config.initial_limit == 0 {
            return;
        }
        let mut interval =
            tokio::time::interval(self.epoch / (self.node_locks.shards().len()) as u32);
        loop {
            for (i, shard) in self.node_locks.shards().iter().enumerate() {
                interval.tick().await;
                // temporary lock a single shard and then clear any semaphores that aren't currently checked out
                // race conditions: if strong_count == 1, there's no way that it can increase while the shard is locked
                // therefore releasing it is safe from race conditions
                info!(
                    name = self.name,
                    shard = i,
                    "performing epoch reclamation on api lock"
                );
                let mut lock = shard.write();
                let timer = self.metrics.reclamation_lag_seconds.start_timer();
                let count = lock
                    .extract_if(|_, semaphore| Arc::strong_count(semaphore.get_mut()) == 1)
                    .count();
                drop(lock);
                self.metrics.semaphores_unregistered.inc_by(count as u64);
                timer.observe();
            }
        }
    }
}

pub(crate) struct WakeComputePermit {
    permit: Token,
}

impl WakeComputePermit {
    pub(crate) fn should_check_cache(&self) -> bool {
        !self.permit.is_disabled()
    }
    pub(crate) fn release(self, outcome: Outcome) {
        self.permit.release(outcome);
    }
    pub(crate) fn release_result<T, E>(self, res: Result<T, E>) -> Result<T, E> {
        match res {
            Ok(_) => self.release(Outcome::Success),
            Err(_) => self.release(Outcome::Overload),
        }
        res
    }
}

impl FetchAuthRules for ControlPlaneBackend {
    async fn fetch_auth_rules(
        &self,
        ctx: &RequestMonitoring,
        endpoint: EndpointId,
    ) -> Result<Vec<AuthRule>, FetchAuthRulesError> {
        self.get_endpoint_jwks(ctx, endpoint)
            .await
            .map_err(FetchAuthRulesError::GetEndpointJwks)
    }
}
