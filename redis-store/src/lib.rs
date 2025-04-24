pub use fred;
use std::fmt::Debug;
use tower_sessions_core::session_store;

#[derive(Debug, thiserror::Error)]
pub enum RedisStoreError {
    #[error(transparent)]
    Redis(#[from] fred::error::Error),

    #[error(transparent)]
    Decode(#[from] rmp_serde::decode::Error),

    #[error(transparent)]
    Encode(#[from] rmp_serde::encode::Error),
}

impl From<RedisStoreError> for session_store::Error {
    fn from(err: RedisStoreError) -> Self {
        match err {
            RedisStoreError::Redis(inner) => session_store::Error::Backend(inner.to_string()),
            RedisStoreError::Decode(inner) => session_store::Error::Decode(inner.to_string()),
            RedisStoreError::Encode(inner) => session_store::Error::Encode(inner.to_string()),
        }
    }
}

#[cfg(feature = "dynamic-pool")]
pub mod dynamic_pool;
#[cfg(feature = "dynamic-pool")]
pub use dynamic_pool::*;

#[cfg(not(feature = "dynamic-pool"))]
pub mod generic;
#[cfg(not(feature = "dynamic-pool"))]
pub use generic::*;
