pub use sqlx;
use tower_sessions_core::session_store;

#[cfg(any(
    all(feature = "time", feature = "chrono"),
    all(not(feature = "time"), not(feature = "chrono"))
))]
compile_error!("Exactly one of `time` and `chrono` features must be enabled. This is due to a change in sqlx where chrono types can only be enabled if time is disabled.");

#[cfg(feature = "mysql")]
#[cfg_attr(docsrs, doc(cfg(feature = "mysql")))]
pub use self::mysql_store::MySqlStore;
#[cfg(feature = "postgres")]
#[cfg_attr(docsrs, doc(cfg(feature = "postgres")))]
pub use self::postgres_store::PostgresStore;
#[cfg(feature = "sqlite")]
#[cfg_attr(docsrs, doc(cfg(feature = "sqlite")))]
pub use self::sqlite_store::SqliteStore;

#[cfg(feature = "sqlite")]
#[cfg_attr(docsrs, doc(cfg(feature = "sqlite")))]
mod sqlite_store;

#[cfg(feature = "postgres")]
#[cfg_attr(docsrs, doc(cfg(feature = "postgres")))]
mod postgres_store;

#[cfg(feature = "mysql")]
#[cfg_attr(docsrs, doc(cfg(feature = "mysql")))]
mod mysql_store;

/// An error type for SQLx stores.
#[derive(thiserror::Error, Debug)]
pub enum SqlxStoreError {
    /// A variant to map `sqlx` errors.
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),

    /// A variant to map `rmp_serde` encode errors.
    #[error(transparent)]
    Encode(#[from] rmp_serde::encode::Error),

    /// A variant to map `rmp_serde` decode errors.
    #[error(transparent)]
    Decode(#[from] rmp_serde::decode::Error),
}

impl From<SqlxStoreError> for session_store::Error {
    fn from(err: SqlxStoreError) -> Self {
        match err {
            SqlxStoreError::Sqlx(inner) => session_store::Error::Backend(inner.to_string()),
            SqlxStoreError::Decode(inner) => session_store::Error::Decode(inner.to_string()),
            SqlxStoreError::Encode(inner) => session_store::Error::Encode(inner.to_string()),
        }
    }
}

#[cfg(feature = "time")]
pub fn current_time() -> time::OffsetDateTime {
    time::OffsetDateTime::now_utc()
}

#[cfg(feature = "chrono")]
pub fn current_time() -> chrono::DateTime<chrono::Utc> {
    chrono::Utc::now()
}

#[cfg(feature = "time")]
pub fn convert_expiry_date(expiry_date: time::OffsetDateTime) -> time::OffsetDateTime {
    expiry_date
}

#[cfg(feature = "chrono")]
pub fn convert_expiry_date(expiry_date: time::OffsetDateTime) -> chrono::DateTime<chrono::Utc> {
    // if we can't convert the expiry date to a chrono type, return the current time i.e. effectively assume our session has expired
    chrono::DateTime::from_timestamp(expiry_date.unix_timestamp(), expiry_date.nanosecond())
        .unwrap_or(chrono::Utc::now())
}
