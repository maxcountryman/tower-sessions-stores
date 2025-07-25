use crate::SqlxStoreError;
use tower_sessions_core::session::Record;

#[cfg(feature = "sqlx-chrono")]
use chrono::{DateTime, Utc};

#[cfg(feature = "sqlx-time")]
use time::OffsetDateTime;

#[cfg(feature = "sqlx-time")]
pub(crate) fn now() -> OffsetDateTime {
    OffsetDateTime::now_utc()
}

#[cfg(feature = "sqlx-chrono")]
pub(crate) fn now() -> DateTime<Utc> {
    Utc::now()
}

pub(crate) trait ExpirationTime {
    #[cfg(feature = "sqlx-time")]
    fn expiry_date(&self) -> Result<OffsetDateTime, SqlxStoreError>;

    #[cfg(feature = "sqlx-chrono")]
    fn expiry_date(&self) -> Result<DateTime<Utc>, SqlxStoreError>;
}

impl ExpirationTime for Record {
    #[cfg(feature = "sqlx-time")]
    fn expiry_date(&self) -> Result<OffsetDateTime, SqlxStoreError> {
        Ok(self.expiry_date)
    }

    #[cfg(feature = "sqlx-chrono")]
    fn expiry_date(&self) -> Result<DateTime<Utc>, SqlxStoreError> {
        DateTime::from_timestamp(
            self.expiry_date.unix_timestamp(),
            self.expiry_date.nanosecond(),
        )
        .ok_or(SqlxStoreError::Sqlx(sqlx::Error::InvalidArgument(
            "fail to convert date time".to_string(),
        )))
    }
}
