pub use time;

use std::time::{Duration as StdDuration, Instant as StdInstant};

use async_trait::async_trait;
use moka::{future::Cache, Expiry};
use time::OffsetDateTime;
use tower_sessions_core::{
    session::{Id, Record},
    session_store, SessionStore,
};

/// A session store that uses Moka, a fast and concurrent caching library.
///
/// This store uses Moka's built-in time-based per-entry expiration policy
/// according to the session's expiry date. Therefore, expired sessions
/// are automatically removed from the cache.
#[derive(Debug, Clone)]
pub struct MokaStore {
    cache: Cache<Id, Record>,
}

impl MokaStore {
    /// Create a new Moka store with the provided maximum capacity.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tower_sessions::MemoryStore;
    /// use tower_sessions_moka_store::MokaStore;
    /// let session_store = MokaStore::new(Some(2_000));
    /// ```
    pub fn new(max_capacity: Option<u64>) -> Self {
        // it would be useful to expose more of the CacheBuilder options to the user,
        // but for now this is the most important one
        let cache_builder = match max_capacity {
            Some(capacity) => Cache::builder().max_capacity(capacity),
            None => Cache::builder(),
        }
        .expire_after(SessionExpiry);

        Self {
            cache: cache_builder.build(),
        }
    }
}

#[async_trait]
impl SessionStore for MokaStore {
    async fn create(&self, record: &mut Record) -> session_store::Result<()> {
        while self.cache.contains_key(&record.id) {
            record.id = Id::default();
        }
        self.cache.insert(record.id, record.clone()).await;
        Ok(())
    }

    async fn save(&self, record: &Record) -> session_store::Result<()> {
        self.cache.insert(record.id, record.clone()).await;
        Ok(())
    }

    async fn load(&self, session_id: &Id) -> session_store::Result<Option<Record>> {
        // expired sessions are automatically removed from the cache,
        // so it's safe to just call get
        Ok(self.cache.get(session_id).await)
    }

    async fn delete(&self, session_id: &Id) -> session_store::Result<()> {
        self.cache.invalidate(session_id).await;
        Ok(())
    }
}

/// Moka per-entry expiration policy for session records.
struct SessionExpiry;

impl SessionExpiry {
    /// Calculates the expiry duration of a record
    /// by comparing it to the current time.
    ///
    /// If the expiry date of the record is in the past,
    /// returns an empty duration.
    fn expiry_date_to_duration(record: &Record) -> StdDuration {
        // we use this to calculate the current time
        // because it is not possible to convert
        // StdInstant to OffsetDateTime
        let now = OffsetDateTime::now_utc();
        let expiry_date = record.expiry_date;

        if expiry_date > now {
            (expiry_date - now).unsigned_abs()
        } else {
            StdDuration::default()
        }
    }
}

impl Expiry<Id, Record> for SessionExpiry {
    fn expire_after_create(
        &self,
        _id: &Id,
        record: &Record,
        _created_at: StdInstant,
    ) -> Option<StdDuration> {
        Some(Self::expiry_date_to_duration(record))
    }

    fn expire_after_update(
        &self,
        _id: &Id,
        record: &Record,
        _updated_at: StdInstant,
        _duration_until_expiry: Option<StdDuration>,
    ) -> Option<StdDuration> {
        // expiry_date could change, so we calculate it again
        Some(Self::expiry_date_to_duration(record))
    }
}
