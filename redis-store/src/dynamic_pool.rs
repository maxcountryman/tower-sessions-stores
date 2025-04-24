use crate::RedisStoreError;
use async_trait::async_trait;
use fred::{
    prelude::{DynamicPool, KeysInterface},
    types::{Expiration, SetOptions},
};
use std::fmt::Debug;
use time::OffsetDateTime;
use tower_sessions_core::{
    session::{Id, Record},
    session_store, SessionStore,
};

/// A Redis session store.
#[derive(Clone)]
pub struct RedisStore {
    client: DynamicPool,
}

impl Debug for RedisStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisStore")
            .field("client", &format_args!("DynamicPool{{...}}"))
            .finish()
    }
}

impl RedisStore {
    /// Create a new Redis store with the provided `DynamicPool` client.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use tower_sessions_redis_store::{fred::prelude::*, RedisStore};
    ///
    /// # tokio_test::block_on(async {
    /// let pool = DynamicPool::new(Config::default(), None, None, None, Default::default()).unwrap();
    ///
    /// let _ = pool.init().await.unwrap();
    /// pool.start_scale_task(Duration::from_secs(10));
    ///
    /// let session_store = RedisStore::new(pool);
    /// })
    /// ```
    pub fn new(client: DynamicPool) -> Self {
        Self { client }
    }

    async fn save_with_options(
        &self,
        record: &Record,
        options: Option<SetOptions>,
    ) -> session_store::Result<bool> {
        let expire = Some(Expiration::EXAT(OffsetDateTime::unix_timestamp(
            record.expiry_date,
        )));

        Ok(self
            .client
            .next()
            .set(
                record.id.to_string(),
                rmp_serde::to_vec(&record)
                    .map_err(RedisStoreError::Encode)?
                    .as_slice(),
                expire,
                options,
                false,
            )
            .await
            .map_err(RedisStoreError::Redis)?)
    }
}

#[async_trait]
impl SessionStore for RedisStore {
    async fn create(&self, record: &mut Record) -> session_store::Result<()> {
        loop {
            if !self.save_with_options(record, Some(SetOptions::NX)).await? {
                record.id = Id::default();
                continue;
            }
            break;
        }
        Ok(())
    }

    async fn save(&self, record: &Record) -> session_store::Result<()> {
        self.save_with_options(record, Some(SetOptions::XX)).await?;
        Ok(())
    }

    async fn load(&self, session_id: &Id) -> session_store::Result<Option<Record>> {
        let data = self
            .client
            .next()
            .get::<Option<Vec<u8>>, _>(session_id.to_string())
            .await
            .map_err(RedisStoreError::Redis)?;

        if let Some(data) = data {
            Ok(Some(
                rmp_serde::from_slice(&data).map_err(RedisStoreError::Decode)?,
            ))
        } else {
            Ok(None)
        }
    }

    async fn delete(&self, session_id: &Id) -> session_store::Result<()> {
        let _: () = self
            .client
            .next()
            .del(session_id.to_string())
            .await
            .map_err(RedisStoreError::Redis)?;
        Ok(())
    }
}
