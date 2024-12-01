use std::fmt::Debug;

use async_trait::async_trait;
pub use fred;
use fred::{
    prelude::KeysInterface,
    types::{Expiration, SetOptions},
};
use time::OffsetDateTime;
use tower_sessions_core::{
    session::{Id, Record},
    session_store, SessionStore,
};

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

/// A Redis session store.
#[derive(Debug, Clone, Default)]
pub struct RedisStore<C: KeysInterface + Send + Sync> {
    client: C,
}

impl<C: KeysInterface + Send + Sync> RedisStore<C> {
    /// Create a new Redis store with the provided client.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use tower_sessions_redis_store::{fred::prelude::*, RedisStore};
    ///
    /// # tokio_test::block_on(async {
    /// let pool = Pool::new(Config::default(), None, None, None, 6).unwrap();
    ///
    /// let _ = pool.connect();
    /// pool.wait_for_connect().await.unwrap();
    ///
    /// let session_store = RedisStore::new(pool);
    /// })
    /// ```
    pub fn new(client: C) -> Self {
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
impl<C> SessionStore for RedisStore<C>
where
    C: KeysInterface + Send + Sync + Debug + 'static,
{
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
            .del(session_id.to_string())
            .await
            .map_err(RedisStoreError::Redis)?;
        Ok(())
    }
}
