use async_trait::async_trait;
use bson::doc;
pub use mongodb;
use mongodb::{Client, Collection};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use tower_sessions_core::{
    session::{Id, Record},
    session_store, ExpiredDeletion, SessionStore,
};

/// An error type for `MongoDBStore`.
#[derive(thiserror::Error, Debug)]
pub enum MongoDBStoreError {
    /// A variant to map to `mongodb::error::Error` errors.
    #[error(transparent)]
    MongoDB(#[from] mongodb::error::Error),

    /// A variant to map `rmp_serde` encode errors.
    #[error(transparent)]
    Encode(#[from] rmp_serde::encode::Error),

    /// A variant to map `rmp_serde` decode errors.
    #[error(transparent)]
    Decode(#[from] rmp_serde::decode::Error),

    /// A variant to map `mongodb::bson` encode errors.
    #[error(transparent)]
    BsonSerialize(#[from] bson::ser::Error),
}

impl From<MongoDBStoreError> for session_store::Error {
    fn from(err: MongoDBStoreError) -> Self {
        match err {
            MongoDBStoreError::MongoDB(inner) => session_store::Error::Backend(inner.to_string()),
            MongoDBStoreError::Decode(inner) => session_store::Error::Decode(inner.to_string()),
            MongoDBStoreError::Encode(inner) => session_store::Error::Encode(inner.to_string()),
            MongoDBStoreError::BsonSerialize(inner) => {
                session_store::Error::Encode(inner.to_string())
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct MongoDBSessionRecord {
    #[serde(rename = "_id")]
    id: String,
    
    data: bson::Binary,

    #[serde(rename = "expireAt")]
    expiry_date: bson::DateTime,
}

impl TryFrom<&Record> for MongoDBSessionRecord {
    type Error = MongoDBStoreError;
    fn try_from(record: &Record) -> Result<Self, MongoDBStoreError> {
        Ok(MongoDBSessionRecord {
            id: record.id.to_string(),
            data: bson::Binary {
                subtype: bson::spec::BinarySubtype::Generic,
                bytes: rmp_serde::to_vec(record).map_err(MongoDBStoreError::Encode)?,
            },
            expiry_date: bson::DateTime::from(record.expiry_date),
        })
    }
}

/// A MongoDB session store.
#[derive(Clone, Debug)]
pub struct MongoDBStore {
    collection: Collection<MongoDBSessionRecord>,
}

impl MongoDBStore {
    /// Create a new MongoDBStore store with the provided connection pool.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use tower_sessions_mongodb_store::{mongodb::Client, MongoDBStore};
    ///
    /// # tokio_test::block_on(async {
    /// let database_url = std::option_env!("DATABASE_URL").unwrap();
    /// let client = Client::with_uri_str(database_url).await.unwrap();
    /// let session_store = MongoDBStore::new(client, "database".to_string());
    /// # })
    /// ```
    pub fn new(client: Client, database: String) -> Self {
        Self {
            collection: client.database(&database).collection("sessions"),
        }
    }
}

#[async_trait]
impl ExpiredDeletion for MongoDBStore {
    async fn delete_expired(&self) -> session_store::Result<()> {
        self.collection
            .delete_many(
                doc! { "expireAt": {"$lt": OffsetDateTime::now_utc()} },
            )
            .await
            .map_err(MongoDBStoreError::MongoDB)?;

        Ok(())
    }
}

#[async_trait]
impl SessionStore for MongoDBStore {
    async fn create(&self, record: &mut Record) -> session_store::Result<()> {
        loop {
            let mongo_record: MongoDBSessionRecord = (&*record).try_into()?;

            let result = self.collection
                .insert_one(mongo_record)
                .await;

            match result {
                Ok(_) => return Ok(()),
                Err(err) if matches!(*err.kind, 
                    mongodb::error::ErrorKind::Write(
                        mongodb::error::WriteFailure::WriteError(
                            mongodb::error::WriteError { code: 11000, .. } // duplicate key
                        )
                    )
                ) => {},
                Err(err) => return Err(MongoDBStoreError::MongoDB(err).into()),
            };
            
            record.id = Id::default();
        }
    }

    async fn save(&self, record: &Record) -> session_store::Result<()> {
        let mongo_record: MongoDBSessionRecord = record.try_into()?;

        self.collection
            .replace_one(
                doc! { "_id": record.id.to_string() },
                mongo_record,
            )
            .upsert(true)
            .await
            .map_err(MongoDBStoreError::MongoDB)?;

        Ok(())
    }

    async fn load(&self, session_id: &Id) -> session_store::Result<Option<Record>> {
        let doc = self
            .collection
            .find_one(
                doc! {
                    "_id": session_id.to_string(),
                    "expireAt": {"$gt": OffsetDateTime::now_utc()}
                },
            )
            .await
            .map_err(MongoDBStoreError::MongoDB)?;

        if let Some(doc) = doc {
            Ok(Some(
                rmp_serde::from_slice(&doc.data.bytes).map_err(MongoDBStoreError::Decode)?,
            ))
        } else {
            Ok(None)
        }
    }

    async fn delete(&self, session_id: &Id) -> session_store::Result<()> {
        self.collection
            .delete_one(doc! { "_id": session_id.to_string() })
            .await
            .map_err(MongoDBStoreError::MongoDB)?;

        Ok(())
    }
}
