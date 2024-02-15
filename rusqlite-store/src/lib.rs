use async_trait::async_trait;
use rusqlite::OptionalExtension;
use time::OffsetDateTime;
pub use tokio_rusqlite;
use tokio_rusqlite::{params, Connection, Result as SqlResult};
use tower_sessions_core::{
    session::{Id, Record},
    session_store::{self, ExpiredDeletion},
    SessionStore,
};

/// An error type for Rusqlite stores.
#[derive(thiserror::Error, Debug)]
pub enum RusqliteStoreError {
    /// A variant to map `rusqlite` errors.
    #[error(transparent)]
    TokioRusqlite(#[from] tokio_rusqlite::Error),

    /// A variant to map `rmp_serde` encode errors.
    #[error(transparent)]
    Encode(#[from] rmp_serde::encode::Error),

    /// A variant to map `rmp_serde` decode errors.
    #[error(transparent)]
    Decode(#[from] rmp_serde::decode::Error),
}

impl From<RusqliteStoreError> for session_store::Error {
    fn from(err: RusqliteStoreError) -> Self {
        match err {
            RusqliteStoreError::TokioRusqlite(inner) => {
                session_store::Error::Backend(inner.to_string())
            }
            RusqliteStoreError::Decode(inner) => session_store::Error::Decode(inner.to_string()),
            RusqliteStoreError::Encode(inner) => session_store::Error::Encode(inner.to_string()),
        }
    }
}

#[derive(Clone, Debug)]
pub struct RusqliteStore {
    conn: Connection,
    table_name: String,
}

impl RusqliteStore {
    /// Create a new SQLite store with the provided connection.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use tower_sessions_rusqlite_store::{tokio_rusqlite::Connection, RusqliteStore};
    ///
    /// # tokio_test::block_on(async {
    /// let conn = Connection::open_in_memory().await.unwrap();
    /// let session_store = RusqliteStore::new(conn);
    /// # })
    /// ```
    pub fn new(conn: Connection) -> Self {
        Self {
            conn,
            table_name: "tower_sessions".into(),
        }
    }

    /// Set the session table name with the provided name.
    pub fn with_table_name(mut self, table_name: impl AsRef<str>) -> Result<Self, String> {
        let table_name = table_name.as_ref();
        if !is_valid_table_name(table_name) {
            return Err(format!(
                "Invalid table name '{}'. Table names must be alphanumeric and may contain \
                 hyphens or underscores.",
                table_name
            ));
        }

        self.table_name = table_name.to_owned();
        Ok(self)
    }

    /// Migrate the session schema.
    pub async fn migrate(&self) -> SqlResult<()> {
        let conn = self.conn.clone();
        let query = format!(
            r#"
            create table if not exists {}
            (
                id text primary key not null,
                data blob not null,
                expiry_date integer not null
            )
            "#,
            self.table_name
        );
        conn.call(
            move |conn| conn.execute(&query, []).map_err(|e| e.into()), // Convert to tokio_rusqlite::Error
        )
        .await?;

        Ok(())
    }
}

#[async_trait]
impl ExpiredDeletion for RusqliteStore {
    async fn delete_expired(&self) -> session_store::Result<()> {
        let conn = self.conn.clone();
        let query = format!(
            r#"
            delete from {table_name}
            where expiry_date < datetime('now', 'utc')
            "#,
            table_name = self.table_name
        );
        conn.call(move |conn| {
            conn.execute(&query, [OffsetDateTime::now_utc().unix_timestamp()])
                .map_err(|e| e.into())
        })
        .await
        .map_err(RusqliteStoreError::TokioRusqlite)?;

        Ok(())
    }
}

#[async_trait]
impl SessionStore for RusqliteStore {
    async fn save(&self, record: &Record) -> session_store::Result<()> {
        let conn = self.conn.clone();

        conn.call({
            let table_name = self.table_name.clone();
            let record_id = record.id.to_string();
            let record_data = rmp_serde::to_vec(record).map_err(RusqliteStoreError::Encode)?;
            let record_expiry = record.expiry_date;

            move |conn| {
                let query = format!(
                    r#"
                insert into {}
                (id, data, expiry_date) values (?, ?, ?)
                on conflict(id) do update set
                data = excluded.data,
                expiry_date = excluded.expiry_date
                "#,
                    table_name
                );
                conn.execute(
                    &query,
                    params![record_id, record_data, record_expiry.unix_timestamp()],
                )
                .map_err(|e| e.into())
            }
        })
        .await
        .map_err(RusqliteStoreError::TokioRusqlite)?;

        Ok(())
    }

    async fn load(&self, session_id: &Id) -> session_store::Result<Option<Record>> {
        let conn = self.conn.clone();

        let data = conn
            .call({
                let table_name = self.table_name.clone();
                let session_id = session_id.to_string();
                move |conn| {
                    let query = format!(
                        r#"
                    select data from {}
                    where id = ? and expiry_date > ?
                    "#,
                        table_name
                    );
                    let mut stmt = conn.prepare(&query)?;
                    stmt.query_row(
                        params![session_id, OffsetDateTime::now_utc().unix_timestamp()],
                        |row| {
                            let data: Vec<u8> = row.get(0)?;
                            Ok(data)
                        },
                    )
                    .optional()
                    .map_err(|e| e.into())
                }
            })
            .await
            .map_err(RusqliteStoreError::TokioRusqlite)?;

        match data {
            Some(data) => {
                let record: Record =
                    rmp_serde::from_slice(&data).map_err(RusqliteStoreError::Decode)?;
                Ok(Some(record))
            }
            None => Ok(None),
        }
    }

    async fn delete(&self, session_id: &Id) -> session_store::Result<()> {
        let conn = self.conn.clone();

        conn.call({
            let table_name = self.table_name.clone();
            let session_id = session_id.to_string();
            move |conn| {
                let query = format!(
                    r#"
                delete from {} where id = ?
                "#,
                    table_name
                );
                conn.execute(&query, params![session_id])
                    .map_err(|e| e.into())
            }
        })
        .await
        .map_err(RusqliteStoreError::TokioRusqlite)?;

        Ok(())
    }
}

fn is_valid_table_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}
