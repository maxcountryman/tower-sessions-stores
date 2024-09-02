use async_trait::async_trait;
use sqlx::{sqlite::SqlitePool, SqliteConnection};
use tower_sessions_core::{
    session::{Id, Record},
    session_store::{self, ExpiredDeletion},
    SessionStore,
};

use crate::{convert_expiry_date, current_time, SqlxStoreError};

/// A SQLite session store.
#[derive(Clone, Debug)]
pub struct SqliteStore {
    pool: SqlitePool,
    table_name: String,
}

impl SqliteStore {
    /// Create a new SQLite store with the provided connection pool.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use tower_sessions_sqlx_store::{sqlx::SqlitePool, SqliteStore};
    ///
    /// # tokio_test::block_on(async {
    /// let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
    /// let session_store = SqliteStore::new(pool);
    /// # })
    /// ```
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            pool,
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

        table_name.clone_into(&mut self.table_name);
        Ok(self)
    }

    /// Migrate the session schema.
    pub async fn migrate(&self) -> sqlx::Result<()> {
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
        sqlx::query(&query).execute(&self.pool).await?;
        Ok(())
    }

    async fn id_exists(&self, conn: &mut SqliteConnection, id: &Id) -> session_store::Result<bool> {
        let query = format!(
            r#"
            select exists(select 1 from {table_name} where id = ?)
            "#,
            table_name = self.table_name
        );

        Ok(sqlx::query_scalar(&query)
            .bind(id.to_string())
            .fetch_one(conn)
            .await
            .map_err(SqlxStoreError::Sqlx)?)
    }

    async fn save_with_conn(
        &self,
        conn: &mut SqliteConnection,
        record: &Record,
    ) -> session_store::Result<()> {
        let query = format!(
            r#"
            insert into {table_name}
              (id, data, expiry_date) values (?, ?, ?)
            on conflict(id) do update set
              data = excluded.data,
              expiry_date = excluded.expiry_date
            "#,
            table_name = self.table_name
        );
        sqlx::query(&query)
            .bind(record.id.to_string())
            .bind(rmp_serde::to_vec(record).map_err(SqlxStoreError::Encode)?)
            .bind(convert_expiry_date(record.expiry_date))
            .execute(conn)
            .await
            .map_err(SqlxStoreError::Sqlx)?;

        Ok(())
    }
}

#[async_trait]
impl ExpiredDeletion for SqliteStore {
    async fn delete_expired(&self) -> session_store::Result<()> {
        let query = format!(
            r#"
            delete from {table_name}
            where expiry_date < datetime('now', 'utc')
            "#,
            table_name = self.table_name
        );
        sqlx::query(&query)
            .execute(&self.pool)
            .await
            .map_err(SqlxStoreError::Sqlx)?;
        Ok(())
    }
}

#[async_trait]
impl SessionStore for SqliteStore {
    async fn create(&self, record: &mut Record) -> session_store::Result<()> {
        let mut tx = self.pool.begin().await.map_err(SqlxStoreError::Sqlx)?;

        while self.id_exists(&mut tx, &record.id).await? {
            record.id = Id::default(); // Generate a new ID
        }
        self.save_with_conn(&mut tx, record).await?;

        tx.commit().await.map_err(SqlxStoreError::Sqlx)?;

        Ok(())
    }

    async fn save(&self, record: &Record) -> session_store::Result<()> {
        let mut conn = self.pool.acquire().await.map_err(SqlxStoreError::Sqlx)?;
        self.save_with_conn(&mut conn, record).await
    }

    async fn load(&self, session_id: &Id) -> session_store::Result<Option<Record>> {
        let query = format!(
            r#"
            select data from {}
            where id = ? and expiry_date > ?
            "#,
            self.table_name
        );
        let data: Option<(Vec<u8>,)> = sqlx::query_as(&query)
            .bind(session_id.to_string())
            .bind(current_time())
            .fetch_optional(&self.pool)
            .await
            .map_err(SqlxStoreError::Sqlx)?;

        if let Some((data,)) = data {
            Ok(Some(
                rmp_serde::from_slice(&data).map_err(SqlxStoreError::Decode)?,
            ))
        } else {
            Ok(None)
        }
    }

    async fn delete(&self, session_id: &Id) -> session_store::Result<()> {
        let query = format!(
            r#"
            delete from {} where id = ?
            "#,
            self.table_name
        );
        sqlx::query(&query)
            .bind(session_id.to_string())
            .execute(&self.pool)
            .await
            .map_err(SqlxStoreError::Sqlx)?;

        Ok(())
    }
}

fn is_valid_table_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}
