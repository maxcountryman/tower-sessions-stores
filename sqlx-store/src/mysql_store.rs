use async_trait::async_trait;
use sqlx::{MySqlConnection, MySqlPool};
use time::OffsetDateTime;
use tower_sessions_core::{
    session::{Id, Record},
    session_store, ExpiredDeletion, SessionStore,
};

use crate::SqlxStoreError;

/// A MySQL session store.
#[derive(Clone, Debug)]
pub struct MySqlStore {
    pool: MySqlPool,
    schema_name: String,
    table_name: String,
}

impl MySqlStore {
    /// Create a new MySqlStore store with the provided connection pool.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use tower_sessions_sqlx::{sqlx::MySqlPool, MySqlStore};
    ///
    /// # tokio_test::block_on(async {
    /// let database_url = std::option_env!("DATABASE_URL").unwrap();
    /// let pool = MySqlPool::connect(database_url).await.unwrap();
    /// let session_store = MySqlStore::new(pool);
    /// # })
    /// ```
    pub fn new(pool: MySqlPool) -> Self {
        Self {
            pool,
            schema_name: "tower_sessions".to_string(),
            table_name: "session".to_string(),
        }
    }

    /// Set the session table schema name with the provided name.
    pub fn with_schema_name(mut self, schema_name: impl AsRef<str>) -> Result<Self, String> {
        let schema_name = schema_name.as_ref();
        if !is_valid_identifier(schema_name) {
            return Err(format!(
                "Invalid schema name '{}'. Schema names must start with a letter or underscore \
                 (including letters with diacritical marks and non-Latin letters).Subsequent \
                 characters can be letters, underscores, digits (0-9), or dollar signs ($).",
                schema_name
            ));
        }

        schema_name.clone_into(&mut self.schema_name);
        Ok(self)
    }

    /// Set the session table name with the provided name.
    pub fn with_table_name(mut self, table_name: impl AsRef<str>) -> Result<Self, String> {
        let table_name = table_name.as_ref();
        if !is_valid_identifier(table_name) {
            return Err(format!(
                "Invalid table name '{}'. Table names must start with a letter or underscore \
                 (including letters with diacritical marks and non-Latin letters).Subsequent \
                 characters can be letters, underscores, digits (0-9), or dollar signs ($).",
                table_name
            ));
        }

        table_name.clone_into(&mut self.table_name);
        Ok(self)
    }

    /// Migrate the session schema.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use tower_sessions_sqlx::{sqlx::MySqlPool, MySqlStore};
    ///
    /// # tokio_test::block_on(async {
    /// let database_url = std::option_env!("DATABASE_URL").unwrap();
    /// let pool = MySqlPool::connect(database_url).await.unwrap();
    /// let session_store = MySqlStore::new(pool);
    /// session_store.migrate().await.unwrap();
    /// # })
    /// ```
    pub async fn migrate(&self) -> sqlx::Result<()> {
        let mut tx = self.pool.begin().await?;

        let create_schema_query = format!(
            "create schema if not exists {schema_name}",
            schema_name = self.schema_name,
        );
        sqlx::query(&create_schema_query).execute(&mut *tx).await?;

        let create_table_query = format!(
            r#"
            create table if not exists `{schema_name}`.`{table_name}`
            (
                id char(22) primary key not null,
                data blob not null,
                expiry_date timestamp(6) not null
            )
            "#,
            schema_name = self.schema_name,
            table_name = self.table_name
        );
        sqlx::query(&create_table_query).execute(&mut *tx).await?;

        tx.commit().await?;

        Ok(())
    }

    async fn id_exists(&self, conn: &mut MySqlConnection, id: &Id) -> session_store::Result<bool> {
        let query = format!(
            r#"
            select exists(select 1 from `{schema_name}`.`{table_name}` where id = ?)
            "#,
            schema_name = self.schema_name,
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
        conn: &mut MySqlConnection,
        record: &Record,
    ) -> session_store::Result<()> {
        let query = format!(
            r#"
            insert into `{schema_name}`.`{table_name}`
              (id, data, expiry_date) values (?, ?, ?)
            on duplicate key update
              data = values(data),
              expiry_date = values(expiry_date)
            "#,
            schema_name = self.schema_name,
            table_name = self.table_name
        );
        sqlx::query(&query)
            .bind(record.id.to_string())
            .bind(rmp_serde::to_vec(&record).map_err(SqlxStoreError::Encode)?)
            .bind(record.expiry_date)
            .execute(conn)
            .await
            .map_err(SqlxStoreError::Sqlx)?;
        Ok(())
    }
}

#[async_trait]
impl ExpiredDeletion for MySqlStore {
    async fn delete_expired(&self) -> session_store::Result<()> {
        let query = format!(
            r#"
            delete from `{schema_name}`.`{table_name}`
            where expiry_date < utc_timestamp()
            "#,
            schema_name = self.schema_name,
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
impl SessionStore for MySqlStore {
    async fn create(&self, record: &mut Record) -> session_store::Result<()> {
        let mut tx = self.pool.begin().await.map_err(SqlxStoreError::Sqlx)?;

        while self.id_exists(&mut tx, &record.id).await? {
            record.id = Id::default();
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
            select data from `{schema_name}`.`{table_name}`
            where id = ? and expiry_date > ?
            "#,
            schema_name = self.schema_name,
            table_name = self.table_name
        );
        let data: Option<(Vec<u8>,)> = sqlx::query_as(&query)
            .bind(session_id.to_string())
            .bind(OffsetDateTime::now_utc())
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
            r#"delete from `{schema_name}`.`{table_name}` where id = ?"#,
            schema_name = self.schema_name,
            table_name = self.table_name
        );
        sqlx::query(&query)
            .bind(session_id.to_string())
            .execute(&self.pool)
            .await
            .map_err(SqlxStoreError::Sqlx)?;

        Ok(())
    }
}

/// A valid MySQL identifier must start with a letter or underscore
/// (including letters with diacritical marks and non-Latin letters). Subsequent
/// characters in an identifier or keyword can be letters, underscores, digits
/// (0-9), or dollar signs ($).
/// See https://dev.mysql.com/doc/refman/8.4/en/identifiers.html for details.
fn is_valid_identifier(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .next()
            .map(|c| c.is_alphabetic() || c == '_')
            .unwrap_or_default()
        && name
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '$')
}
