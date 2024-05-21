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
    // schema_name: String,
    table_name: String,
    data_key: String,
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
            // schema_name: "tower_sessions".to_string(),
            table_name: "session".to_string(),
            data_key: "axum-login.data".to_string(),
        }
    }

    pub fn with_data_key(mut self, data_key: impl AsRef<str>) -> Self {
        self.data_key = data_key.as_ref().to_string();
        self
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

        // let create_schema_query = format!(
        //     "create schema if not exists {schema_name}",
        //     schema_name = self.schema_name,
        // );
        // sqlx::query(&create_schema_query).execute(&mut *tx).await?;

        // let create_table_query = format!(
        //     r#"
        //     create table if not exists `{schema_name}`.`{table_name}`
        //     (
        //         id char(22) primary key not null,
        //         data blob not null,
        //         expiry_date timestamp(6) not null
        //     )
        //     "#,
        //     schema_name = self.schema_name,
        //     table_name = self.table_name
        // );
        let create_table_query = format!(
            r#"
            create table if not exists `{table_name}`
            (
                id char(22) primary key not null,
                user_id varchar(255) not null,
                data blob not null,
                user_agent varchar(255),
                expiry_date timestamp(6) not null
            )
            "#,
            table_name = self.table_name
        );
        sqlx::query(&create_table_query).execute(&mut *tx).await?;

        tx.commit().await?;

        Ok(())
    }

    async fn id_exists(&self, conn: &mut MySqlConnection, id: &Id) -> session_store::Result<bool> {
        // let query = format!(
        //     r#"
        //     select exists(select 1 from `{schema_name}`.`{table_name}` where id = ?)
        //     "#,
        //     schema_name = self.schema_name,
        //     table_name = self.table_name
        // );
        let query = format!(
            r#"
            select exists(select 1 from `{table_name}` where id = ?)
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
        conn: &mut MySqlConnection,
        record: &Record,
    ) -> session_store::Result<()> {
        // let query = format!(
        //     r#"
        //     insert into `{schema_name}`.`{table_name}`
        //       (id, data, expiry_date) values (?, ?, ?)
        //     on duplicate key update
        //       data = values(data),
        //       expiry_date = values(expiry_date)
        //     "#,
        //     schema_name = self.schema_name,
        //     table_name = self.table_name
        // );
        if let Some(data) = record.data.get(&self.data_key) {
            let user_id = data.get("user_id").map(|v| v.as_str()).flatten();
            if let Some(user_id) = user_id {
                let user_agent = data.get("user_agent").map(|v| v.as_str()).flatten();
                let query = format!(
                    r#"
                    insert into `{table_name}`
                      (id, user_id, data, user_agent, expiry_date) values (?, ?, ?, ?, ?)
                    on duplicate key update
                      data = values(data),
                      expiry_date = values(expiry_date)
                    "#,
                    table_name = self.table_name
                );
                sqlx::query(&query)
                    .bind(&record.id.to_string())
                    .bind(user_id)
                    .bind(rmp_serde::to_vec(&record).map_err(SqlxStoreError::Encode)?)
                    .bind(user_agent)
                    .bind(record.expiry_date)
                    .execute(conn)
                    .await
                    .map_err(SqlxStoreError::Sqlx)?;
                Ok(())
            } else {
                Err(session_store::Error::Backend(
                    "No user id in record".to_string(),
                ))
            }
        } else {
            Err(session_store::Error::Backend(
                "No user id in record".to_string(),
            ))
        }
    }
}

#[async_trait]
impl ExpiredDeletion for MySqlStore {
    async fn delete_expired(&self) -> session_store::Result<()> {
        // let query = format!(
        //     r#"
        //     delete from `{schema_name}`.`{table_name}`
        //     where expiry_date < utc_timestamp()
        //     "#,
        //     schema_name = self.schema_name,
        //     table_name = self.table_name
        // );
        let query = format!(
            r#"
            delete from `{table_name}`
            where expiry_date < utc_timestamp()
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
        // let query = format!(
        //     r#"
        //     select data from `{schema_name}`.`{table_name}`
        //     where id = ? and expiry_date > ?
        //     "#,
        //     schema_name = self.schema_name,
        //     table_name = self.table_name
        // );
        let query = format!(
            r#"
            select data from `{table_name}`
            where id = ? and expiry_date > ?
            "#,
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
        // let query = format!(
        //     r#"delete from `{schema_name}`.`{table_name}` where id = ?"#,
        //     schema_name = self.schema_name,
        //     table_name = self.table_name
        // );
        let query = format!(
            r#"delete from `{table_name}` where id = ?"#,
            table_name = self.table_name
        );
        sqlx::query(&query)
            .bind(&session_id.to_string())
            .execute(&self.pool)
            .await
            .map_err(SqlxStoreError::Sqlx)?;

        Ok(())
    }
}
