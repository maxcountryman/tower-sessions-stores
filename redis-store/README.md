<h1 align="center">
    tower-sessions-redis-store
</h1>

<p align="center">
    Redis via `fred` session store for `tower-sessions`.
</p>

## 🤸 Usage

```rust
use std::net::SocketAddr;

use axum::{response::IntoResponse, routing::get, Router};
use serde::{Deserialize, Serialize};
use time::Duration;
use tower_sessions::{Expiry, Session, SessionManagerLayer};
use tower_sessions_redis_store::{fred::prelude::*, RedisStore};

const COUNTER_KEY: &str = "counter";

#[derive(Debug, Serialize, Deserialize, Default)]
struct Counter(usize);

async fn handler(session: Session) -> impl IntoResponse {
    let counter: Counter = session.get(COUNTER_KEY).await.unwrap().unwrap_or_default();
    session.insert(COUNTER_KEY, counter.0 + 1).await.unwrap();
    format!("Current count: {}", counter.0)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = Pool::new(Config::default(), None, None, None, 6)?;

    let redis_conn = pool.connect();
    pool.wait_for_connect().await?;

    let session_store = RedisStore::new(pool);
    let session_layer = SessionManagerLayer::new(session_store)
        .with_secure(false)
        .with_expiry(Expiry::OnInactivity(Duration::seconds(10)));

    let app = Router::new().route("/", get(handler)).layer(session_layer);

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app.into_make_service()).await?;

    redis_conn.await??;

    Ok(())
}
```
