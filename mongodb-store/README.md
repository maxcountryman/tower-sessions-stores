<h1 align="center">
    tower-sessions-mongodb-store
</h1>

<p align="center">
    MongoDB session store for `tower-sessions`.
</p>

## 🤸 Usage

```rust
use std::net::SocketAddr;

use axum::{response::IntoResponse, routing::get, Router};
use serde::{Deserialize, Serialize};
use time::Duration;
use tower_sessions::{Expiry, Session, SessionManagerLayer};
use tower_sessions_mongodb_store::{mongodb::Client, MongoDBStore};

const COUNTER_KEY: &str = "counter";

#[derive(Serialize, Deserialize, Default)]
struct Counter(usize);

async fn handler(session: Session) -> impl IntoResponse {
    let counter: Counter = session.get(COUNTER_KEY).await.unwrap().unwrap_or_default();
    session.insert(COUNTER_KEY, counter.0 + 1).await.unwrap();
    format!("Current count: {}", counter.0)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = std::option_env!("DATABASE_URL").expect("Missing DATABASE_URL.");
    let client = Client::with_uri_str(database_url).await?;
    let session_store = MongoDBStore::new(client, "tower-sessions".to_string());
    let session_layer = SessionManagerLayer::new(session_store)
        .with_secure(false)
        .with_expiry(Expiry::OnInactivity(Duration::seconds(10)));

    let app = Router::new().route("/", get(handler)).layer(session_layer);

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app.into_make_service()).await?;

    Ok(())
}
```
