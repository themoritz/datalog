use anyhow::Result;
use axum::{
    debug_handler,
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use datalog::{pg_store::PgStore, transact::Update};
use tokio::runtime;

#[derive(Clone)]
struct App {
    pool: sqlx::PgPool,
}

impl App {
    async fn new() -> Result<Self> {
        let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            let def = "postgres://localhost/postgres".to_string();
            println!("DATABASE_URL not set, using default `{def}`");
            def
        });
        let pool = sqlx::PgPool::connect(&db_url).await?;

        Ok(App { pool })
    }
}

enum AppError {
    Failed,
    Sqlx(String),
}

impl From<sqlx::Error> for AppError {
    fn from(value: sqlx::Error) -> Self {
        AppError::Sqlx(value.to_string())
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        match self {
            AppError::Failed => {
                (StatusCode::INTERNAL_SERVER_ERROR, "failed".to_string()).into_response()
            }
            AppError::Sqlx(err) => (StatusCode::INTERNAL_SERVER_ERROR, err).into_response(),
        }
    }
}

#[tokio::main]
pub async fn main() -> Result<()> {
    let app = App::new().await?;

    let router = Router::new()
        .route("/tx", post(tx))
        .route("/snapshot", get(snapshot))
        .with_state(app);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;

    axum::serve(listener, router.into_make_service()).await?;

    Ok(())
}

#[debug_handler]
async fn tx(State(app): State<App>, Json(updates): Json<Vec<Update>>) -> Result<(), AppError> {
    let pg_store = PgStore::new(1, app.pool, runtime::Handle::current(), false);

    for update in updates {
        if update.add {
            pg_store.insert_async(update.e, update.a, update.v).await?;
        } else {
            pg_store.retract_async(update.e, update.a, update.v).await?;
        }
    }

    Ok(())
}

#[debug_handler]
async fn snapshot(State(app): State<App>) -> Result<Json<Vec<Update>>, AppError> {
    let pg_store = PgStore::new(1, app.pool, runtime::Handle::current(), false);
    Ok(Json(pg_store.snapshot().await?))
}
