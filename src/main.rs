use axum::{http::HeaderMap, routing::post, Router};
use dotenvy::dotenv;
use library_updater::{
    config,
    updater::{self, cron_jobs},
};
use sentry::{integrations::debug_images::DebugImagesIntegration, types::Dsn, ClientOptions};
use sentry_tracing::EventFilter;
use std::{net::SocketAddr, str::FromStr};
use tower_http::trace::{self, TraceLayer};
use tracing::log;
use tracing::Level;
use tracing_subscriber::filter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

async fn health() -> &'static str {
    "OK"
}

async fn status() -> axum::Json<serde_json::Value> {
    let snap = updater::RUN_STATE.snapshot();
    axum::Json(serde_json::json!({
        "running": snap.running,
        "last_start": snap.last_start,
        "last_finish": snap.last_finish,
        "last_success_at": snap.last_success_at,
        "last_result": snap.last_result,
        "rows_processed_total": snap.rows_processed_total,
        "rows_skipped_total": snap.rows_skipped_total,
        "errors_total": snap.errors_total,
        "webhook_errors_total": snap.webhook_errors_total,
        "last_webhook_error": snap.last_webhook_error,
    }))
}

async fn metrics() -> String {
    let snap = updater::RUN_STATE.snapshot();
    format!(
        "# HELP update_running 1 if an update is currently running\n\
# TYPE update_running gauge\n\
update_running {}\n\
# HELP update_last_success_timestamp Unix timestamp of last successful update\n\
# TYPE update_last_success_timestamp gauge\n\
update_last_success_timestamp {}\n\
# HELP update_rows_processed_total Total rows upserted across all update runs\n\
# TYPE update_rows_processed_total counter\n\
update_rows_processed_total {}\n\
# HELP update_rows_skipped_total Total rows skipped (parse errors) across all update runs\n\
# TYPE update_rows_skipped_total counter\n\
update_rows_skipped_total {}\n\
# HELP update_errors_total Total failed update runs\n\
# TYPE update_errors_total counter\n\
update_errors_total {}\n\
# HELP update_webhook_errors_total Total webhook delivery failures across all update runs\n\
# TYPE update_webhook_errors_total counter\n\
update_webhook_errors_total {}\n",
        if snap.running { 1 } else { 0 },
        snap.last_success_at,
        snap.rows_processed_total,
        snap.rows_skipped_total,
        snap.errors_total,
        snap.webhook_errors_total,
    )
}

async fn update(headers: HeaderMap) -> &'static str {
    let config_api_key = config::CONFIG.api_key.clone();

    let api_key = match headers.get("Authorization") {
        Some(v) => v,
        None => return "No api-key!",
    };

    if config_api_key != api_key.to_str().unwrap() {
        return "Wrong api-key!";
    }

    tokio::spawn(async {
        match updater::update().await {
            Ok(_) => log::info!("Updated!"),
            Err(err) => {
                log::error!("Updater err: {:?}", err);
                sentry::capture_error(err.as_ref());
            }
        };
    });

    "Update started"
}

async fn start_app() {
    let app = Router::new()
        .route("/health", axum::routing::get(health))
        .route("/status", axum::routing::get(status))
        .route("/metrics", axum::routing::get(metrics))
        .route("/update", post(update))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(trace::DefaultMakeSpan::new().level(Level::INFO))
                .on_response(trace::DefaultOnResponse::new().level(Level::INFO)),
        );

    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));

    log::info!("Start webserver...");
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
    log::info!("Webserver shutdown...")
}

#[tokio::main]
async fn main() {
    dotenv().ok();

    let options = ClientOptions {
        dsn: Some(
            Dsn::from_str(&config::CONFIG.sentry_dsn)
                .unwrap_or_else(|e| panic!("Invalid SENTRY_DSN env variable: {e}")),
        ),
        default_integrations: true,
        ..Default::default()
    }
    .add_integration(DebugImagesIntegration::new());

    let _guard = sentry::init(options);

    let sentry_layer = sentry_tracing::layer().event_filter(|md| match md.level() {
        &tracing::Level::ERROR => EventFilter::Event,
        _ => EventFilter::Ignore,
    });

    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_target(false))
        .with(filter::LevelFilter::INFO)
        .with(sentry_layer)
        .init();

    let cron_task = async {
        match cron_jobs().await {
            Ok(_) => {}
            Err(err) => log::error!("cron_jobs failed to start: {:?}", err),
        }
    };

    tokio::join![cron_task, start_app()];
}
