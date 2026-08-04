use axum::{
    http::{HeaderMap, StatusCode},
    routing::post,
    Router,
};
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

/// Constant-time comparison of two byte slices (Spec 06.3): avoids leaking
/// timing information about *where* the provided API key first differs
/// from the configured one. Deliberately hand-rolled instead of pulling in
/// the `subtle` crate, since this is the only place a constant-time compare
/// is needed and the implementation is small and easy to audit: the loop
/// always walks the full length of both slices (no early `return` on the
/// first mismatching byte) and accumulates differences via bitwise OR
/// rather than `==`/`!=`, so the number of loop iterations and branches
/// taken is independent of where (or whether) the inputs differ. The one
/// piece of information this does *not* hide is whether the lengths match
/// (handled up front) - that only reveals the key's length, not its
/// content, and the length check itself takes the same amount of time
/// regardless of content.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }

    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }

    diff == 0
}

/// Checks the `Authorization` header against `expected_api_key` and, if it
/// matches, attempts to start an update run. Split out from `update()` so
/// tests can exercise the full auth + status-code logic (Spec 06.1/06.2)
/// without requiring `config::CONFIG` (and therefore every env var it
/// reads) to be initialized.
async fn handle_update(headers: &HeaderMap, expected_api_key: &str) -> (StatusCode, &'static str) {
    let api_key = match headers.get("Authorization") {
        Some(v) => v,
        None => return (StatusCode::UNAUTHORIZED, "No api-key!"),
    };

    // `HeaderValue::to_str()` errors on non-visible-ASCII bytes (Spec
    // 06.1); a remote caller can send arbitrary bytes in this header
    // without needing to know anything about the real key, so this must be
    // handled as "unauthorized", not unwrapped/panicked on.
    let api_key_str = match api_key.to_str() {
        Ok(v) => v,
        Err(_) => return (StatusCode::UNAUTHORIZED, "Wrong api-key!"),
    };

    if !constant_time_eq(expected_api_key.as_bytes(), api_key_str.as_bytes()) {
        return (StatusCode::UNAUTHORIZED, "Wrong api-key!");
    }

    match updater::try_start_update() {
        updater::UpdateStart::Started => (StatusCode::ACCEPTED, "Update started"),
        updater::UpdateStart::Busy => (StatusCode::CONFLICT, "Update already running"),
    }
}

async fn update(headers: HeaderMap) -> (StatusCode, &'static str) {
    handle_update(&headers, &config::CONFIG.api_key).await
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    const TEST_KEY: &str = "correct-horse-battery-staple";

    #[test]
    fn constant_time_eq_matches_equal_slices() {
        assert!(constant_time_eq(b"same", b"same"));
    }

    #[test]
    fn constant_time_eq_rejects_different_content_same_length() {
        assert!(!constant_time_eq(b"aaaa", b"aaab"));
    }

    #[test]
    fn constant_time_eq_rejects_different_length() {
        assert!(!constant_time_eq(b"short", b"much longer value"));
    }

    #[tokio::test]
    async fn update_missing_header_returns_401() {
        let headers = HeaderMap::new();
        let (status, _) = handle_update(&headers, TEST_KEY).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn update_invalid_header_bytes_return_401_without_panicking() {
        let mut headers = HeaderMap::new();
        // `HeaderValue::from_bytes` accepts non-UTF8/non-ASCII opaque bytes;
        // `to_str()` on it fails. This must not panic the handler (Spec
        // 06.1), and must be treated as unauthorized (Spec 06.2).
        headers.insert(
            "Authorization",
            HeaderValue::from_bytes(&[0xFF, 0xFE, 0xFD]).unwrap(),
        );

        let (status, _) = handle_update(&headers, TEST_KEY).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn update_wrong_key_returns_401() {
        let mut headers = HeaderMap::new();
        headers.insert("Authorization", HeaderValue::from_static("wrong-key"));

        let (status, _) = handle_update(&headers, TEST_KEY).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }
}
