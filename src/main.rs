#[macro_use]
extern crate lazy_static;

pub mod config;
pub mod types;
pub mod updater;
pub mod utils;

use axum::{http::HeaderMap, routing::post, Router};
use dotenvy::dotenv;
use sentry::{integrations::debug_images::DebugImagesIntegration, types::Dsn, ClientOptions};
use sentry_tracing::EventFilter;
use std::{net::SocketAddr, str::FromStr};
use tower_http::trace::{self, TraceLayer};
use tracing::log;
use tracing::Level;
use tracing_subscriber::filter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use crate::updater::cron_jobs;

async fn health() -> &'static str {
    "OK"
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
            Err(err) => log::info!("Updater err: {:?}", err),
        };
    });

    "Update started"
}

async fn start_app() {
    let app = Router::new()
        .route("/health", axum::routing::get(health))
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
        dsn: Some(Dsn::from_str(&config::CONFIG.sentry_dsn).unwrap()),
        default_integrations: false,
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

    tokio::join![cron_jobs(), start_app()];
}
