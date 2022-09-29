#[macro_use]
extern crate lazy_static;

pub mod config;
pub mod types;
pub mod utils;
pub mod updater;

use std::net::SocketAddr;
use axum::{Router, routing::post, http::HeaderMap};

use crate::updater::cron_jobs;

async fn update(headers: HeaderMap) -> &'static str {
    let config_api_key = config::CONFIG.api_key.clone();

    let api_key = match headers.get("Authorization") {
        Some(v) => v,
        None => return "No api-key!",
    };

    if config_api_key != api_key.to_str().unwrap() {
        return "Wrong api-key!"
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
        .route("/update", post(update));

    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

#[tokio::main]
async fn main() {
    let _guard = sentry::init(config::CONFIG.sentry_dsn.clone());
    env_logger::init();

    tokio::spawn(async {
        match cron_jobs().await {
            Ok(_) => (),
            Err(e) => panic!("{:?}", e),
        }
    });

    start_app().await;
}
