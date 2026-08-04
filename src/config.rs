use reqwest::header::{HeaderName, HeaderValue};
use serde::Deserialize;
use serde_json::Map;
use std::str::FromStr;

#[derive(Deserialize, Clone)]
pub enum Method {
    #[serde(rename = "get")]
    Get,
    #[serde(rename = "post")]
    Post,
}

#[derive(Clone)]
pub struct Webhook {
    pub method: Method,
    pub url: String,
    pub headers: Vec<(HeaderName, HeaderValue)>,
}

#[derive(Deserialize)]
struct RawWebhook {
    method: Method,
    url: String,
    headers: Map<String, serde_json::Value>,
}

fn build_webhook(raw: RawWebhook) -> Webhook {
    let headers = raw
        .headers
        .into_iter()
        .map(|(key, val)| {
            let value_str = match val {
                serde_json::Value::String(s) => s,
                other => panic!(
                    "WEBHOOKS: header '{key}' for url '{}' must be a string, got {other:?}",
                    raw.url
                ),
            };
            let name = HeaderName::from_str(&key).unwrap_or_else(|e| {
                panic!(
                    "WEBHOOKS: invalid header name '{key}' for url '{}': {e}",
                    raw.url
                )
            });
            let value = HeaderValue::from_str(&value_str).unwrap_or_else(|e| {
                panic!(
                    "WEBHOOKS: invalid header value for '{key}' on url '{}': {e}",
                    raw.url
                )
            });
            (name, value)
        })
        .collect();

    Webhook {
        method: raw.method,
        url: raw.url,
        headers,
    }
}

pub struct Config {
    pub api_key: String,

    pub sentry_dsn: String,

    pub postgres_db_name: String,
    pub postgres_host: String,
    pub postgres_port: u16,
    pub postgres_user: String,
    pub postgres_password: String,

    pub fl_base_url: String,

    pub webhooks: Vec<Webhook>,

    pub data_dir: String,
    pub download_connect_timeout_secs: u64,
    pub download_idle_timeout_secs: u64,
    pub download_max_attempts: u32,
}

fn get_env(env: &'static str) -> String {
    std::env::var(env).unwrap_or_else(|_| panic!("Cannot get the {} env variable", env))
}

fn get_env_or<T: std::str::FromStr>(env: &'static str, default: T) -> T {
    std::env::var(env)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

impl Config {
    pub fn load() -> Config {
        Config {
            api_key: get_env("API_KEY"),

            sentry_dsn: get_env("SENTRY_DSN"),

            postgres_db_name: get_env("POSTGRES_DB_NAME"),
            postgres_host: get_env("POSTGRES_HOST"),
            postgres_port: get_env("POSTGRES_PORT")
                .parse()
                .unwrap_or_else(|e| panic!("Invalid POSTGRES_PORT env variable: {e}")),
            postgres_user: get_env("POSTGRES_USER"),
            postgres_password: get_env("POSTGRES_PASSWORD"),

            fl_base_url: get_env("FL_BASE_URL"),

            webhooks: serde_json::from_str::<Vec<RawWebhook>>(&get_env("WEBHOOKS"))
                .unwrap_or_else(|e| panic!("Cannot parse WEBHOOKS env variable as JSON: {e}"))
                .into_iter()
                .map(build_webhook)
                .collect(),

            data_dir: get_env_or("DATA_DIR", "data".to_string()),
            download_connect_timeout_secs: get_env_or("DOWNLOAD_CONNECT_TIMEOUT_SECS", 10),
            download_idle_timeout_secs: get_env_or("DOWNLOAD_IDLE_TIMEOUT_SECS", 60),
            download_max_attempts: get_env_or("DOWNLOAD_MAX_ATTEMPTS", 3),
        }
    }
}

lazy_static! {
    pub static ref CONFIG: Config = Config::load();
}
