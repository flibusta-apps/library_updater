use serde::Deserialize;
use serde_json::Map;

#[derive(Deserialize, Clone)]
pub enum Method {
    #[serde(rename = "get")]
    Get,
    #[serde(rename = "post")]
    Post
}

#[derive(Deserialize, Clone)]
pub struct Webhook {
    pub method: Method,
    pub url: String,
    pub headers: Map<String, serde_json::Value>,
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
}

fn get_env(env: &'static str) -> String {
    std::env::var(env).unwrap_or_else(|_| panic!("Cannot get the {} env variable", env))
}

impl Config {
    pub fn load() -> Config {
        Config {
            api_key: get_env("API_KEY"),

            sentry_dsn: get_env("SENTRY_DSN"),

            postgres_db_name: get_env("POSTGRES_DB_NAME"),
            postgres_host: get_env("POSTGRES_HOST"),
            postgres_port: get_env("POSTGRES_PORT").parse().unwrap(),
            postgres_user: get_env("POSTGRES_USER"),
            postgres_password: get_env("POSTGRES_PASSWORD"),

            fl_base_url: get_env("FL_BASE_URL"),

            webhooks: serde_json::from_str(&get_env("WEBHOOKS")).unwrap(),
        }
    }
}

lazy_static! {
    pub static ref CONFIG: Config = Config::load();
}
