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
    /// Hard per-file deadline (seconds) covering all download attempts of a
    /// single dump file, so a server that never answers can't hang a run
    /// (and the locks it holds) forever.
    pub download_total_timeout_secs: u64,

    /// Languages considered "kept" (non-deleted) for books; anything else is
    /// soft-deleted at merge time. Comma-separated, lowercased, trimmed.
    /// Deliberately panics if the resulting list is empty: an empty list
    /// would flip `is_deleted = true` for the entire catalog.
    pub allowed_langs: Vec<String>,
    /// Max size of the deadpool-postgres connection pool. deadpool's default
    /// (`cpu_count * 4`) can silently serialize the 12 concurrent
    /// `stage_file` tasks (each holding a connection for the whole file)
    /// onto far fewer real connections than intended.
    pub postgres_max_pool_size: usize,
    /// How long (seconds) a pool checkout will wait for a free connection
    /// before giving up, set explicitly on the deadpool-postgres pool
    /// config rather than relying on deadpool's default (unbounded) wait.
    /// After Spec 14.1's one-pooled-client-per-`stage_file`-task model, a
    /// saturated pool (e.g. `postgres_max_pool_size` too small for the
    /// concurrent Phase A file tasks) should fail loudly and quickly rather
    /// than queue checkouts silently, which would otherwise look like a
    /// stall rather than a config problem.
    pub postgres_pool_wait_timeout_secs: u64,
    /// Minimum ratio of (rows staged this run) / (non-deleted rows currently
    /// in the DB for this source) required to proceed to the merge
    /// transaction. Guards against a truncated/partial upstream dump
    /// silently soft-deleting the whole catalog via the anti-join delete
    /// steps. Skipped when the DB-side count is 0 (first run for a source).
    pub min_staging_ratio: f64,
    /// Maximum tolerated ratio `skipped / (staged + skipped)` per dump file.
    /// Above it the run fails before the merge transaction.
    pub max_skipped_ratio: f64,
    /// `work_mem` used for the Phase B merge transaction (`SET LOCAL
    /// work_mem = ...`). The merge plan does several hash joins/aggregates
    /// over the staging tables, so a larger-than-default work_mem avoids
    /// disk spills. Overridable per-deployment via `MERGE_WORK_MEM`.
    pub merge_work_mem: String,
}

fn parse_allowed_langs(raw: &str) -> Vec<String> {
    let langs: Vec<String> = raw
        .split(',')
        .map(|s| s.trim().to_lowercase())
        .filter(|s| !s.is_empty())
        .collect();

    if langs.is_empty() {
        panic!(
            "ALLOWED_LANGS resolved to an empty list ({:?}); an empty allow-list would soft-delete the entire catalog",
            raw
        );
    }

    langs
}

fn get_env(env: &'static str) -> String {
    std::env::var(env).unwrap_or_else(|_| panic!("Cannot get the {} env variable", env))
}

/// Reads and parses an optional env variable. Unset -> `default`; set but
/// unparsable -> panic at startup (no silent fallback to the default).
fn get_env_or<T>(env: &'static str, default: T) -> T
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    match std::env::var(env) {
        Err(_) => default,
        Ok(v) => v
            .trim()
            .parse()
            .unwrap_or_else(|e| panic!("Invalid {env} env variable ({v:?}): {e}")),
    }
}

fn require_positive(env: &'static str, v: u64) -> u64 {
    if v == 0 {
        panic!("{env} must be greater than 0");
    }
    v
}

fn parse_api_key(raw: String) -> String {
    if raw.trim().is_empty() {
        panic!("API_KEY env variable must not be empty");
    }
    raw
}

/// `MIN_STAGING_RATIO` must be finite and in (0, 1]: 0 (or negative) would
/// disable the truncated-dump guard entirely.
fn validate_min_staging_ratio(v: f64) -> f64 {
    if !v.is_finite() || v <= 0.0 || v > 1.0 {
        panic!("MIN_STAGING_RATIO must be a finite number in (0, 1], got {v}");
    }
    v
}

/// `MAX_SKIPPED_RATIO` must be finite and in [0, 1].
fn validate_max_skipped_ratio(v: f64) -> f64 {
    if !v.is_finite() || !(0.0..=1.0).contains(&v) {
        panic!("MAX_SKIPPED_RATIO must be a finite number in [0, 1], got {v}");
    }
    v
}

impl Config {
    pub fn load() -> Config {
        Config {
            api_key: parse_api_key(get_env("API_KEY")),

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
            download_connect_timeout_secs: require_positive(
                "DOWNLOAD_CONNECT_TIMEOUT_SECS",
                get_env_or("DOWNLOAD_CONNECT_TIMEOUT_SECS", 10),
            ),
            download_idle_timeout_secs: require_positive(
                "DOWNLOAD_IDLE_TIMEOUT_SECS",
                get_env_or("DOWNLOAD_IDLE_TIMEOUT_SECS", 60),
            ),
            download_max_attempts: require_positive(
                "DOWNLOAD_MAX_ATTEMPTS",
                get_env_or("DOWNLOAD_MAX_ATTEMPTS", 3u64),
            ) as u32,
            download_total_timeout_secs: require_positive(
                "DOWNLOAD_TOTAL_TIMEOUT_SECS",
                get_env_or("DOWNLOAD_TOTAL_TIMEOUT_SECS", 1800),
            ),

            allowed_langs: parse_allowed_langs(&get_env_or(
                "ALLOWED_LANGS",
                "ru,be,uk".to_string(),
            )),
            postgres_max_pool_size: require_positive(
                "POSTGRES_MAX_POOL_SIZE",
                get_env_or("POSTGRES_MAX_POOL_SIZE", 16u64),
            ) as usize,
            postgres_pool_wait_timeout_secs: require_positive(
                "POSTGRES_POOL_WAIT_TIMEOUT_SECS",
                get_env_or("POSTGRES_POOL_WAIT_TIMEOUT_SECS", 10),
            ),
            min_staging_ratio: validate_min_staging_ratio(get_env_or("MIN_STAGING_RATIO", 0.5)),
            max_skipped_ratio: validate_max_skipped_ratio(get_env_or("MAX_SKIPPED_RATIO", 0.01)),
            merge_work_mem: get_env_or("MERGE_WORK_MEM", "256MB".to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic(expected = "API_KEY")]
    fn empty_api_key_is_rejected() {
        parse_api_key("   ".to_string());
    }

    #[test]
    fn min_staging_ratio_accepts_valid_values() {
        assert_eq!(validate_min_staging_ratio(0.5), 0.5);
        assert_eq!(validate_min_staging_ratio(1.0), 1.0);
    }

    #[test]
    #[should_panic(expected = "MIN_STAGING_RATIO")]
    fn min_staging_ratio_rejects_zero() {
        validate_min_staging_ratio(0.0);
    }

    #[test]
    #[should_panic(expected = "MIN_STAGING_RATIO")]
    fn min_staging_ratio_rejects_nan() {
        validate_min_staging_ratio(f64::NAN);
    }

    #[test]
    #[should_panic(expected = "MIN_STAGING_RATIO")]
    fn min_staging_ratio_rejects_above_one() {
        validate_min_staging_ratio(1.5);
    }

    #[test]
    #[should_panic(expected = "MAX_SKIPPED_RATIO")]
    fn max_skipped_ratio_rejects_negative() {
        validate_max_skipped_ratio(-0.1);
    }

    #[test]
    #[should_panic(expected = "Invalid CFG_TEST_UNPARSABLE")]
    fn unparsable_numeric_env_fails() {
        std::env::set_var("CFG_TEST_UNPARSABLE", "abc");
        let _: u64 = get_env_or("CFG_TEST_UNPARSABLE", 5);
    }
}

lazy_static! {
    pub static ref CONFIG: Config = Config::load();
}
