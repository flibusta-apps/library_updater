use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::config::{self, Webhook};
use deadpool_postgres::{Config, CreatePoolError, ManagerConfig, Pool, RecyclingMethod, Runtime};
use futures::{io::copy, Stream, StreamExt, TryStreamExt};
use reqwest::header::HeaderMap;
use tokio::fs::File;
use tokio_cron_scheduler::{Job, JobScheduler};
use tokio_postgres::NoTls;
use tracing::log;

use async_compression::futures::bufread::GzipDecoder;

use crate::types::{
    Author, AuthorAnnotation, AuthorAnnotationPic, BookAnnotation, BookAnnotationPic, BookAuthor,
    BookGenre, FromVecExpression, Genre, Sequence, SequenceInfo, Translator, Update,
};
use crate::utils::read_lines;
use sql_parse::{
    parse_statement, InsertReplace, InsertReplaceType, Issues, ParseOptions, SQLArguments,
    SQLDialect, Statement,
};
use tokio_util::compat::TokioAsyncReadCompatExt;

use crate::types::Book;

lazy_static! {
    static ref HTTP_CLIENT: reqwest::Client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(
            config::CONFIG.download_connect_timeout_secs
        ))
        .build()
        .expect("failed to build download http client");
}

/// Wraps a byte stream so that if no new chunk arrives within `idle`, the
/// stream yields a single `TimedOut` io::Error instead of hanging forever.
fn with_idle_timeout<S>(
    stream: S,
    idle: Duration,
) -> impl Stream<Item = Result<bytes::Bytes, std::io::Error>>
where
    S: Stream<Item = reqwest::Result<bytes::Bytes>> + Unpin,
{
    futures::stream::unfold(stream, move |mut s| async move {
        match tokio::time::timeout(idle, futures::StreamExt::next(&mut s)).await {
            Ok(Some(Ok(chunk))) => Some((Ok(chunk), s)),
            Ok(Some(Err(err))) => Some((Err(std::io::Error::other(err)), s)),
            Ok(None) => None,
            Err(_) => Some((
                Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!("download stalled: no bytes received for {idle:?}"),
                )),
                s,
            )),
        }
    })
}

/// Performs a single download attempt: GET the url, stream+decompress the
/// gzip body into `part_path`, and (if the server advertised a
/// `Content-Length`) verify the compressed byte count matches.
async fn download_attempt(
    client: &reqwest::Client,
    url: &str,
    part_path: &Path,
    idle: Duration,
) -> Result<(), Box<dyn std::error::Error + Send>> {
    let response = match client.get(url).send().await {
        Ok(v) => v,
        Err(err) => return Err(Box::new(err)),
    };

    let response = match response.error_for_status() {
        Ok(v) => v,
        Err(err) => return Err(Box::new(err)),
    };

    let expected_len = response.content_length();

    let mut file = match File::create(part_path).await {
        Ok(v) => v.compat(),
        Err(err) => {
            log::error!("Can't create {}: {:?}", part_path.display(), err);
            return Err(Box::new(err));
        }
    };

    let byte_count = Arc::new(AtomicU64::new(0));
    let byte_count_clone = byte_count.clone();

    let stream = with_idle_timeout(Box::pin(response.bytes_stream()), idle);
    let counted_stream = stream.inspect(move |chunk| {
        if let Ok(chunk) = chunk {
            byte_count_clone.fetch_add(chunk.len() as u64, Ordering::Relaxed);
        }
    });

    let data = counted_stream.into_async_read();
    let decoder = GzipDecoder::new(data);

    match copy(decoder, &mut file).await {
        Ok(_) => (),
        Err(err) => {
            return Err(Box::new(err));
        }
    };

    if let Some(expected) = expected_len {
        let actual = byte_count.load(Ordering::Relaxed);
        if actual != expected {
            return Err(Box::new(std::io::Error::other(format!(
                "downloaded {actual} compressed bytes but Content-Length was {expected}"
            ))));
        }
    }

    Ok(())
}

/// Downloads `url` into `<dest_dir>/<file_name>.part`, retrying up to
/// `max_attempts` times with exponential backoff, and only renames the part
/// file to `<dest_dir>/<file_name>` on full success. Any failure at any
/// point during an attempt deletes the `.part` file before retrying/failing.
async fn download_file_with_client(
    client: &reqwest::Client,
    url: &str,
    dest_dir: &Path,
    file_name: &str,
    idle: Duration,
    max_attempts: u32,
) -> Result<(), Box<dyn std::error::Error + Send>> {
    log::info!("Download {file_name}...");

    let final_path = dest_dir.join(file_name);
    let part_path = dest_dir.join(format!("{file_name}.part"));

    let mut last_err: Option<Box<dyn std::error::Error + Send>> = None;

    for attempt in 0..max_attempts {
        let attempt_result = download_attempt(client, url, &part_path, idle).await;

        match attempt_result {
            Ok(()) => match tokio::fs::rename(&part_path, &final_path).await {
                Ok(()) => {
                    log::info!("{file_name} downloaded!");
                    return Ok(());
                }
                Err(err) => {
                    log::error!(
                        "download attempt {} for {file_name} failed to rename part file: {err}",
                        attempt + 1
                    );
                    match tokio::fs::remove_file(&part_path).await {
                        Ok(_) => (),
                        Err(rm_err) => log::debug!("Can't remove part file: {:?}", rm_err),
                    };
                    last_err = Some(Box::new(err));
                }
            },
            Err(err) => {
                log::error!(
                    "download attempt {} for {file_name} failed: {err}",
                    attempt + 1
                );
                match tokio::fs::remove_file(&part_path).await {
                    Ok(_) => (),
                    Err(rm_err) => log::debug!("Can't remove part file: {:?}", rm_err),
                };
                last_err = Some(err);
            }
        }

        if attempt + 1 < max_attempts {
            let backoff_ms = 500u64 * 2u64.pow(attempt);
            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
        }
    }

    Err(last_err.unwrap_or_else(|| {
        Box::new(std::io::Error::other(format!(
            "download failed for {file_name} after {max_attempts} attempts"
        )))
    }))
}

async fn download_file(
    dest_dir: &Path,
    file_name: &str,
) -> Result<(), Box<dyn std::error::Error + Send>> {
    let link = format!("{}/sql/{file_name}.gz", &config::CONFIG.fl_base_url);

    download_file_with_client(
        &HTTP_CLIENT,
        &link,
        dest_dir,
        file_name,
        Duration::from_secs(config::CONFIG.download_idle_timeout_secs),
        config::CONFIG.download_max_attempts,
    )
    .await
}

async fn process<T>(
    pool: Pool,
    source_id: i16,
    file_name: &str,
    deps: Vec<tokio::sync::watch::Receiver<Option<UpdateStatus>>>,
) -> Result<(), Box<dyn std::error::Error + Send>>
where
    T: Debug + FromVecExpression<T> + Update,
{
    let data_dir = PathBuf::from(&config::CONFIG.data_dir);
    let final_path = data_dir.join(file_name);

    let result = process_inner::<T>(pool, source_id, file_name, &data_dir, &final_path, deps).await;

    // Always clean up the decompressed dump after processing, regardless of
    // whether processing succeeded or failed.
    match tokio::fs::remove_file(&final_path).await {
        Ok(_) => (),
        Err(err) => log::debug!("Can't remove {}: {:?}", final_path.display(), err),
    };

    result
}

async fn process_inner<T>(
    pool: Pool,
    source_id: i16,
    file_name: &str,
    data_dir: &Path,
    final_path: &Path,
    deps: Vec<tokio::sync::watch::Receiver<Option<UpdateStatus>>>,
) -> Result<(), Box<dyn std::error::Error + Send>>
where
    T: Debug + FromVecExpression<T> + Update,
{
    for mut dep in deps {
        let failed = match dep.wait_for(|s| s.is_some()).await {
            Ok(guard) => matches!(*guard, Some(UpdateStatus::Fail)),
            Err(_) => true, // sender dropped without setting a status (e.g. producer panicked) => treat as failure
        };
        if failed {
            return Err(Box::new(std::io::Error::other(format!(
                "dependency failed, aborting {file_name}"
            ))));
        }
    }

    let start_time = std::time::Instant::now();

    match download_file(data_dir, file_name).await {
        Ok(_) => (),
        Err(err) => return Err(err),
    };

    let parse_options = ParseOptions::new()
        .dialect(SQLDialect::MariaDB)
        .arguments(SQLArguments::QuestionMark)
        .warn_unquoted_identifiers(true);

    let lines = read_lines(final_path);

    let lines = match lines {
        Ok(v) => v,
        Err(err) => return Err(Box::new(err)),
    };

    let before_update_client = match pool.get().await {
        Ok(c) => c,
        Err(err) => return Err(Box::new(err)),
    };

    match T::before_update(&before_update_client).await {
        Ok(_) => (),
        Err(err) => return Err(err),
    };

    log::info!("Start update {file_name}...");

    let mut parse_error_count: u32 = 0;

    let mut upserted_count: u32 = 0;

    let mut line_no: u64 = 0;

    for line in lines.into_iter() {
        line_no += 1;

        let line = match line {
            Ok(line) => line,
            Err(err) => {
                return Err(Box::new(std::io::Error::new(
                    err.kind(),
                    format!("{file_name}: invalid data at/after line {line_no}: {err}"),
                )));
            }
        };

        let mut issues = Issues::new(&line);
        let ast = parse_statement(&line, &mut issues, &parse_options);

        if let Some(Statement::InsertReplace(
            i @ InsertReplace {
                type_: InsertReplaceType::Insert(_),
                ..
            },
        )) = ast
        {
            for value in i.values.into_iter() {
                for t_value in value.1.into_iter() {
                    let value = match T::from_vec_expression(&t_value) {
                        Ok(value) => value,
                        Err(e) => {
                            log::error!("Parse error in {file_name}: {:?}", e);
                            parse_error_count += 1;
                            continue;
                        }
                    };

                    let client = match pool.get().await {
                        Ok(c) => c,
                        Err(err) => return Err(Box::new(err)),
                    };

                    match value.update(&client, source_id).await {
                        Ok(_) => {
                            // log::info!("{:?}", value);
                            upserted_count += 1;
                        }
                        Err(err) => {
                            log::error!("Update error: {:?} : {:?}", value, err);
                            return Err(err);
                        }
                    }
                }
            }
        }
    }

    let after_update_client = match pool.get().await {
        Ok(c) => c,
        Err(err) => return Err(Box::new(err)),
    };

    match T::after_update(&after_update_client).await {
        Ok(_) => (),
        Err(err) => return Err(err),
    };

    log::info!(
        "{file_name} summary: parsed={} upserted={} skipped={} duration={:.2}s",
        upserted_count + parse_error_count,
        upserted_count,
        parse_error_count,
        start_time.elapsed().as_secs_f64()
    );
    RUN_STATE
        .rows_processed_total
        .fetch_add(upserted_count as u64, Ordering::Relaxed);
    RUN_STATE
        .rows_skipped_total
        .fetch_add(parse_error_count as u64, Ordering::Relaxed);

    if parse_error_count > 0 {
        log::error!("{file_name}: {parse_error_count} row(s) failed to parse");
        return Err(Box::new(std::io::Error::other(format!(
            "{file_name}: {parse_error_count} row(s) failed to parse"
        ))));
    }

    log::info!("Updated {file_name}...");

    Ok(())
}

async fn run_task<T>(
    pool: Pool,
    source_id: i16,
    file_name: &'static str,
    deps: Vec<tokio::sync::watch::Receiver<Option<UpdateStatus>>>,
    status_tx: tokio::sync::watch::Sender<Option<UpdateStatus>>,
) -> (&'static str, Result<(), Box<dyn std::error::Error + Send>>)
where
    T: Debug + FromVecExpression<T> + Update,
{
    let result = process::<T>(pool, source_id, file_name, deps).await;
    let status = if result.is_ok() {
        UpdateStatus::Success
    } else {
        RUN_STATE.errors_total.fetch_add(1, Ordering::Relaxed);
        UpdateStatus::Fail
    };
    let _ = status_tx.send(Some(status));
    (file_name, result)
}

async fn get_postgres_pool() -> Result<Pool, CreatePoolError> {
    let mut config = Config::new();

    config.host = Some(config::CONFIG.postgres_host.clone());
    config.port = Some(config::CONFIG.postgres_port);
    config.dbname = Some(config::CONFIG.postgres_db_name.clone());
    config.user = Some(config::CONFIG.postgres_user.clone());
    config.password = Some(config::CONFIG.postgres_password.clone());
    config.connect_timeout = Some(std::time::Duration::from_secs(5));
    config.manager = Some(ManagerConfig {
        recycling_method: RecyclingMethod::Verified,
    });

    match config.create_pool(Some(Runtime::Tokio1), NoTls) {
        Ok(pool) => Ok(pool),
        Err(err) => Err(err),
    }
}

async fn get_source(pool: Pool) -> Result<i16, Box<dyn std::error::Error>> {
    let client = match pool.get().await {
        Ok(c) => c,
        Err(err) => return Err(Box::new(err)),
    };

    let row = match client
        .query_one("SELECT id FROM sources WHERE name = 'flibusta';", &[])
        .await
    {
        Ok(v) => v,
        Err(err) => return Err(Box::new(err)),
    };

    let id = row.get(0);

    Ok(id)
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum UpdateStatus {
    Success,
    Fail,
}

const WEBHOOK_MAX_ATTEMPTS: u32 = 3;
const WEBHOOK_RETRY_BACKOFFS_MS: [u64; 2] = [500, 1500];

async fn send_webhooks() -> Result<(), String> {
    let client = match reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
    {
        Ok(v) => v,
        Err(err) => {
            let msg = format!("failed to build webhook http client: {err}");
            log::error!("{msg}");
            return Err(msg);
        }
    };

    let mut failures: Vec<String> = Vec::new();

    for webhook in config::CONFIG.webhooks.iter() {
        let Webhook {
            method,
            url,
            headers,
        } = webhook.clone();

        let mut last_error: Option<String> = None;

        for attempt in 0..WEBHOOK_MAX_ATTEMPTS {
            let builder = match method {
                config::Method::Get => client.get(url.clone()),
                config::Method::Post => client.post(url.clone()),
            };

            let request_headers = HeaderMap::from_iter(headers.clone());

            let response = builder.headers(request_headers).send().await;

            let attempt_result = match response {
                Ok(v) => match v.error_for_status() {
                    Ok(_) => Ok(()),
                    Err(err) => Err(err.to_string()),
                },
                Err(err) => Err(err.to_string()),
            };

            match attempt_result {
                Ok(_) => {
                    last_error = None;
                    break;
                }
                Err(err) => {
                    last_error = Some(err);
                    if attempt < WEBHOOK_MAX_ATTEMPTS - 1 {
                        let backoff_ms = WEBHOOK_RETRY_BACKOFFS_MS[attempt as usize];
                        tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                    }
                }
            }
        }

        if let Some(err) = last_error {
            let msg = format!("webhook {url} failed: {err}");
            log::error!("{msg}");
            failures.push(msg);
        }
    }

    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("; "))
    }
}

lazy_static! {
    pub static ref UPDATE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::new(());
}

pub struct RunState {
    pub running: AtomicBool,
    pub last_start: AtomicI64,
    pub last_finish: AtomicI64,
    pub last_success_at: AtomicI64,
    pub last_result: std::sync::RwLock<Option<String>>,
    pub rows_processed_total: AtomicU64,
    pub rows_skipped_total: AtomicU64,
    pub errors_total: AtomicU64,
    pub webhook_errors_total: AtomicU64,
    pub last_webhook_error: std::sync::RwLock<Option<String>>,
}

pub struct StatusSnapshot {
    pub running: bool,
    pub last_start: i64,
    pub last_finish: i64,
    pub last_success_at: i64,
    pub last_result: Option<String>,
    pub rows_processed_total: u64,
    pub rows_skipped_total: u64,
    pub errors_total: u64,
    pub webhook_errors_total: u64,
    pub last_webhook_error: Option<String>,
}

impl RunState {
    fn new() -> Self {
        Self {
            running: AtomicBool::new(false),
            last_start: AtomicI64::new(0),
            last_finish: AtomicI64::new(0),
            last_success_at: AtomicI64::new(0),
            last_result: std::sync::RwLock::new(None),
            rows_processed_total: AtomicU64::new(0),
            rows_skipped_total: AtomicU64::new(0),
            errors_total: AtomicU64::new(0),
            webhook_errors_total: AtomicU64::new(0),
            last_webhook_error: std::sync::RwLock::new(None),
        }
    }

    fn begin_run(&self) {
        self.running.store(true, Ordering::SeqCst);
        self.last_start
            .store(chrono::Utc::now().timestamp(), Ordering::SeqCst);
    }

    fn end_run(&self, success: bool, message: String) {
        self.running.store(false, Ordering::SeqCst);
        let now = chrono::Utc::now().timestamp();
        self.last_finish.store(now, Ordering::SeqCst);
        if success {
            self.last_success_at.store(now, Ordering::SeqCst);
        }
        *self.last_result.write().unwrap() = Some(message);
    }

    pub fn snapshot(&self) -> StatusSnapshot {
        StatusSnapshot {
            running: self.running.load(Ordering::SeqCst),
            last_start: self.last_start.load(Ordering::SeqCst),
            last_finish: self.last_finish.load(Ordering::SeqCst),
            last_success_at: self.last_success_at.load(Ordering::SeqCst),
            last_result: self.last_result.read().unwrap().clone(),
            rows_processed_total: self.rows_processed_total.load(Ordering::SeqCst),
            rows_skipped_total: self.rows_skipped_total.load(Ordering::SeqCst),
            errors_total: self.errors_total.load(Ordering::SeqCst),
            webhook_errors_total: self.webhook_errors_total.load(Ordering::SeqCst),
            last_webhook_error: self.last_webhook_error.read().unwrap().clone(),
        }
    }
}

lazy_static! {
    pub static ref RUN_STATE: RunState = RunState::new();
}

pub async fn update() -> Result<(), Box<dyn std::error::Error>> {
    let _lock = match UPDATE_LOCK.try_lock() {
        Ok(v) => v,
        Err(err) => return Err(Box::new(err)),
    };

    RUN_STATE.begin_run();
    let result = run_update_inner().await;
    match &result {
        Ok(_) => RUN_STATE.end_run(true, "success".to_string()),
        Err(err) => RUN_STATE.end_run(false, err.to_string()),
    }
    result
}

async fn run_update_inner() -> Result<(), Box<dyn std::error::Error>> {
    log::info!("Start update...");

    match tokio::fs::create_dir_all(&config::CONFIG.data_dir).await {
        Ok(_) => (),
        Err(err) => {
            log::error!(
                "Can't create data dir {}: {:?}",
                config::CONFIG.data_dir,
                err
            );
            return Err(Box::new(err));
        }
    };

    let pool = match get_postgres_pool().await {
        Ok(pool) => pool,
        Err(err) => return Err(Box::new(err)),
    };

    let source_id = match get_source(pool.clone()).await {
        Ok(v) => v,
        Err(err) => return Err(err),
    };

    let (author_tx, author_rx) = tokio::sync::watch::channel(None);
    let (book_tx, book_rx) = tokio::sync::watch::channel(None);
    let (book_author_tx, _book_author_rx) = tokio::sync::watch::channel(None);
    let (translator_tx, _translator_rx) = tokio::sync::watch::channel(None);
    let (sequence_tx, sequence_rx) = tokio::sync::watch::channel(None);
    let (sequence_info_tx, _sequence_info_rx) = tokio::sync::watch::channel(None);
    let (book_annotation_tx, book_annotation_rx) = tokio::sync::watch::channel(None);
    let (book_annotation_pics_tx, _book_annotation_pics_rx) = tokio::sync::watch::channel(None);
    let (author_annotation_tx, author_annotation_rx) = tokio::sync::watch::channel(None);
    let (author_annotation_pics_tx, _author_annotation_pics_rx) = tokio::sync::watch::channel(None);
    let (genre_tx, genre_rx) = tokio::sync::watch::channel(None);
    let (book_genre_tx, _book_genre_rx) = tokio::sync::watch::channel(None);

    let author_process = tokio::spawn(run_task::<Author>(
        pool.clone(),
        source_id,
        "lib.libavtorname.sql",
        vec![],
        author_tx,
    ));

    let book_process = tokio::spawn(run_task::<Book>(
        pool.clone(),
        source_id,
        "lib.libbook.sql",
        vec![],
        book_tx,
    ));

    let book_author_process = tokio::spawn(run_task::<BookAuthor>(
        pool.clone(),
        source_id,
        "lib.libavtor.sql",
        vec![author_rx.clone(), book_rx.clone()],
        book_author_tx,
    ));

    let translator_process = tokio::spawn(run_task::<Translator>(
        pool.clone(),
        source_id,
        "lib.libtranslator.sql",
        vec![author_rx.clone(), book_rx.clone()],
        translator_tx,
    ));

    let sequence_process = tokio::spawn(run_task::<Sequence>(
        pool.clone(),
        source_id,
        "lib.libseqname.sql",
        vec![],
        sequence_tx,
    ));

    let sequence_info_process = tokio::spawn(run_task::<SequenceInfo>(
        pool.clone(),
        source_id,
        "lib.libseq.sql",
        vec![book_rx.clone(), sequence_rx.clone()],
        sequence_info_tx,
    ));

    let book_annotation_process = tokio::spawn(run_task::<BookAnnotation>(
        pool.clone(),
        source_id,
        "lib.b.annotations.sql",
        vec![book_rx.clone()],
        book_annotation_tx,
    ));

    let book_annotation_pics_process = tokio::spawn(run_task::<BookAnnotationPic>(
        pool.clone(),
        source_id,
        "lib.b.annotations_pics.sql",
        vec![book_annotation_rx.clone()],
        book_annotation_pics_tx,
    ));

    let author_annotation_process = tokio::spawn(run_task::<AuthorAnnotation>(
        pool.clone(),
        source_id,
        "lib.a.annotations.sql",
        vec![author_rx.clone()],
        author_annotation_tx,
    ));

    let author_annotation_pics_process = tokio::spawn(run_task::<AuthorAnnotationPic>(
        pool.clone(),
        source_id,
        "lib.a.annotations_pics.sql",
        vec![author_annotation_rx.clone()],
        author_annotation_pics_tx,
    ));

    let genre_process = tokio::spawn(run_task::<Genre>(
        pool.clone(),
        source_id,
        "lib.libgenrelist.sql",
        vec![],
        genre_tx,
    ));

    let book_genre_process = tokio::spawn(run_task::<BookGenre>(
        pool.clone(),
        source_id,
        "lib.libgenre.sql",
        vec![genre_rx.clone(), book_rx.clone()],
        book_genre_tx,
    ));

    let handles = [
        author_process,
        book_process,
        book_author_process,
        translator_process,
        sequence_process,
        sequence_info_process,
        book_annotation_process,
        book_annotation_pics_process,
        author_annotation_process,
        author_annotation_pics_process,
        genre_process,
        book_genre_process,
    ];

    let mut failures: Vec<String> = Vec::new();

    for handle in handles {
        match handle.await {
            Ok((file_name, Ok(()))) => {
                let _ = file_name;
            }
            Ok((file_name, Err(err))) => {
                failures.push(format!("{file_name}: {err}"));
            }
            Err(join_err) => {
                failures.push(format!("join error: {join_err}"));
            }
        }
    }

    if !failures.is_empty() {
        for failure in &failures {
            log::error!("{failure}");
        }

        return Err(Box::new(std::io::Error::other(failures.join("; "))));
    }

    match send_webhooks().await {
        Ok(_) => {
            *RUN_STATE.last_webhook_error.write().unwrap() = None;
            log::info!("Webhooks sended!");
        }
        Err(err) => {
            RUN_STATE
                .webhook_errors_total
                .fetch_add(1, Ordering::Relaxed);
            *RUN_STATE.last_webhook_error.write().unwrap() = Some(err.clone());
            log::error!("Webhook delivery failed after successful DB update: {err}");
        }
    };

    Ok(())
}

pub async fn cron_jobs() -> Result<(), Box<dyn std::error::Error>> {
    let job_scheduler = match JobScheduler::new().await {
        Ok(v) => v,
        Err(err) => return Err(Box::new(err)),
    };

    let update_job = match Job::new_async("0 0 3 * * *", |_uuid, _l| {
        Box::pin(async {
            match update().await {
                Ok(_) => log::info!("Updated"),
                Err(err) => {
                    log::error!("Update err: {:?}", err);
                    sentry::capture_error(err.as_ref());
                }
            };
        })
    }) {
        Ok(v) => v,
        Err(err) => return Err(Box::new(err)),
    };

    match job_scheduler.add(update_job).await {
        Ok(_) => (),
        Err(err) => return Err(Box::new(err)),
    };

    log::info!("Scheduler start...");
    match job_scheduler.start().await {
        Ok(v) => v,
        Err(err) => return Err(Box::new(err)),
    };

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_compression::futures::bufread::GzipEncoder;
    use axum::{body::Body, http::header, response::Response, routing::get, Router};
    use futures::io::{AsyncReadExt, Cursor};
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::net::TcpListener;

    async fn spawn_server(router: Router) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        format!("http://{addr}")
    }

    async fn gzip_compress(data: &[u8]) -> Vec<u8> {
        let mut encoder = GzipEncoder::new(Cursor::new(data));
        let mut out = Vec::new();
        encoder.read_to_end(&mut out).await.unwrap();
        out
    }

    fn temp_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("lu_test_{name}_{nanos}"));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[tokio::test]
    async fn test_download_fails_and_cleans_up_on_truncated_content_length() {
        let dir = temp_dir("truncated");
        let file_name = "dump.sql";

        let compressed = gzip_compress(b"hello world, some test payload for gzip").await;
        let declared_len = compressed.len() as u64 + 1024;

        let router = Router::new().route(
            "/dump.sql.gz",
            get(move || {
                let compressed = compressed.clone();
                async move {
                    Response::builder()
                        .header(header::CONTENT_LENGTH, declared_len)
                        .body(Body::from_stream(futures::stream::once(async move {
                            Ok::<_, std::io::Error>(bytes::Bytes::from(compressed))
                        })))
                        .unwrap()
                }
            }),
        );

        let base_url = spawn_server(router).await;
        let url = format!("{base_url}/dump.sql.gz");

        let client = reqwest::Client::new();
        let result =
            download_file_with_client(&client, &url, &dir, file_name, Duration::from_secs(5), 1)
                .await;

        assert!(result.is_err(), "expected truncated download to fail");

        let final_path = dir.join(file_name);
        let part_path = dir.join(format!("{file_name}.part"));
        assert!(!final_path.exists(), "final file must not be left behind");
        assert!(!part_path.exists(), "part file must be cleaned up");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_download_fails_on_idle_timeout() {
        let dir = temp_dir("idle");
        let file_name = "dump.sql";

        let router = Router::new().route(
            "/dump.sql.gz",
            get(|| async {
                // Body that never yields any data, simulating a stalled connection.
                Response::builder()
                    .body(Body::from_stream(futures::stream::pending::<
                        Result<bytes::Bytes, std::io::Error>,
                    >()))
                    .unwrap()
            }),
        );

        let base_url = spawn_server(router).await;
        let url = format!("{base_url}/dump.sql.gz");

        let client = reqwest::Client::new();
        let idle = Duration::from_millis(200);

        let start = std::time::Instant::now();
        let result = download_file_with_client(&client, &url, &dir, file_name, idle, 1).await;
        let elapsed = start.elapsed();

        assert!(result.is_err(), "expected stalled download to fail");
        assert!(
            elapsed < Duration::from_secs(5),
            "download should fail quickly after idle timeout, took {elapsed:?}"
        );

        let final_path = dir.join(file_name);
        let part_path = dir.join(format!("{file_name}.part"));
        assert!(!final_path.exists(), "final file must not be left behind");
        assert!(!part_path.exists(), "part file must be cleaned up");

        let _ = std::fs::remove_dir_all(&dir);
    }
}
