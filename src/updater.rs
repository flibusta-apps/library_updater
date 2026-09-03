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
    BookGenre, FromVecExpression, Genre, Sequence, SequenceInfo, Staged, Translator,
};
use crate::utils::read_lines;
use sql_parse::{
    parse_statement, Expression, InsertReplace, InsertReplaceType, Issues, ParseOptions,
    SQLArguments, SQLDialect, Statement,
};
use tokio_postgres::types::ToSql;
use tokio_postgres::Client;
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
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    log::info!("Download {file_name}...");

    let final_path = dest_dir.join(file_name);
    let part_path = dest_dir.join(format!("{file_name}.part"));

    let mut last_err: Option<Box<dyn std::error::Error + Send + Sync>> = None;

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
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let link = format!("{}/sql/{file_name}.gz", config::CONFIG.fl_base_url);

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

/// Number of rows successfully staged / skipped (failed to parse or failed
/// range conversion) for a single dump file.
#[derive(Debug, Default, Clone, Copy)]
struct StageStats {
    staged: u64,
    skipped: u64,
}

/// One Phase A `stage_file` task's outcome, tagged with its dump file name
/// for error reporting/logging (see `stage_task`/`aggregate_stage_outcomes`).
type StageOutcome = (
    &'static str,
    Result<StageStats, Box<dyn std::error::Error + Send + Sync>>,
);

/// Phase A: download one dump file and bulk-load it into its staging table
/// via `COPY ... FROM STDIN BINARY`. Uses exactly one pooled connection for
/// the whole file (unlike the old row-by-row `process::<T>`, which grabbed a
/// connection per row). Has no dependency on any other `stage_file` call -
/// all 12 entity types can load fully concurrently because resolution of
/// remote ids to local ids now happens JOIN-side in the Phase B merge
/// transaction rather than by choreographed ordering here.
async fn stage_file<T>(
    pool: Pool,
    file_name: &'static str,
) -> Result<StageStats, Box<dyn std::error::Error + Send + Sync>>
where
    T: Debug + FromVecExpression<T> + Staged,
{
    let data_dir = PathBuf::from(&config::CONFIG.data_dir);
    let final_path = data_dir.join(file_name);

    let result = stage_file_inner::<T>(pool, file_name, &data_dir, &final_path).await;

    // Always clean up the decompressed dump after processing, regardless of
    // whether processing succeeded or failed.
    match tokio::fs::remove_file(&final_path).await {
        Ok(_) => (),
        Err(err) => log::debug!("Can't remove {}: {:?}", final_path.display(), err),
    };

    result
}

/// Parse a single line from a Flibusta MySQL dump into the list of
/// value-rows found in its `INSERT` statement. Returns an empty `Vec` for
/// any line that isn't a recognized `INSERT` (blank lines, `LOCK TABLES`
/// statements, comments, a genuine parse failure, etc.) - not an error,
/// since dump files routinely contain such non-`INSERT` lines interleaved
/// with the data rows.
///
/// Pure and I/O-free (no DB, no filesystem): this is the "parse line ->
/// entities" step, kept deliberately separate from the `COPY`/IO loop in
/// `stage_file_inner` so it can be unit-tested directly, including from
/// `crate::types`'s `FromVecExpression` fixture tests.
pub fn parse_insert_values(line: &str) -> Vec<Vec<Expression<'_>>> {
    let parse_options = ParseOptions::new()
        .dialect(SQLDialect::MariaDB)
        .arguments(SQLArguments::QuestionMark)
        .warn_unquoted_identifiers(true);

    let mut issues = Issues::new(line);
    let ast = parse_statement(line, &mut issues, &parse_options);

    match ast {
        Some(Statement::InsertReplace(
            i @ InsertReplace {
                type_: InsertReplaceType::Insert(_),
                ..
            },
        )) => i.values.into_iter().flat_map(|v| v.1).collect(),
        _ => Vec::new(),
    }
}

/// One parsed-and-converted outcome flowing from the blocking parse worker
/// to the async COPY writer in `stage_file_inner`. `Row` carries data ready
/// to bind to the COPY sink; `Skipped` carries a human-readable reason for
/// a row that failed to parse or convert (bounded logging, see 14.4).
enum ParsedItem {
    Row(Vec<crate::types::Val>),
    Skipped(String),
}

/// Bounded channel capacity between the blocking parse worker and the async
/// COPY writer in `stage_file_inner`: gives backpressure so parsed rows
/// don't pile up in RAM ahead of a slow writer (Spec 14.2).
const PARSE_CHANNEL_CAPACITY: usize = 1024;

/// Cap on how many per-row skip failures get logged at ERROR (with detail)
/// per file; beyond this, only a final count is logged (Spec 14.4 — avoids
/// unbounded multi-KB log lines on a systematically bad dump).
const MAX_LOGGED_SKIPS_PER_FILE: u64 = 20;

async fn stage_file_inner<T>(
    pool: Pool,
    file_name: &str,
    data_dir: &Path,
    final_path: &Path,
) -> Result<StageStats, Box<dyn std::error::Error + Send + Sync>>
where
    T: Debug + FromVecExpression<T> + Staged,
{
    let start_time = std::time::Instant::now();

    download_file(data_dir, file_name).await?;

    let lines = read_lines(final_path)?;

    let client = pool.get().await?;

    let copy_sql = format!(
        "COPY {} ({}) FROM STDIN BINARY",
        T::STAGING_TABLE,
        T::COLUMNS.join(", ")
    );

    let sink = client.copy_in(&copy_sql).await?;
    let writer = tokio_postgres::binary_copy::BinaryCopyInWriter::new(sink, &T::column_types());
    futures::pin_mut!(writer);

    log::info!("Start staging {file_name}...");

    let (tx, mut rx) = tokio::sync::mpsc::channel::<ParsedItem>(PARSE_CHANNEL_CAPACITY);

    let producer_file_name = file_name.to_string();
    let producer_handle = tokio::task::spawn_blocking(
        move || -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            let file_name = producer_file_name;

            for (line_no, line) in (1_u64..).zip(lines) {
                let line = line.map_err(|err| {
                    Box::new(std::io::Error::new(
                        err.kind(),
                        format!("{file_name}: invalid data at/after line {line_no}: {err}"),
                    )) as Box<dyn std::error::Error + Send + Sync>
                })?;

                for t_value in parse_insert_values(&line) {
                    let value = match T::from_vec_expression(&t_value) {
                        Ok(value) => value,
                        Err(e) => {
                            if tx
                                .blocking_send(ParsedItem::Skipped(format!("{:?}", e)))
                                .is_err()
                            {
                                return Ok(());
                            }
                            continue;
                        }
                    };

                    let row = match value.to_row() {
                        Ok(row) => row,
                        Err(e) => {
                            if tx
                                .blocking_send(ParsedItem::Skipped(format!("{:?}", e)))
                                .is_err()
                            {
                                return Ok(());
                            }
                            continue;
                        }
                    };

                    if tx.blocking_send(ParsedItem::Row(row)).is_err() {
                        return Ok(());
                    }
                }
            }

            Ok(())
        },
    );

    let mut skipped: u64 = 0;
    let mut logged: u64 = 0;

    while let Some(item) = rx.recv().await {
        match item {
            ParsedItem::Row(row) => {
                let params: Vec<&(dyn ToSql + Sync)> =
                    row.iter().map(|v| v as &(dyn ToSql + Sync)).collect();

                writer.as_mut().write(&params).await?;
            }
            ParsedItem::Skipped(detail) => {
                skipped += 1;
                if logged < MAX_LOGGED_SKIPS_PER_FILE {
                    log::error!("Parse error in {file_name}: {detail}");
                    logged += 1;
                }
            }
        }
    }

    if skipped > logged {
        log::error!(
            "{file_name}: ... and {} more skipped rows",
            skipped - logged
        );
    }

    match producer_handle.await {
        Ok(result) => result?,
        Err(join_err) => {
            return Err(Box::new(std::io::Error::other(join_err.to_string())));
        }
    }

    let staged = writer.finish().await?;

    log::info!(
        "{file_name} summary: staged={staged} skipped={skipped} duration={:.2}s",
        start_time.elapsed().as_secs_f64()
    );

    Ok(StageStats { staged, skipped })
}

/// Wraps `stage_file` so the spawned task's result carries the file name for
/// error reporting/logging.
async fn stage_task<T>(
    pool: Pool,
    file_name: &'static str,
) -> (
    &'static str,
    Result<StageStats, Box<dyn std::error::Error + Send + Sync>>,
)
where
    T: Debug + FromVecExpression<T> + Staged,
{
    (file_name, stage_file::<T>(pool, file_name).await)
}

/// Builds the deadpool-postgres pool used for all catalog-update DB access.
///
/// **Security posture (Spec 06.5):** the connection is made with `NoTls`.
/// This is only acceptable when Postgres is reachable exclusively over a
/// private/isolated network (e.g. a VPC-internal address, container
/// network, or Unix socket) that an external attacker cannot reach or
/// intercept traffic on - if that assumption doesn't hold for a given
/// deployment, TLS must be enabled here instead. Separately, the Postgres
/// role configured via `POSTGRES_USER`/`POSTGRES_PASSWORD` should be granted
/// DML privileges only (`SELECT`/`INSERT`/`UPDATE`/`DELETE`/`COPY` on the
/// catalog tables) and *not* `CREATE`/DDL rights: `crate::schema::ensure()`
/// runs bootstrap DDL (table/function creation) and should be executed
/// separately, as a migration step, using a distinct, more-privileged role.
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
    let mut pool_config = deadpool_postgres::PoolConfig::new(config::CONFIG.postgres_max_pool_size);
    pool_config.timeouts.wait = Some(std::time::Duration::from_secs(
        config::CONFIG.postgres_pool_wait_timeout_secs,
    ));
    config.pool = Some(pool_config);

    match config.create_pool(Some(Runtime::Tokio1), NoTls) {
        Ok(pool) => Ok(pool),
        Err(err) => Err(err),
    }
}

async fn get_source(client: &Client) -> Result<i16, Box<dyn std::error::Error + Send + Sync>> {
    let row = client
        .query_one("SELECT id FROM sources WHERE name = 'flibusta';", &[])
        .await?;

    Ok(row.get(0))
}

/// Compares the number of rows staged for `staging_table` against the
/// number currently present in the DB (via `target_count_sql`, parameterized
/// by `$1 = source_id`) and fails if the staged count looks like a
/// truncated/partial dump. Skipped when the DB-side count is 0 (first run
/// ever for this source - there's nothing to compare against, and the
/// anti-join deletes are harmless on an empty table anyway).
///
/// This is a deliberate safety gate: the merge transaction's anti-join
/// deletes (`crate::merge::MERGE_PLAN`, steps "books soft-delete removed"
/// and friends) would otherwise happily wipe the whole catalog if a
/// truncated/corrupt dump made it through staging.
pub async fn check_staging_ratio(
    client: &Client,
    staging_table: &str,
    target_count_sql: &str,
    source_id: i16,
    label: &str,
    min_ratio: f64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let staged: i64 = client
        .query_one(&format!("SELECT COUNT(*) FROM {staging_table}"), &[])
        .await?
        .get(0);
    let target: i64 = client
        .query_one(target_count_sql, &[&source_id])
        .await?
        .get(0);

    if target == 0 {
        // First run ever for this source: nothing to compare against.
        return Ok(());
    }

    let ratio = staged as f64 / target as f64;

    if ratio < min_ratio {
        return Err(format!(
            "sanity check failed for {label}: staged={staged} target={target} ratio={ratio:.3} < min_staging_ratio={min_ratio} \
             (looks like a truncated/partial dump; aborting before the merge transaction to avoid wiping the catalog)"
        )
        .into());
    }

    Ok(())
}

pub async fn sanity_check(
    pool: &Pool,
    source_id: i16,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client = pool.get().await?;
    let min_ratio = config::CONFIG.min_staging_ratio;

    check_staging_ratio(
        &client,
        "staging_books",
        "SELECT COUNT(*) FROM books WHERE source = $1 AND NOT is_deleted",
        source_id,
        "books",
        min_ratio,
    )
    .await?;

    check_staging_ratio(
        &client,
        "staging_book_authors",
        "SELECT COUNT(*) FROM book_authors ba JOIN books b ON b.id = ba.book WHERE b.source = $1",
        source_id,
        "book_authors",
        min_ratio,
    )
    .await?;

    check_staging_ratio(
        &client,
        "staging_book_genres",
        "SELECT COUNT(*) FROM book_genres bg JOIN books b ON b.id = bg.book WHERE b.source = $1",
        source_id,
        "book_genres",
        min_ratio,
    )
    .await?;

    check_staging_ratio(
        &client,
        "staging_translations",
        "SELECT COUNT(*) FROM translations t JOIN books b ON b.id = t.book WHERE b.source = $1",
        source_id,
        "translations",
        min_ratio,
    )
    .await?;

    check_staging_ratio(
        &client,
        "staging_book_sequences",
        "SELECT COUNT(*) FROM book_sequences bs JOIN books b ON b.id = bs.book WHERE b.source = $1",
        source_id,
        "book_sequences",
        min_ratio,
    )
    .await?;

    Ok(())
}

/// Aggregates the per-file Phase A outcomes (after `JoinHandle`s have
/// already been awaited) into total `StageStats` and a list of failure
/// messages, one per failed file. Pure and I/O-free, so the orchestration
/// invariant it encodes - **any single failed `stage_file` task fails the
/// whole batch** (the caller only proceeds to `load_and_merge_transaction`,
/// i.e. only starts the merge transaction, when the returned failure list
/// is empty) - is directly unit-testable without a DB or network.
///
/// This replaces the old (Spec 01-obsoleted) 12-way watch-channel
/// dependency graph, where a failing dependency had to fail its specific
/// dependents within a bounded time. Phase A tasks are now fully
/// independent of each other, so the only orchestration invariant left to
/// prove is this simpler one: any failure anywhere in Phase A -> no Phase B.
fn aggregate_stage_outcomes(outcomes: Vec<StageOutcome>) -> (StageStats, Vec<String>) {
    let mut failures = Vec::new();
    let mut total_staged: u64 = 0;
    let mut total_skipped: u64 = 0;

    for (file_name, result) in outcomes {
        match result {
            Ok(stats) => {
                total_staged += stats.staged;
                total_skipped += stats.skipped;
            }
            Err(err) => failures.push(format!("{file_name}: {err}")),
        }
    }

    (
        StageStats {
            staged: total_staged,
            skipped: total_skipped,
        },
        failures,
    )
}

/// Phase A + Phase B: truncate staging tables, load all 12 dump files into
/// them concurrently (Phase A, no inter-task dependencies), sanity-check the
/// staged row counts, then run the whole `crate::merge::MERGE_PLAN` inside
/// one transaction (Phase B) so readers only ever see the previous complete
/// catalog or the new complete catalog for each entity - never a mix.
///
/// Returns the aggregate `StageStats` (rows staged/skipped across all 12
/// files) alongside the outcome, regardless of success or failure, so the
/// caller can persist them into `catalog_updates.rows_staged`/`rows_skipped`
/// even when the run fails partway through.
async fn load_and_merge(
    pool: &Pool,
    source_id: i16,
) -> (
    StageStats,
    Result<(), Box<dyn std::error::Error + Send + Sync>>,
) {
    let truncate_client = match pool.get().await {
        Ok(v) => v,
        Err(err) => return (StageStats::default(), Err(Box::new(err))),
    };
    if let Err(err) = truncate_client
        .batch_execute(crate::schema::TRUNCATE_ALL_STAGING_SQL)
        .await
    {
        return (StageStats::default(), Err(Box::new(err)));
    }
    drop(truncate_client);

    let handles = vec![
        tokio::spawn(stage_task::<Author>(pool.clone(), "lib.libavtorname.sql")),
        tokio::spawn(stage_task::<Book>(pool.clone(), "lib.libbook.sql")),
        tokio::spawn(stage_task::<BookAuthor>(pool.clone(), "lib.libavtor.sql")),
        tokio::spawn(stage_task::<Translator>(
            pool.clone(),
            "lib.libtranslator.sql",
        )),
        tokio::spawn(stage_task::<Sequence>(pool.clone(), "lib.libseqname.sql")),
        tokio::spawn(stage_task::<SequenceInfo>(pool.clone(), "lib.libseq.sql")),
        tokio::spawn(stage_task::<BookAnnotation>(
            pool.clone(),
            "lib.b.annotations.sql",
        )),
        tokio::spawn(stage_task::<BookAnnotationPic>(
            pool.clone(),
            "lib.b.annotations_pics.sql",
        )),
        tokio::spawn(stage_task::<AuthorAnnotation>(
            pool.clone(),
            "lib.a.annotations.sql",
        )),
        tokio::spawn(stage_task::<AuthorAnnotationPic>(
            pool.clone(),
            "lib.a.annotations_pics.sql",
        )),
        tokio::spawn(stage_task::<Genre>(pool.clone(), "lib.libgenrelist.sql")),
        tokio::spawn(stage_task::<BookGenre>(pool.clone(), "lib.libgenre.sql")),
    ];

    let mut outcomes: Vec<StageOutcome> = Vec::with_capacity(handles.len());

    for handle in handles {
        match handle.await {
            Ok((file_name, result)) => outcomes.push((file_name, result)),
            Err(join_err) => outcomes.push((
                "<task join error>",
                Err(Box::new(std::io::Error::other(join_err.to_string()))),
            )),
        }
    }

    for (file_name, result) in &outcomes {
        match result {
            Ok(stats) if stats.skipped > 0 => {
                log::warn!(
                    "{file_name}: {} row(s) skipped while staging",
                    stats.skipped
                );
            }
            Ok(_) => (),
            Err(_) => {
                RUN_STATE.errors_total.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    let (stats, failures) = aggregate_stage_outcomes(outcomes);

    RUN_STATE
        .rows_processed_total
        .fetch_add(stats.staged, Ordering::Relaxed);
    RUN_STATE
        .rows_skipped_total
        .fetch_add(stats.skipped, Ordering::Relaxed);

    if !failures.is_empty() {
        for failure in &failures {
            log::error!("{failure}");
        }
        // Staging tables are invisible to readers regardless of outcome, and
        // (per `aggregate_stage_outcomes`) a single failing stage_file task
        // is fatal to the whole Phase A batch, so it's safe - and required -
        // to bail out here without ever calling
        // `load_and_merge_transaction`, i.e. without touching production
        // tables. See `aggregate_stage_outcomes_one_failure_fails_the_whole_batch`
        // for the unit-level proof of this invariant (the direct
        // replacement for the old 12-way dependency-graph orchestration
        // test, which Spec 01 made obsolete by removing the dependency
        // graph entirely - Phase A tasks are now fully independent).
        return (stats, Err(failures.join("; ").into()));
    }

    match load_and_merge_transaction(pool, source_id).await {
        Ok(()) => (stats, Ok(())),
        Err(err) => {
            RUN_STATE.errors_total.fetch_add(1, Ordering::Relaxed);
            (stats, Err(err))
        }
    }
}

/// Phase B proper: sanity-check the staged row counts, then run the whole
/// `crate::merge::MERGE_PLAN` inside one transaction.
async fn load_and_merge_transaction(
    pool: &Pool,
    source_id: i16,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    sanity_check(pool, source_id).await?;

    let mut client = pool.get().await?;
    let tx = client.transaction().await?;

    tx.batch_execute(&format!(
        "SET LOCAL statement_timeout = '30min'; SET LOCAL lock_timeout = '30s'; SET LOCAL work_mem = '{}';",
        config::CONFIG.merge_work_mem
    ))
    .await?;

    tx.batch_execute(crate::schema::ANALYZE_ALL_STAGING_SQL)
        .await?;

    for step in crate::merge::MERGE_PLAN {
        log::info!("merge step: {}", step.name);
        match step.params {
            crate::merge::Params::None => {
                tx.batch_execute(step.sql).await?;
            }
            crate::merge::Params::Source => {
                tx.execute(step.sql, &[&source_id]).await?;
            }
            crate::merge::Params::SourceLangs => {
                tx.execute(step.sql, &[&source_id, &config::CONFIG.allowed_langs])
                    .await?;
            }
        }
    }

    tx.commit().await?;

    Ok(())
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

/// Runs one full update while holding `_lock` (the caller is required to
/// have already acquired `UPDATE_LOCK`), recording start/end into
/// `RUN_STATE`. Shared by both `update()` (used by `cron_jobs()`, which
/// awaits the result directly) and `try_start_update()` (used by the
/// `/update` HTTP handler, which spawns this as a background task) so the
/// begin/run/end bookkeeping only lives in one place.
async fn run_and_record(
    _lock: tokio::sync::MutexGuard<'static, ()>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    RUN_STATE.begin_run();
    let result = run_update_inner().await;
    match &result {
        Ok(_) => RUN_STATE.end_run(true, "success".to_string()),
        Err(err) => RUN_STATE.end_run(false, err.to_string()),
    }
    result
}

pub async fn update() -> Result<(), Box<dyn std::error::Error>> {
    let lock = match UPDATE_LOCK.try_lock() {
        Ok(v) => v,
        Err(err) => return Err(Box::new(err)),
    };

    // Widen `Box<dyn Error + Send + Sync>` (used internally so the update
    // future stays `Send` across the advisory-lock release await) to the
    // plain `Box<dyn Error>` of this function's public signature. A no-op
    // unsized coercion, not a lossy conversion - the source chain consumed
    // by `sentry::capture_error` in main.rs is unaffected.
    run_and_record(lock)
        .await
        .map_err(|err| err as Box<dyn std::error::Error>)
}

/// Outcome of `try_start_update()`: whether an update run was actually
/// (synchronously, before returning) confirmed to have started, or whether
/// one was already in progress.
#[derive(Debug, PartialEq, Eq)]
pub enum UpdateStart {
    /// `UPDATE_LOCK` was free; a background task has been spawned to run
    /// the update.
    Started,
    /// `UPDATE_LOCK` was already held by another in-progress run; nothing
    /// was started.
    Busy,
}

/// Non-async, non-blocking entry point for the `/update` HTTP handler
/// (Spec 06.4): attempts `UPDATE_LOCK.try_lock()` itself so the caller
/// learns synchronously whether an update run actually started, without
/// having to await any of the update's own async work. On success, spawns
/// a task that takes ownership of the lock guard and runs
/// `run_and_record()` (the same begin/run/end + result-logging/Sentry-
/// capture behavior `update()`'s callers already rely on).
pub fn try_start_update() -> UpdateStart {
    match UPDATE_LOCK.try_lock() {
        Ok(lock) => {
            tokio::spawn(async move {
                match run_and_record(lock).await {
                    Ok(_) => log::info!("Updated!"),
                    Err(err) => {
                        log::error!("Updater err: {:?}", err);
                        sentry::capture_error(err.as_ref());
                    }
                }
            });
            UpdateStart::Started
        }
        Err(_) => UpdateStart::Busy,
    }
}

/// Postgres advisory lock key for the whole catalog-update run. A fixed,
/// well-known string hashed via `hashtext` rather than a magic numeric
/// constant, to make the lock's purpose obvious in `pg_locks`.
const ADVISORY_LOCK_SQL_KEY: &str = "library_updater:catalog_update";

async fn run_update_inner() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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

    let client = pool.get().await?;
    crate::schema::ensure(&client).await?;

    // Postgres-level advisory lock, in addition to the process-local
    // `UPDATE_LOCK` mutex: the mutex only prevents two update runs within
    // the *same* process from overlapping, but this service can run as
    // multiple replicas / get restarted mid-run, and all instances share
    // the same mutable staging tables (a TRUNCATE from one racing a COPY
    // from another would corrupt an in-flight load). Held on a dedicated
    // connection for the whole run and explicitly released before that
    // connection goes back to the pool, since deadpool reuses sessions and
    // an un-released advisory lock would otherwise "leak" onto whichever
    // future pool user happens to be handed this same backend.
    let lock_client = pool.get().await?;
    let locked: bool = lock_client
        .query_one(
            "SELECT pg_try_advisory_lock(hashtext($1))",
            &[&ADVISORY_LOCK_SQL_KEY],
        )
        .await?
        .get(0);

    if !locked {
        return Err(format!(
            "another catalog update is already holding the Postgres advisory lock ({ADVISORY_LOCK_SQL_KEY}); refusing to start a concurrent run"
        )
        .into());
    }

    let source_id = get_source(&client).await?;

    let result = run_update_body(&pool, &client, source_id).await;

    if let Err(unlock_err) = lock_client
        .execute(
            "SELECT pg_advisory_unlock(hashtext($1))",
            &[&ADVISORY_LOCK_SQL_KEY],
        )
        .await
    {
        log::error!("failed to release Postgres advisory lock: {unlock_err}");
    }
    drop(lock_client);

    result
}

/// The actual update body, run while holding both the process-local
/// `UPDATE_LOCK` and the Postgres advisory lock acquired by the caller.
async fn run_update_body(
    pool: &Pool,
    client: &Client,
    source_id: i16,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Repair bookkeeping left behind by a killed/crashed previous process:
    // a run still marked 'running' after a full day definitely didn't
    // finish cleanly. Scoped by `started_at` age (rather than any 'running'
    // row) so this can't clobber a legitimately-running peer whose merge is
    // just taking a while - the Postgres advisory lock above is what
    // actually prevents concurrent runs; this is only cleanup for rows
    // orphaned by a hard process kill.
    client
        .execute(
            "UPDATE catalog_updates SET status = 'failed', finished_at = now(), error = 'abandoned (process restart)' WHERE status = 'running' AND started_at < now() - interval '1 day'",
            &[],
        )
        .await?;

    let run_id: i64 = client
        .query_one(
            "INSERT INTO catalog_updates (source, status) VALUES ($1, 'running') RETURNING id",
            &[&source_id],
        )
        .await?
        .get(0);

    let (stats, result) = load_and_merge(pool, source_id).await;

    let (status, err_message): (&str, Option<String>) = match &result {
        Ok(_) => ("success", None),
        Err(e) => ("failed", Some(e.to_string())),
    };

    let staged_i64 = stats.staged as i64;
    let skipped_i64 = stats.skipped as i64;

    // The bookkeeping UPDATE below is best-effort: if it fails, we still
    // want to propagate `result` (the real, typed error whose source chain
    // is what `sentry::capture_error` in main.rs consumes) rather than
    // masking it with a bookkeeping failure.
    match pool.get().await {
        Ok(bookkeeping_client) => {
            if let Err(update_err) = bookkeeping_client
                .execute(
                    "UPDATE catalog_updates SET status = $2, finished_at = now(), error = $3, rows_staged = $4, rows_skipped = $5 WHERE id = $1",
                    &[&run_id, &status, &err_message, &staged_i64, &skipped_i64],
                )
                .await
            {
                log::error!(
                    "failed to write final catalog_updates bookkeeping for run {run_id}: {update_err}"
                );
            }
        }
        Err(pool_err) => {
            log::error!(
                "failed to get a pool connection for final catalog_updates bookkeeping for run {run_id}: {pool_err}"
            );
        }
    }

    // Propagate the original, typed `result` (not a stringified version)
    // now that the bookkeeping UPDATE above has run - its own failure was
    // logged separately above rather than masking this one.
    result?;

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

    // --- parse_insert_values ---

    #[test]
    fn parse_insert_values_single_row() {
        let line = "INSERT INTO `libavtorname` VALUES (1,'John','','Doe');";
        let rows = parse_insert_values(line);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].len(), 4);
    }

    #[test]
    fn parse_insert_values_multiple_rows_in_one_statement() {
        let line = "INSERT INTO `libavtorname` VALUES (1,'John','','Doe'),(2,'Jane','','Roe');";
        let rows = parse_insert_values(line);
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn parse_insert_values_returns_empty_for_non_insert_lines() {
        assert!(parse_insert_values("").is_empty());
        assert!(parse_insert_values("LOCK TABLES `libavtorname` WRITE;").is_empty());
        assert!(parse_insert_values("UNLOCK TABLES;").is_empty());
        assert!(parse_insert_values("-- a comment").is_empty());
    }

    #[test]
    fn parse_insert_values_returns_empty_for_malformed_line() {
        assert!(parse_insert_values("INSERT INTO `x` VALUES (1, 'unterminated").is_empty());
    }

    // --- aggregate_stage_outcomes ---
    //
    // These prove the orchestration invariant that replaces the old (Spec
    // 01-obsoleted) "a failing dependency fails dependents within bounded
    // time" test: since Phase A `stage_file` tasks have no dependencies on
    // each other any more, the only invariant left is that a single failed
    // task fails the *whole* Phase A batch, which `load_and_merge` uses to
    // decide never to call `load_and_merge_transaction` (i.e. the merge
    // transaction, and therefore any write to production tables, never
    // starts).

    #[test]
    fn aggregate_stage_outcomes_all_success_has_no_failures() {
        let outcomes: Vec<StageOutcome> = vec![
            (
                "authors",
                Ok(StageStats {
                    staged: 5,
                    skipped: 0,
                }),
            ),
            (
                "books",
                Ok(StageStats {
                    staged: 3,
                    skipped: 1,
                }),
            ),
        ];

        let (stats, failures) = aggregate_stage_outcomes(outcomes);

        assert!(failures.is_empty());
        assert_eq!(stats.staged, 8);
        assert_eq!(stats.skipped, 1);
    }

    #[test]
    fn aggregate_stage_outcomes_one_failure_fails_the_whole_batch() {
        let outcomes: Vec<StageOutcome> = vec![
            (
                "good_file",
                Ok(StageStats {
                    staged: 5,
                    skipped: 0,
                }),
            ),
            (
                "bad_file",
                Err(Box::new(std::io::Error::other("boom"))
                    as Box<dyn std::error::Error + Send + Sync>),
            ),
            (
                "another_good_file",
                Ok(StageStats {
                    staged: 2,
                    skipped: 0,
                }),
            ),
        ];

        let (stats, failures) = aggregate_stage_outcomes(outcomes);

        assert_eq!(
            failures.len(),
            1,
            "exactly the one failing file must be reported as a failure"
        );
        assert!(failures[0].contains("bad_file"));
        assert!(failures[0].contains("boom"));
        // Stats from the files that did succeed are still aggregated for
        // bookkeeping (`catalog_updates.rows_staged`/`rows_skipped`), even
        // though the batch as a whole is treated as failed by the caller.
        assert_eq!(stats.staged, 7);
    }

    #[test]
    fn aggregate_stage_outcomes_multiple_failures_are_all_reported() {
        let outcomes: Vec<StageOutcome> = vec![
            (
                "bad_file_1",
                Err(Box::new(std::io::Error::other("network error"))
                    as Box<dyn std::error::Error + Send + Sync>),
            ),
            (
                "bad_file_2",
                Err(Box::new(std::io::Error::other("parse error"))
                    as Box<dyn std::error::Error + Send + Sync>),
            ),
        ];

        let (_stats, failures) = aggregate_stage_outcomes(outcomes);

        assert_eq!(failures.len(), 2);
    }

    // --- try_start_update ---
    //
    // These prove the Spec 06.4 lock semantics: the caller (the `/update`
    // HTTP handler) must be able to tell, synchronously and without
    // touching Postgres, whether an update run actually started or one was
    // already in progress.

    // These two cases share `UPDATE_LOCK` (global process state), so they
    // run as a single test to avoid racing against each other if `cargo
    // test` were to run them concurrently on separate threads.
    #[tokio::test]
    async fn try_start_update_reports_busy_then_started() {
        {
            // Hold the lock ourselves to simulate a run already in
            // progress. `try_lock()` on the real `UPDATE_LOCK` fails purely
            // based on mutex state, before any DB code runs, so this never
            // touches Postgres.
            let _guard = UPDATE_LOCK
                .try_lock()
                .expect("UPDATE_LOCK must be free at the start of this test");

            assert_eq!(try_start_update(), UpdateStart::Busy);
        }

        // Lock released: `try_lock()` inside `try_start_update()` should
        // now succeed and report `Started`, having spawned a background
        // task. That spawned task's actual DB work will fail in this test
        // environment (no `config::CONFIG`/Postgres available) - that's
        // fine, only the synchronous return value is under test here (the
        // DB-touching behavior is covered by other, integration-level
        // tests).
        assert_eq!(try_start_update(), UpdateStart::Started);

        // Give the spawned task a moment to run (and release the lock)
        // before this test function returns.
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
