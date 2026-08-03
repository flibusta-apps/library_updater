use std::fmt::Debug;

use crate::config::{self, Webhook};
use deadpool_postgres::{Config, CreatePoolError, ManagerConfig, Pool, RecyclingMethod, Runtime};
use futures::{io::copy, TryStreamExt};
use reqwest::header::HeaderMap;
use tokio::fs::{remove_file, File};
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

async fn download_file(filename_str: &str) -> Result<(), Box<dyn std::error::Error + Send>> {
    log::info!("Download {filename_str}...");

    let link = format!("{}/sql/{filename_str}.gz", &config::CONFIG.fl_base_url);

    let response = match reqwest::get(link).await {
        Ok(v) => v,
        Err(err) => return Err(Box::new(err)),
    };

    let response = match response.error_for_status() {
        Ok(v) => v,
        Err(err) => return Err(Box::new(err)),
    };

    match remove_file(filename_str).await {
        Ok(_) => (),
        Err(err) => log::debug!("Can't remove file: {:?}", err),
    };

    let mut file = match File::create(filename_str).await {
        Ok(v) => v.compat(),
        Err(err) => {
            log::error!("Can't create {filename_str}: {:?}", err);
            return Err(Box::new(err));
        }
    };

    let data = response
        .bytes_stream()
        .map_err(std::io::Error::other)
        .into_async_read();

    let decoder = GzipDecoder::new(data);

    match copy(decoder, &mut file).await {
        Ok(_) => (),
        Err(err) => {
            log::error!("Can't write data {filename_str}: {}", err);
            return Err(Box::new(err));
        }
    };

    log::info!("{filename_str} downloaded!");

    Ok(())
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

    match download_file(file_name).await {
        Ok(_) => (),
        Err(err) => return Err(err),
    };

    let parse_options = ParseOptions::new()
        .dialect(SQLDialect::MariaDB)
        .arguments(SQLArguments::QuestionMark)
        .warn_unquoted_identifiers(true);

    let lines = read_lines(file_name);

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

    for line in lines.into_iter() {
        let line = match line {
            Ok(line) => line,
            Err(err) => return Err(Box::new(err)),
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

async fn send_webhooks() -> Result<(), Box<reqwest::Error>> {
    for webhook in config::CONFIG.webhooks.clone().into_iter() {
        let Webhook {
            method,
            url,
            headers,
        } = webhook;

        let client = reqwest::Client::new();

        let builder = match method {
            config::Method::Get => client.get(url),
            config::Method::Post => client.post(url),
        };

        let headers = HeaderMap::from_iter(headers);

        let response = builder.headers(headers).send().await;

        let response = match response {
            Ok(v) => v,
            Err(err) => return Err(Box::new(err)),
        };

        match response.error_for_status() {
            Ok(_) => (),
            Err(err) => return Err(Box::new(err)),
        };
    }

    Ok(())
}

lazy_static! {
    pub static ref UPDATE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::new(());
}

pub async fn update() -> Result<(), Box<dyn std::error::Error>> {
    let _lock = match UPDATE_LOCK.try_lock() {
        Ok(v) => v,
        Err(err) => return Err(Box::new(err)),
    };

    log::info!("Start update...");

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
            log::info!("Webhooks sended!");
        }
        Err(err) => {
            log::info!("Webhooks send failed : {err}");
            return Err(Box::new(err));
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
                Err(err) => log::info!("Update err: {:?}", err),
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
