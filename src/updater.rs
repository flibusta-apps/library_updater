use std::{
    fmt::Debug,
    sync::Arc,
    str::FromStr
};

use crate::config::{Webhook, self};
use deadpool_postgres::{Config, CreatePoolError, ManagerConfig, Pool, RecyclingMethod, Runtime};
use futures::{io::copy, TryStreamExt};
use reqwest::header::{HeaderMap, HeaderValue, HeaderName};
use tokio::fs::{File, remove_file};
use tokio::sync::Mutex;
use tokio_cron_scheduler::{JobScheduler, Job};
use tokio_postgres::NoTls;
use tracing::log;

use async_compression::futures::bufread::GzipDecoder;

use sql_parse::{
    parse_statement, InsertReplace, InsertReplaceType, ParseOptions, SQLArguments, SQLDialect,
    Statement,
};
use tokio_util::compat::TokioAsyncReadCompatExt;
use crate::types::{
    Author, AuthorAnnotation, AuthorAnnotationPic, BookAnnotation, BookAnnotationPic, BookAuthor,
    BookGenre, FromVecExpression, Genre, Sequence, SequenceInfo, Translator, Update,
};
use crate::utils::read_lines;

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
            return Err(Box::new(err))
        },
    };

    let data = response
        .bytes_stream()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
        .into_async_read();

    let decoder = GzipDecoder::new(data);

    match copy(decoder, &mut file).await {
        Ok(_) => (),
        Err(err) => {
            log::error!("Can't write data {filename_str}: {}", err);
            return Err(Box::new(err))
        },
    };

    log::info!("{filename_str} downloaded!");

    Ok(())
}

async fn process<T>(
    pool: Pool,
    source_id: i16,
    file_name: &str,
    deps: Vec<Arc<Mutex<Option<UpdateStatus>>>>,
) -> Result<(), Box<dyn std::error::Error + Send>>
where
    T: Debug + FromVecExpression<T> + Update,
{
    if !deps.is_empty() {
        loop {
            let mut some_failed = false;
            let mut some_none = false;

            for dep in deps.iter() {
                let status = dep.lock().await;
                match &*status {
                    Some(status) => match status {
                        UpdateStatus::Success => (),
                        UpdateStatus::Fail => some_failed = true,
                    },
                    None => some_none = true,
                }
            }

            if !some_failed && !some_none {
                break;
            }

            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
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

    match T::before_update(&pool.get().await.unwrap()).await {
        Ok(_) => (),
        Err(err) => return Err(err),
    };

    log::info!("Start update {file_name}...");

    for line in lines.into_iter() {
        let line = match line {
            Ok(line) => line,
            Err(err) => return Err(Box::new(err)),
        };

        let mut issues = Vec::new();
        let ast = parse_statement(&line, &mut issues, &parse_options);

        if let Some(Statement::InsertReplace(
            i @ InsertReplace {
                type_: InsertReplaceType::Insert(_),
                ..
            },
        )) = ast {
            for value in i.values.into_iter() {
                for t_value in value.1.into_iter() {
                    let value = T::from_vec_expression(&t_value);
                    let client = pool.get().await.unwrap();

                    match value.update(&client, source_id).await {
                        Ok(_) => {
                            // log::info!("{:?}", value);
                        }
                        Err(err) => {
                            log::error!("Update error: {:?} : {:?}", value, err);
                            return Err(err)
                        },
                    }
                }
            }
        }
    }

    match T::after_update(&pool.get().await.unwrap()).await {
        Ok(_) => (),
        Err(err) => return Err(err),
    };

    log::info!("Updated {file_name}...");

    Ok(())
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
    let client = pool.get().await.unwrap();

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

enum UpdateStatus {
    Success,
    Fail,
}

async fn send_webhooks() -> Result<(), Box<reqwest::Error>> {
    for webhook in config::CONFIG.webhooks.clone().into_iter() {
        let Webhook { method, url, headers } = webhook;

        let client = reqwest::Client::new();

        let builder = match method {
            config::Method::Get => {
                client.get(url)
            },
            config::Method::Post => {
                client.post(url)
            },
        };

        let t_headers: Vec<(HeaderName, HeaderValue)> = headers.into_iter().map(|(key, val)| {
            let value = match val {
                serde_json::Value::String(v) => v,
                _ => panic!("Header value not string!")
            };

            (
                HeaderName::from_str(key.as_ref()).unwrap(),
                HeaderValue::from_str(&value).unwrap()
            )
        }).collect();

        let headers = HeaderMap::from_iter(t_headers.into_iter());

        let response = builder.headers(headers).send().await;

        let response = match response {
            Ok(v) => v,
            Err(err) => return Err(Box::new(err)),
        };

        match response.error_for_status() {
            Ok(_) => (),
            Err(err) => return Err(Box::new(err)),
        };
    };

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
        Err(err) => panic!("{:?}", err),
    };

    let source_id = match get_source(pool.clone()).await {
        Ok(v) => Arc::new(v),
        Err(err) => panic!("{:?}", err),
    };

    let author_status: Arc<Mutex<Option<UpdateStatus>>> = Arc::new(Mutex::new(None));
    let book_status: Arc<Mutex<Option<UpdateStatus>>> = Arc::new(Mutex::new(None));
    let sequence_status: Arc<Mutex<Option<UpdateStatus>>> = Arc::new(Mutex::new(None));
    let book_annotation_status: Arc<Mutex<Option<UpdateStatus>>> = Arc::new(Mutex::new(None));
    let author_annotation_status: Arc<Mutex<Option<UpdateStatus>>> = Arc::new(Mutex::new(None));
    let genre_status: Arc<Mutex<Option<UpdateStatus>>> = Arc::new(Mutex::new(None));

    let pool_clone = pool.clone();
    let author_status_clone = author_status.clone();
    let source_id_clone = source_id.clone();
    let author_process = tokio::spawn(async move {
        match process::<Author>(pool_clone, *source_id_clone, "lib.libavtorname.sql", vec![]).await
        {
            Ok(_) => {
                let mut status = author_status_clone.lock().await;
                *status = Some(UpdateStatus::Success);
                Ok(())
            }
            Err(err) => {
                let mut status = author_status_clone.lock().await;
                *status = Some(UpdateStatus::Success);
                Err(err)
            }
        }
    });

    let pool_clone = pool.clone();
    let book_status_clone = book_status.clone();
    let source_id_clone = source_id.clone();
    let book_process = tokio::spawn(async move {
        match process::<Book>(pool_clone, *source_id_clone, "lib.libbook.sql", vec![]).await {
            Ok(_) => {
                let mut status = book_status_clone.lock().await;
                *status = Some(UpdateStatus::Success);
                Ok(())
            }
            Err(err) => {
                let mut status = book_status_clone.lock().await;
                *status = Some(UpdateStatus::Fail);
                Err(err)
            }
        }
    });

    let pool_clone = pool.clone();
    let deps = vec![author_status.clone(), book_status.clone()];
    let source_id_clone = source_id.clone();
    let book_author_process = tokio::spawn(async move {
        process::<BookAuthor>(pool_clone, *source_id_clone, "lib.libavtor.sql", deps).await
    });

    let pool_clone = pool.clone();
    let deps = vec![author_status.clone(), book_status.clone()];
    let source_id_clone = source_id.clone();
    let translator_process = tokio::spawn(async move {
        process::<Translator>(pool_clone, *source_id_clone, "lib.libtranslator.sql", deps).await
    });

    let pool_clone = pool.clone();
    let sequence_status_clone = sequence_status.clone();
    let source_id_clone = source_id.clone();
    let sequence_process = tokio::spawn(async move {
        match process::<Sequence>(pool_clone, *source_id_clone, "lib.libseqname.sql", vec![]).await
        {
            Ok(_) => {
                let mut status = sequence_status_clone.lock().await;
                *status = Some(UpdateStatus::Success);
                Ok(())
            }
            Err(err) => {
                let mut status = sequence_status_clone.lock().await;
                *status = Some(UpdateStatus::Fail);
                Err(err)
            }
        }
    });

    let pool_clone = pool.clone();
    let deps = vec![book_status.clone(), sequence_status.clone()];
    let source_id_clone = source_id.clone();
    let sequence_info_process = tokio::spawn(async move {
        process::<SequenceInfo>(pool_clone, *source_id_clone, "lib.libseq.sql", deps).await
    });

    let pool_clone = pool.clone();
    let deps = vec![book_status.clone()];
    let book_annotation_status_clone = book_annotation_status.clone();
    let source_id_clone = source_id.clone();
    let book_annotation_process = tokio::spawn(async move {
        match process::<BookAnnotation>(pool_clone, *source_id_clone, "lib.b.annotations.sql", deps)
            .await
        {
            Ok(_) => {
                let mut status = book_annotation_status_clone.lock().await;
                *status = Some(UpdateStatus::Success);
                Ok(())
            }
            Err(err) => {
                let mut status = book_annotation_status_clone.lock().await;
                *status = Some(UpdateStatus::Fail);
                Err(err)
            }
        }
    });

    let pool_clone = pool.clone();
    let deps = vec![book_annotation_status.clone()];
    let source_id_clone = source_id.clone();
    let book_annotation_pics_process = tokio::spawn(async move {
        process::<BookAnnotationPic>(
            pool_clone,
            *source_id_clone,
            "lib.b.annotations_pics.sql",
            deps,
        )
        .await
    });

    let pool_clone = pool.clone();
    let deps = vec![author_status.clone()];
    let author_annotation_status_clone = author_annotation_status.clone();
    let source_id_clone = source_id.clone();
    let author_annotation_process = tokio::spawn(async move {
        match process::<AuthorAnnotation>(
            pool_clone,
            *source_id_clone,
            "lib.a.annotations.sql",
            deps,
        )
        .await
        {
            Ok(_) => {
                let mut status = author_annotation_status_clone.lock().await;
                *status = Some(UpdateStatus::Success);
                Ok(())
            }
            Err(err) => {
                let mut status = author_annotation_status_clone.lock().await;
                *status = Some(UpdateStatus::Fail);
                Err(err)
            }
        }
    });

    let pool_clone = pool.clone();
    let deps = vec![author_annotation_status.clone()];
    let source_id_clone = source_id.clone();
    let author_annotation_pics_process = tokio::spawn(async move {
        process::<AuthorAnnotationPic>(
            pool_clone,
            *source_id_clone,
            "lib.a.annotations_pics.sql",
            deps,
        )
        .await
    });

    let pool_clone = pool.clone();
    let genre_status_clone = genre_status.clone();
    let source_id_clone = source_id.clone();
    let genre_annotation_process = tokio::spawn(async move {
        match process::<Genre>(pool_clone, *source_id_clone, "lib.libgenrelist.sql", vec![]).await {
            Ok(_) => {
                let mut status = genre_status_clone.lock().await;
                *status = Some(UpdateStatus::Success);
                Ok(())
            }
            Err(err) => {
                let mut status = genre_status_clone.lock().await;
                *status = Some(UpdateStatus::Fail);
                Err(err)
            }
        }
    });

    let pool_clone = pool.clone();
    let deps = vec![genre_status.clone(), book_status.clone()];
    let source_id_clone = source_id.clone();
    let book_genre_process = tokio::spawn(async move {
        process::<BookGenre>(pool_clone, *source_id_clone, "lib.libgenre.sql", deps).await
    });

    for process in [
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
        genre_annotation_process,
        book_genre_process
    ] {
        let process_result = match process.await {
            Ok(v) => v,
            Err(err) => return Err(Box::new(err)),
        };

        match process_result {
            Ok(_) => (),
            Err(err) => panic!("{:?}", err),
        }
    }

    match send_webhooks().await {
        Ok(_) => {
            log::info!("Webhooks sended!");
        },
        Err(err) => {
            log::info!("Webhooks send failed : {err}");
            return Err(Box::new(err))
        },
    };

    Ok(())
}

pub async fn cron_jobs() {
    let job_scheduler = JobScheduler::new().await.unwrap();

    let update_job = match Job::new_async("0 0 3 * * *", |_uuid, _l| Box::pin(async {
        match update().await {
            Ok(_) => log::info!("Updated"),
            Err(err) => log::info!("Update err: {:?}", err),
        };
    })) {
        Ok(v) => v,
        Err(err) => panic!("{:?}", err),
    };

    job_scheduler.add(update_job).await.unwrap();

    log::info!("Scheduler start...");
    match job_scheduler.start().await {
        Ok(v) => v,
        Err(err) => panic!("{:?}", err),
    };
}