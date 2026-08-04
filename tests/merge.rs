use library_updater::merge::{MergeStep, Params, MERGE_PLAN};
use library_updater::schema;
use library_updater::updater::check_staging_ratio;
use testcontainers_modules::postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};
use tokio_postgres::{Client, NoTls};

const SCHEMA_SQL: &str = "
CREATE TABLE sources (
    id SMALLSERIAL PRIMARY KEY,
    name VARCHAR(64) NOT NULL UNIQUE
);
CREATE TABLE books (
    id SERIAL PRIMARY KEY,
    source SMALLINT NOT NULL,
    remote_id INTEGER NOT NULL,
    title VARCHAR(256) NOT NULL,
    lang VARCHAR(3) NOT NULL,
    file_type VARCHAR(4) NOT NULL,
    uploaded DATE NOT NULL,
    is_deleted BOOLEAN NOT NULL DEFAULT false,
    pages INTEGER,
    year SMALLINT NOT NULL DEFAULT 0,
    CONSTRAINT uc_books_source_remote_id UNIQUE (source, remote_id)
);
CREATE TABLE authors (
    id SERIAL PRIMARY KEY,
    source SMALLINT NOT NULL,
    remote_id INTEGER NOT NULL,
    first_name VARCHAR(256) NOT NULL,
    last_name VARCHAR(256) NOT NULL,
    middle_name VARCHAR(256),
    CONSTRAINT uc_authors_source_remote_id UNIQUE (source, remote_id)
);
CREATE TABLE genres (
    id SERIAL PRIMARY KEY,
    source SMALLINT NOT NULL,
    remote_id INTEGER NOT NULL,
    code VARCHAR(45) NOT NULL,
    description VARCHAR(99) NOT NULL,
    meta VARCHAR(45) NOT NULL,
    CONSTRAINT uc_genres_source_remote_id UNIQUE (source, remote_id)
);
CREATE TABLE sequences (
    id SERIAL PRIMARY KEY,
    source SMALLINT NOT NULL,
    remote_id INTEGER NOT NULL,
    name VARCHAR(256) NOT NULL
);
CREATE TABLE book_authors (
    id SERIAL PRIMARY KEY,
    book INTEGER NOT NULL REFERENCES books(id),
    author INTEGER NOT NULL REFERENCES authors(id)
);
CREATE TABLE book_genres (
    id SERIAL PRIMARY KEY,
    genre INTEGER NOT NULL REFERENCES genres(id),
    book INTEGER NOT NULL REFERENCES books(id),
    CONSTRAINT uc_book_genres_book_genre UNIQUE (book, genre)
);
CREATE TABLE translations (
    id SERIAL PRIMARY KEY,
    position SMALLINT NOT NULL,
    author INTEGER NOT NULL REFERENCES authors(id),
    book INTEGER NOT NULL REFERENCES books(id)
);
CREATE TABLE book_sequences (
    id SERIAL PRIMARY KEY,
    book INTEGER NOT NULL REFERENCES books(id),
    sequence INTEGER NOT NULL REFERENCES sequences(id),
    position SMALLINT NOT NULL
);
CREATE TABLE book_annotations (
    id SERIAL PRIMARY KEY,
    book INTEGER NOT NULL REFERENCES books(id),
    title VARCHAR(256) NOT NULL,
    text TEXT NOT NULL,
    file VARCHAR(256)
);
CREATE TABLE author_annotations (
    id SERIAL PRIMARY KEY,
    author INTEGER NOT NULL UNIQUE REFERENCES authors(id),
    title VARCHAR(256) NOT NULL,
    text TEXT NOT NULL,
    file VARCHAR(256)
);
";

const SOURCE_ID: i16 = 1;
const OTHER_SOURCE_ID: i16 = 2;

/// Spin up an ephemeral Postgres container, connect to it, and apply the
/// base (non-staging) schema. `schema::ensure` is then responsible for
/// creating the staging tables and additive unique indexes, exactly as it
/// would on a real deployment.
async fn setup() -> (ContainerAsync<postgres::Postgres>, Client) {
    let container = postgres::Postgres::default()
        .with_tag("16-alpine")
        .start()
        .await
        .expect("failed to start postgres container");

    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("failed to get mapped port");

    let (client, connection) = tokio_postgres::connect(
        &format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres"),
        NoTls,
    )
    .await
    .expect("failed to connect to postgres");

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });

    client
        .batch_execute(SCHEMA_SQL)
        .await
        .expect("failed to apply schema");

    schema::ensure(&client)
        .await
        .expect("schema::ensure failed");

    (container, client)
}

fn allowed_langs() -> Vec<String> {
    vec!["ru".to_string(), "be".to_string(), "uk".to_string()]
}

/// Runs the full `MERGE_PLAN` inside a single transaction against `client`,
/// mirroring `crate::updater::load_and_merge`'s Phase B loop.
async fn run_merge_plan(
    client: &mut Client,
    source_id: i16,
    langs: &[String],
) -> Result<(), tokio_postgres::Error> {
    let tx = client.transaction().await?;
    tx.batch_execute(schema::ANALYZE_ALL_STAGING_SQL).await?;

    for step in MERGE_PLAN {
        run_step(&tx, step, source_id, langs).await?;
    }

    tx.commit().await
}

async fn run_step(
    tx: &tokio_postgres::Transaction<'_>,
    step: &MergeStep,
    source_id: i16,
    langs: &[String],
) -> Result<(), tokio_postgres::Error> {
    match step.params {
        Params::None => tx.batch_execute(step.sql).await,
        Params::Source => tx.execute(step.sql, &[&source_id]).await.map(|_| ()),
        Params::SourceLangs => tx
            .execute(step.sql, &[&source_id, &langs])
            .await
            .map(|_| ()),
    }
}

async fn count(client: &Client, table: &str) -> i64 {
    let row = client
        .query_one(&format!("SELECT COUNT(*) FROM {table}"), &[])
        .await
        .expect("failed to count rows");
    row.get(0)
}

async fn stage_author(client: &Client, remote_id: i32, first: &str, last: &str) {
    client
        .execute(
            "INSERT INTO staging_authors (remote_id, first_name, last_name, middle_name) VALUES ($1, $2, $3, '')",
            &[&remote_id, &first, &last],
        )
        .await
        .expect("failed to stage author");
}

async fn stage_book(client: &Client, remote_id: i32, title: &str, lang: &str) {
    client
        .execute(
            "INSERT INTO staging_books (remote_id, title, lang, file_type, uploaded, is_deleted, pages, year)
             VALUES ($1, $2, $3, 'fb2', '2020-01-01', false, 100, 2020)",
            &[&remote_id, &title, &lang],
        )
        .await
        .expect("failed to stage book");
}

async fn truncate_staging(client: &Client) {
    client
        .batch_execute(schema::TRUNCATE_ALL_STAGING_SQL)
        .await
        .expect("failed to truncate staging tables");
}

/// Builds a small `deadpool_postgres::Pool` against the already-running test
/// container, for the handful of tests (see below) that exercise
/// `updater::check_staging_ratio`, which takes a `tokio_postgres::Client`
/// but is normally called from behind a pool in production.
async fn build_pool(port: u16) -> deadpool_postgres::Pool {
    let mut config = deadpool_postgres::Config::new();
    config.host = Some("127.0.0.1".to_string());
    config.port = Some(port);
    config.dbname = Some("postgres".to_string());
    config.user = Some("postgres".to_string());
    config.password = Some("postgres".to_string());

    config
        .create_pool(Some(deadpool_postgres::Runtime::Tokio1), NoTls)
        .expect("failed to create deadpool pool against test container")
}

async fn stage_genre(client: &Client, remote_id: i32, code: &str) {
    client
        .execute(
            "INSERT INTO staging_genres (remote_id, code, description, meta) VALUES ($1, $2, '', '')",
            &[&remote_id, &code],
        )
        .await
        .expect("failed to stage genre");
}

async fn stage_book_author(client: &Client, book_remote_id: i32, author_remote_id: i32) {
    client
        .execute(
            "INSERT INTO staging_book_authors (book_remote_id, author_remote_id) VALUES ($1, $2)",
            &[&book_remote_id, &author_remote_id],
        )
        .await
        .expect("failed to stage book_author");
}

async fn stage_book_genre(client: &Client, book_remote_id: i32, genre_remote_id: i32) {
    client
        .execute(
            "INSERT INTO staging_book_genres (book_remote_id, genre_remote_id) VALUES ($1, $2)",
            &[&book_remote_id, &genre_remote_id],
        )
        .await
        .expect("failed to stage book_genre");
}

#[tokio::test]
async fn authors_upsert_inserts_updates_and_skips_noop_writes() {
    let (_container, mut client) = setup().await;
    let langs = allowed_langs();

    stage_author(&client, 1, "John", "Doe").await;
    run_merge_plan(&mut client, SOURCE_ID, &langs)
        .await
        .expect("first merge failed");

    assert_eq!(count(&client, "authors").await, 1);
    let row = client
        .query_one("SELECT first_name, last_name FROM authors", &[])
        .await
        .unwrap();
    let first_name: String = row.get(0);
    assert_eq!(first_name, "John");

    // second run: updated name for the same remote_id
    truncate_staging(&client).await;
    stage_author(&client, 1, "Jane", "Doe").await;
    run_merge_plan(&mut client, SOURCE_ID, &langs)
        .await
        .expect("second merge failed");

    assert_eq!(
        count(&client, "authors").await,
        1,
        "should upsert, not duplicate"
    );
    let row = client
        .query_one("SELECT first_name FROM authors", &[])
        .await
        .unwrap();
    let first_name: String = row.get(0);
    assert_eq!(first_name, "Jane");
}

#[tokio::test]
async fn books_upsert_soft_deletes_disallowed_language() {
    let (_container, mut client) = setup().await;
    let langs = allowed_langs();

    stage_book(&client, 1, "Some Title", "en").await;
    run_merge_plan(&mut client, SOURCE_ID, &langs)
        .await
        .expect("merge failed");

    let row = client
        .query_one("SELECT is_deleted FROM books", &[])
        .await
        .unwrap();
    let is_deleted: bool = row.get(0);
    assert!(is_deleted, "book with disallowed lang must be soft-deleted");
}

#[tokio::test]
async fn book_missing_from_next_dump_is_soft_deleted() {
    let (_container, mut client) = setup().await;
    let langs = allowed_langs();

    // Run 1: book present.
    stage_book(&client, 1, "Some Title", "ru").await;
    run_merge_plan(&mut client, SOURCE_ID, &langs)
        .await
        .expect("first merge failed");

    let row = client
        .query_one("SELECT is_deleted FROM books WHERE remote_id = 1", &[])
        .await
        .unwrap();
    let is_deleted: bool = row.get(0);
    assert!(
        !is_deleted,
        "book must be present and not deleted after run 1"
    );

    // Run 2: book absent from the dump entirely.
    truncate_staging(&client).await;
    run_merge_plan(&mut client, SOURCE_ID, &langs)
        .await
        .expect("second merge failed");

    let row = client
        .query_one("SELECT is_deleted FROM books WHERE remote_id = 1", &[])
        .await
        .unwrap();
    let is_deleted: bool = row.get(0);
    assert!(
        is_deleted,
        "book removed from the dump must be soft-deleted after the next merge"
    );
}

#[tokio::test]
async fn merge_scoped_to_source_leaves_other_sources_untouched() {
    let (_container, mut client) = setup().await;
    let langs = allowed_langs();

    // Pre-existing row belonging to a different source.
    client
        .execute(
            "INSERT INTO books (source, remote_id, title, lang, file_type, uploaded, is_deleted, pages, year)
             VALUES ($1, 1, 'Other Source Book', 'ru', 'fb2', '2020-01-01', false, 100, 2020)",
            &[&OTHER_SOURCE_ID],
        )
        .await
        .unwrap();
    client
        .execute(
            "INSERT INTO authors (source, remote_id, first_name, last_name, middle_name)
             VALUES ($1, 1, 'Other', 'Author', '')",
            &[&OTHER_SOURCE_ID],
        )
        .await
        .unwrap();
    client
        .execute(
            "INSERT INTO book_authors (book, author)
             SELECT b.id, a.id FROM books b, authors a
             WHERE b.source = $1 AND a.source = $1",
            &[&OTHER_SOURCE_ID],
        )
        .await
        .unwrap();

    // A merge run scoped to SOURCE_ID with an *empty* dump should not touch
    // (soft-delete or hard-delete) any row belonging to OTHER_SOURCE_ID.
    run_merge_plan(&mut client, SOURCE_ID, &langs)
        .await
        .expect("merge failed");

    let row = client
        .query_one(
            "SELECT is_deleted FROM books WHERE source = $1 AND remote_id = 1",
            &[&OTHER_SOURCE_ID],
        )
        .await
        .unwrap();
    let is_deleted: bool = row.get(0);
    assert!(
        !is_deleted,
        "a source-scoped merge must not soft-delete another source's books"
    );

    assert_eq!(
        count(&client, "book_authors").await,
        1,
        "a source-scoped merge must not hard-delete another source's link rows"
    );
}

#[tokio::test]
async fn failed_merge_step_rolls_back_all_changes() {
    let (_container, mut client) = setup().await;
    let langs = allowed_langs();

    // Baseline: nothing staged, nothing in authors/books yet.
    stage_author(&client, 1, "John", "Doe").await;
    stage_book(&client, 1, "Some Title", "ru").await;

    // Sabotage: drop a table referenced later in the plan (book_authors) so
    // that the "book_authors insert" step fails mid-transaction, after the
    // authors/books upserts and the "d_book_authors create"/index+analyze
    // steps (all earlier in the plan) have already run inside the same
    // transaction.
    client
        .batch_execute("DROP TABLE book_authors CASCADE;")
        .await
        .unwrap();

    let result = run_merge_plan(&mut client, SOURCE_ID, &langs).await;
    assert!(result.is_err(), "expected the merge to fail");

    // Because everything ran in one transaction, the earlier authors/books
    // upserts must have been rolled back along with the failing step.
    assert_eq!(
        count(&client, "authors").await,
        0,
        "authors upsert must be rolled back when a later step fails"
    );
    assert_eq!(
        count(&client, "books").await,
        0,
        "books upsert must be rolled back when a later step fails"
    );
}

/// A `staging_book_authors` row whose `book_remote_id`/`author_remote_id`
/// doesn't resolve to any row in `books`/`authors` for this source (e.g. a
/// dangling FK left behind by upstream dump inconsistency, or - as here - a
/// book that was staged this run but whose language got soft-deleted, so it
/// never got a *new* `books` row this source... no: more directly, an
/// author_remote_id that simply doesn't exist in `staging_authors` at all)
/// must be silently dropped by the `d_book_authors create` step's inner
/// `JOIN`s, not cause the merge to error out. This is intentional,
/// documented behavior (see `crate::merge`'s module doc), not a bug -
/// this test pins it down so a future change to an outer join doesn't
/// silently change the semantics.
#[tokio::test]
async fn book_authors_insert_skips_rows_with_missing_fk() {
    let (_container, mut client) = setup().await;
    let langs = allowed_langs();

    stage_author(&client, 1, "John", "Doe").await;
    stage_book(&client, 1, "Some Title", "ru").await;
    // Valid link: book 1 <-> author 1.
    stage_book_author(&client, 1, 1).await;
    // Dangling link: author_remote_id 999 was never staged in
    // staging_authors (and therefore never inserted into authors), so the
    // JOIN in "d_book_authors create" cannot resolve it to a local id.
    stage_book_author(&client, 1, 999).await;
    // Dangling link: book_remote_id 888 was never staged either.
    stage_book_author(&client, 888, 1).await;

    run_merge_plan(&mut client, SOURCE_ID, &langs)
        .await
        .expect("merge must succeed even though some link rows have missing FKs");

    assert_eq!(
        count(&client, "book_authors").await,
        1,
        "only the link row whose both ends resolved via JOIN must be inserted; \
         rows referencing a remote_id absent from this run's dump must be silently skipped"
    );

    let row = client
        .query_one("SELECT book, author FROM book_authors", &[])
        .await
        .unwrap();
    let book_id: i32 = row.get(0);
    let author_id: i32 = row.get(1);

    let expected_book_id: i32 = client
        .query_one("SELECT id FROM books WHERE remote_id = 1", &[])
        .await
        .unwrap()
        .get(0);
    let expected_author_id: i32 = client
        .query_one("SELECT id FROM authors WHERE remote_id = 1", &[])
        .await
        .unwrap()
        .get(0);

    assert_eq!(book_id, expected_book_id);
    assert_eq!(author_id, expected_author_id);
}

/// F1 regression test: a truncated/empty `staging_book_genres` (simulating a
/// truncated `lib.libgenre.sql` dump) must be caught by
/// `updater::check_staging_ratio` even though `staging_books` and
/// `staging_book_authors` are both fully populated (ratio 1.0) and would
/// pass their own checks. Without this guard, the anti-join hard-delete step
/// "book_genres hard-delete removed" in `MERGE_PLAN` would wipe every
/// `book_genres` row for the source once the merge transaction ran.
#[tokio::test]
async fn sanity_check_catches_truncated_link_staging_table_even_when_books_ratio_is_fine() {
    let (container, mut client) = setup().await;
    let langs = allowed_langs();

    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("failed to get mapped port");
    let pool = build_pool(port).await;

    // Run 1: establish a "current" catalog for SOURCE_ID that includes a
    // book, an author, a genre, a book_authors link and a book_genres link -
    // exactly what a prior successful update would have left behind.
    stage_author(&client, 1, "John", "Doe").await;
    stage_book(&client, 1, "Some Title", "ru").await;
    stage_genre(&client, 1, "sf").await;
    stage_book_author(&client, 1, 1).await;
    stage_book_genre(&client, 1, 1).await;

    run_merge_plan(&mut client, SOURCE_ID, &langs)
        .await
        .expect("first (baseline) merge failed");

    assert_eq!(
        count(&client, "book_genres").await,
        1,
        "baseline book_genres must exist"
    );
    assert_eq!(
        count(&client, "book_authors").await,
        1,
        "baseline book_authors must exist"
    );

    // Run 2: simulate a truncated `lib.libgenre.sql` dump. `staging_books`
    // and `staging_book_authors` are staged exactly as before (ratio 1.0),
    // but `staging_book_genres` is left completely empty.
    truncate_staging(&client).await;
    stage_author(&client, 1, "John", "Doe").await;
    stage_book(&client, 1, "Some Title", "ru").await;
    stage_book_author(&client, 1, 1).await;
    // Deliberately NOT staging book_genres this run.

    let conn = pool.get().await.expect("failed to get pooled connection");

    // Sanity: books and book_authors ratios are both fine (1.0) on their own.
    check_staging_ratio(
        &conn,
        "staging_books",
        "SELECT COUNT(*) FROM books WHERE source = $1 AND NOT is_deleted",
        SOURCE_ID,
        "books",
        0.5,
    )
    .await
    .expect("books ratio check must pass");

    check_staging_ratio(
        &conn,
        "staging_book_authors",
        "SELECT COUNT(*) FROM book_authors ba JOIN books b ON b.id = ba.book WHERE b.source = $1",
        SOURCE_ID,
        "book_authors",
        0.5,
    )
    .await
    .expect("book_authors ratio check must pass");

    // The guard for the truncated link table must fire.
    let result = check_staging_ratio(
        &conn,
        "staging_book_genres",
        "SELECT COUNT(*) FROM book_genres bg JOIN books b ON b.id = bg.book WHERE b.source = $1",
        SOURCE_ID,
        "book_genres",
        0.5,
    )
    .await;

    assert!(
        result.is_err(),
        "check_staging_ratio must fail when staging_book_genres is empty but book_genres already \
         has rows for this source, even though books/book_authors ratios are both fine"
    );
}
