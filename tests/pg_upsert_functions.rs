use library_updater::types::{AuthorAnnotation, BookGenre, Translator, Update};
use testcontainers_modules::postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};
use tokio_postgres::{Client, NoTls};

const SCHEMA_SQL: &str = "
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
    book INTEGER NOT NULL REFERENCES books(id),
    CONSTRAINT uc_translations_book_author UNIQUE (book, author)
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

/// Spin up an ephemeral Postgres container, connect to it, and apply the
/// test schema. The returned container must be kept alive for the duration
/// of the test (drop stops the container).
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

    (container, client)
}

async fn insert_book(client: &Client, remote_id: i32) -> i32 {
    client
        .execute(
            "INSERT INTO books (source, remote_id, title, lang, file_type, uploaded, pages, year)
             VALUES ($1, $2, 'Test Title', 'ru', 'fb2', '2020-01-01', 100, 2020)",
            &[&SOURCE_ID, &remote_id],
        )
        .await
        .expect("failed to insert book");
    remote_id
}

async fn insert_author(client: &Client, remote_id: i32) -> i32 {
    client
        .execute(
            "INSERT INTO authors (source, remote_id, first_name, last_name, middle_name)
             VALUES ($1, $2, 'John', 'Doe', 'M')",
            &[&SOURCE_ID, &remote_id],
        )
        .await
        .expect("failed to insert author");
    remote_id
}

async fn insert_genre(client: &Client, remote_id: i32) -> i32 {
    client
        .execute(
            "INSERT INTO genres (source, remote_id, code, description, meta)
             VALUES ($1, $2, 'sf', 'Science Fiction', 'sf')",
            &[&SOURCE_ID, &remote_id],
        )
        .await
        .expect("failed to insert genre");
    remote_id
}

async fn count(client: &Client, table: &str) -> i64 {
    let row = client
        .query_one(&format!("SELECT COUNT(*) FROM {table}"), &[])
        .await
        .expect("failed to count rows");
    row.get(0)
}

#[tokio::test]
async fn book_genre_creates_link_for_valid_book_and_genre() {
    let (_container, client) = setup().await;

    let book_id = insert_book(&client, 1).await;
    let genre_id = insert_genre(&client, 1).await;

    BookGenre::before_update(&client).await.unwrap();

    let result = BookGenre {
        book_id: book_id as u64,
        genre_id: genre_id as u64,
    }
    .update(&client, SOURCE_ID)
    .await;

    assert!(result.is_ok());
    assert_eq!(count(&client, "book_genres").await, 1);
}

#[tokio::test]
async fn book_genre_skips_when_book_missing() {
    let (_container, client) = setup().await;

    let genre_id = insert_genre(&client, 1).await;

    BookGenre::before_update(&client).await.unwrap();

    let result = BookGenre {
        book_id: 9999,
        genre_id: genre_id as u64,
    }
    .update(&client, SOURCE_ID)
    .await;

    assert!(result.is_ok());
    assert_eq!(count(&client, "book_genres").await, 0);
}

#[tokio::test]
async fn book_genre_skips_when_genre_missing() {
    let (_container, client) = setup().await;

    let book_id = insert_book(&client, 1).await;

    BookGenre::before_update(&client).await.unwrap();

    let result = BookGenre {
        book_id: book_id as u64,
        genre_id: 9999,
    }
    .update(&client, SOURCE_ID)
    .await;

    assert!(result.is_ok());
    assert_eq!(count(&client, "book_genres").await, 0);
}

#[tokio::test]
async fn update_translation_skips_missing_author_without_error() {
    let (_container, client) = setup().await;

    let book_id = insert_book(&client, 1).await;

    Translator::before_update(&client).await.unwrap();

    let result = Translator {
        book_id: book_id as u64,
        author_id: 9999,
        position: 1,
    }
    .update(&client, SOURCE_ID)
    .await;

    assert!(result.is_ok());
    assert_eq!(count(&client, "translations").await, 0);
}

#[tokio::test]
async fn update_translation_creates_row_for_valid_book_and_author() {
    let (_container, client) = setup().await;

    let book_id = insert_book(&client, 1).await;
    let author_id = insert_author(&client, 1).await;

    Translator::before_update(&client).await.unwrap();

    let result = Translator {
        book_id: book_id as u64,
        author_id: author_id as u64,
        position: 3,
    }
    .update(&client, SOURCE_ID)
    .await;

    assert!(result.is_ok());
    assert_eq!(count(&client, "translations").await, 1);

    let row = client
        .query_one("SELECT position FROM translations", &[])
        .await
        .unwrap();
    let position: i16 = row.get(0);
    assert_eq!(position, 3);
}

#[tokio::test]
async fn update_author_annotation_skips_missing_author_without_error() {
    let (_container, client) = setup().await;

    AuthorAnnotation::before_update(&client).await.unwrap();

    let result = AuthorAnnotation {
        author_id: 9999,
        title: "t".into(),
        body: Some("b".into()),
    }
    .update(&client, SOURCE_ID)
    .await;

    assert!(result.is_ok());
    assert_eq!(count(&client, "author_annotations").await, 0);
}

#[tokio::test]
async fn update_author_annotation_creates_row_for_valid_author() {
    let (_container, client) = setup().await;

    let author_id = insert_author(&client, 1).await;

    AuthorAnnotation::before_update(&client).await.unwrap();

    let result = AuthorAnnotation {
        author_id: author_id as u64,
        title: "Some Title".into(),
        body: Some("Some Text".into()),
    }
    .update(&client, SOURCE_ID)
    .await;

    assert!(result.is_ok());
    assert_eq!(count(&client, "author_annotations").await, 1);

    let row = client
        .query_one("SELECT title, text FROM author_annotations", &[])
        .await
        .unwrap();
    let title: String = row.get(0);
    let text: String = row.get(1);
    assert_eq!(title, "Some Title");
    assert_eq!(text, "Some Text");
}
