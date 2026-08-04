//! Idempotent DDL bootstrap for the staging-table + merge-transaction update
//! pipeline (spec 01). Every statement here is `IF NOT EXISTS`/additive and
//! safe to run on every process start.
//!
//! Notes on the additive `CREATE UNIQUE INDEX IF NOT EXISTS` statements
//! below: the previous row-by-row updater always checked `EXISTS(...)`
//! before inserting into `sequences`, `book_sequences`, `translations`,
//! `book_annotations` and `author_annotations`, so no duplicate rows should
//! exist in production for the columns we index here. If that assumption
//! turns out to be wrong for a given deployment, these `CREATE UNIQUE INDEX`
//! statements will fail loudly at startup rather than silently corrupting
//! data - which is the intended, safer failure mode.
use tokio_postgres::Client;

/// Staging tables truncated at the start of every load, in no particular
/// order (they have no FK relationships to each other).
pub const STAGING_TABLES: &[&str] = &[
    "staging_authors",
    "staging_books",
    "staging_genres",
    "staging_sequences",
    "staging_book_authors",
    "staging_translations",
    "staging_book_sequences",
    "staging_book_genres",
    "staging_book_annotations",
    "staging_book_annotation_pics",
    "staging_author_annotations",
    "staging_author_annotation_pics",
];

const CREATE_CATALOG_UPDATES_SQL: &str = "
CREATE TABLE IF NOT EXISTS catalog_updates (
    id bigserial PRIMARY KEY,
    source smallint NOT NULL,
    status text NOT NULL CHECK (status IN ('running','success','failed')),
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    rows_staged bigint NOT NULL DEFAULT 0,
    rows_skipped bigint NOT NULL DEFAULT 0,
    error text
);
CREATE INDEX IF NOT EXISTS ix_catalog_updates_started_at ON catalog_updates (started_at DESC);
";

const CREATE_STAGING_TABLES_SQL: &str = "
CREATE UNLOGGED TABLE IF NOT EXISTS staging_authors (
    remote_id int, first_name text, last_name text, middle_name text
);
CREATE UNLOGGED TABLE IF NOT EXISTS staging_books (
    remote_id int, title text, lang text, file_type text, uploaded date,
    is_deleted bool, pages int, year smallint
);
CREATE UNLOGGED TABLE IF NOT EXISTS staging_genres (
    remote_id int, code text, description text, meta text
);
CREATE UNLOGGED TABLE IF NOT EXISTS staging_sequences (
    remote_id int, name text
);
CREATE UNLOGGED TABLE IF NOT EXISTS staging_book_authors (
    book_remote_id int, author_remote_id int
);
CREATE UNLOGGED TABLE IF NOT EXISTS staging_translations (
    book_remote_id int, author_remote_id int, position smallint
);
CREATE UNLOGGED TABLE IF NOT EXISTS staging_book_sequences (
    book_remote_id int, sequence_remote_id int, position smallint
);
CREATE UNLOGGED TABLE IF NOT EXISTS staging_book_genres (
    book_remote_id int, genre_remote_id int
);
CREATE UNLOGGED TABLE IF NOT EXISTS staging_book_annotations (
    book_remote_id int, title text, text text
);
CREATE UNLOGGED TABLE IF NOT EXISTS staging_book_annotation_pics (
    book_remote_id int, file text
);
CREATE UNLOGGED TABLE IF NOT EXISTS staging_author_annotations (
    author_remote_id int, title text, text text
);
CREATE UNLOGGED TABLE IF NOT EXISTS staging_author_annotation_pics (
    author_remote_id int, file text
);
";

/// Additive unique indexes required by the `ON CONFLICT` clauses in
/// `crate::merge::MERGE_PLAN`, for tables where the old row-by-row updater
/// never relied on (and therefore may never have created) a real unique
/// constraint.
const ENSURE_UNIQUE_INDEXES_SQL: &str = "
CREATE UNIQUE INDEX IF NOT EXISTS ux_sequences_source_remote_id ON sequences (source, remote_id);
CREATE UNIQUE INDEX IF NOT EXISTS ux_book_sequences_book_sequence ON book_sequences (book, sequence);
CREATE UNIQUE INDEX IF NOT EXISTS ux_translations_book_author ON translations (book, author);
CREATE UNIQUE INDEX IF NOT EXISTS ux_book_annotations_book ON book_annotations (book);
CREATE UNIQUE INDEX IF NOT EXISTS ux_author_annotations_author ON author_annotations (author);
";

/// One `TRUNCATE` per staging table, run at the start of every load so a
/// previous (possibly partial) run's rows never leak into a fresh merge.
pub const TRUNCATE_ALL_STAGING_SQL: &str = "
TRUNCATE TABLE staging_authors, staging_books, staging_genres, staging_sequences,
    staging_book_authors, staging_translations, staging_book_sequences, staging_book_genres,
    staging_book_annotations, staging_book_annotation_pics, staging_author_annotations,
    staging_author_annotation_pics;
";

/// `COPY ... FROM STDIN BINARY` leaves the staging tables with no planner
/// statistics; `ANALYZE` before the merge join-heavy steps run.
pub const ANALYZE_ALL_STAGING_SQL: &str = "
ANALYZE staging_authors;
ANALYZE staging_books;
ANALYZE staging_genres;
ANALYZE staging_sequences;
ANALYZE staging_book_authors;
ANALYZE staging_translations;
ANALYZE staging_book_sequences;
ANALYZE staging_book_genres;
ANALYZE staging_book_annotations;
ANALYZE staging_book_annotation_pics;
ANALYZE staging_author_annotations;
ANALYZE staging_author_annotation_pics;
";

/// Runs all idempotent bootstrap DDL. Safe to call on every process start
/// and before every update run.
pub async fn ensure(client: &Client) -> Result<(), tokio_postgres::Error> {
    client.batch_execute(CREATE_CATALOG_UPDATES_SQL).await?;
    client.batch_execute(CREATE_STAGING_TABLES_SQL).await?;
    client.batch_execute(ENSURE_UNIQUE_INDEXES_SQL).await?;
    Ok(())
}
