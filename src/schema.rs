//! Idempotent DDL bootstrap for the staging-table + merge-transaction update
//! pipeline (spec 01). Every statement here is `IF NOT EXISTS`/additive and
//! safe to run on every process start.
//!
//! Notes on the additive unique-index bootstrap below (only created when an
//! equivalent unique index/constraint doesn't already exist): the previous row-by-row updater always checked `EXISTS(...)`
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

/// Unique indexes required by the `ON CONFLICT` clauses in
/// `crate::merge::MERGE_PLAN`. `book_library_server`'s migrations already
/// create equivalent unique constraints (`uc_sequences_source_remote_id`,
/// `uc_book_sequences_book_sequence`, `uc_translations_book_author`, and
/// `UNIQUE` on `book_annotations.book` / `author_annotations.author`), so an
/// extra index is only created when no equivalent plain unique index (same
/// columns, same order) exists yet, e.g. on a database that wasn't built by
/// those migrations. This avoids duplicate unique indexes (double write
/// cost/space).
const ENSURE_UNIQUE_INDEXES_SQL: &str = r#"
DO $$
DECLARE r record;
BEGIN
    FOR r IN
        SELECT * FROM (VALUES
            ('sequences', 'source,remote_id', 'ux_sequences_source_remote_id'),
            ('book_sequences', 'book,sequence', 'ux_book_sequences_book_sequence'),
            ('translations', 'book,author', 'ux_translations_book_author'),
            ('book_annotations', 'book', 'ux_book_annotations_book'),
            ('author_annotations', 'author', 'ux_author_annotations_author')
        ) AS t(tbl, cols, idx)
    LOOP
        IF NOT EXISTS (
            SELECT 1
            FROM pg_index i
            WHERE i.indrelid = to_regclass(r.tbl)
              AND i.indisunique
              AND i.indpred IS NULL
              AND i.indexprs IS NULL
              AND (
                  SELECT string_agg(a.attname::text, ',' ORDER BY k.ord)
                  FROM unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord)
                  JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
              ) = r.cols
        ) THEN
            EXECUTE format('CREATE UNIQUE INDEX IF NOT EXISTS %I ON %I (%s)', r.idx, r.tbl, r.cols);
        END IF;
    END LOOP;
END $$;
"#;

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
