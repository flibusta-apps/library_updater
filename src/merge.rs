//! The Phase B merge plan: a fixed, ordered sequence of set-based SQL
//! statements executed inside a single transaction against the staging
//! tables loaded by Phase A (`crate::updater::stage_file`).
//!
//! Ordering matters:
//!   1. entity upserts (authors, books, genres, sequences)
//!   2. link table merges (book_authors, book_genres, translations, book_sequences),
//!      each building a small `d_*` temp table of resolved (local id, local id)
//!      pairs used both for the insert and, later, for the anti-join hard-delete
//!   3. payload upserts that depend on resolved book/author ids (annotations, pics)
//!   4. anti-join soft-delete of books removed from the dump
//!   5. anti-join hard-delete of link rows removed from the dump (reusing the
//!      `d_*` temp tables built in step 2 - cheap since they're already there)
//!
//! Deliberate limitation (documented, not a bug): authors/genres/sequences
//! rows that disappear from the upstream dump are NOT deleted. They have no
//! `is_deleted` column in the current schema, and hard-deleting them risks
//! FK violations from tables outside this repo (e.g. a `book_library_server`
//! favorites/collections table). Only books and the four link tables above
//! participate in delete/anti-join cleanup, matching spec 01's acceptance
//! criteria (which only names books).

/// How a `MergeStep`'s SQL should be executed.
pub enum Params {
    /// No bind parameters; run via `batch_execute` (may contain several
    /// `;`-separated statements).
    None,
    /// A single `$1 = source_id` (smallint) parameter.
    Source,
    /// `$1 = source_id`, `$2 = allowed_langs` (text[]).
    SourceLangs,
}

pub struct MergeStep {
    pub name: &'static str,
    pub sql: &'static str,
    pub params: Params,
}

pub static MERGE_PLAN: &[MergeStep] = &[
    // -- 1. simple entity upserts -------------------------------------------------
    MergeStep {
        name: "authors upsert",
        sql: "
            WITH s AS MATERIALIZED (
                SELECT DISTINCT ON (remote_id) *
                FROM staging_authors
                WHERE remote_id IS NOT NULL
                ORDER BY remote_id, ctid DESC
            ), updated AS (
                UPDATE authors t
                SET first_name = s.first_name,
                    last_name = s.last_name,
                    middle_name = s.middle_name
                FROM s
                WHERE t.source = $1 AND t.remote_id = s.remote_id
                  AND (t.first_name, t.last_name, t.middle_name) IS DISTINCT FROM (s.first_name, s.last_name, s.middle_name)
            )
            INSERT INTO authors (source, remote_id, first_name, last_name, middle_name)
            SELECT $1, s.remote_id, s.first_name, s.last_name, s.middle_name
            FROM s
            WHERE NOT EXISTS (
                SELECT 1 FROM authors t WHERE t.source = $1 AND t.remote_id = s.remote_id
            )
            ON CONFLICT (source, remote_id) DO NOTHING;
        ",
        params: Params::Source,
    },
    MergeStep {
        name: "books upsert",
        sql: "
            WITH s AS MATERIALIZED (
                SELECT DISTINCT ON (remote_id) remote_id, title, lang, file_type, uploaded,
                       is_deleted OR NOT (lang = ANY($2::text[])) AS is_deleted, pages, year
                FROM staging_books
                WHERE remote_id IS NOT NULL
                ORDER BY remote_id, ctid DESC
            ), updated AS (
                UPDATE books t
                SET title = s.title,
                    lang = s.lang,
                    file_type = s.file_type,
                    uploaded = s.uploaded,
                    is_deleted = s.is_deleted,
                    pages = s.pages,
                    year = s.year
                FROM s
                WHERE t.source = $1 AND t.remote_id = s.remote_id
                  AND (t.title, t.lang, t.file_type, t.uploaded, t.is_deleted, t.pages, t.year) IS DISTINCT FROM (s.title, s.lang, s.file_type, s.uploaded, s.is_deleted, s.pages, s.year)
            )
            INSERT INTO books (source, remote_id, title, lang, file_type, uploaded, is_deleted, pages, year)
            SELECT $1, s.remote_id, s.title, s.lang, s.file_type, s.uploaded, s.is_deleted, s.pages, s.year
            FROM s
            WHERE NOT EXISTS (
                SELECT 1 FROM books t WHERE t.source = $1 AND t.remote_id = s.remote_id
            )
            ON CONFLICT (source, remote_id) DO NOTHING;
        ",
        params: Params::SourceLangs,
    },
    MergeStep {
        name: "genres upsert",
        sql: "
            WITH s AS MATERIALIZED (
                SELECT DISTINCT ON (remote_id) *
                FROM staging_genres
                WHERE remote_id IS NOT NULL
                ORDER BY remote_id, ctid DESC
            ), updated AS (
                UPDATE genres t
                SET code = s.code,
                    description = s.description,
                    meta = s.meta
                FROM s
                WHERE t.source = $1 AND t.remote_id = s.remote_id
                  AND (t.code, t.description, t.meta) IS DISTINCT FROM (s.code, s.description, s.meta)
            )
            INSERT INTO genres (source, remote_id, code, description, meta)
            SELECT $1, s.remote_id, s.code, s.description, s.meta
            FROM s
            WHERE NOT EXISTS (
                SELECT 1 FROM genres t WHERE t.source = $1 AND t.remote_id = s.remote_id
            )
            ON CONFLICT (source, remote_id) DO NOTHING;
        ",
        params: Params::Source,
    },
    MergeStep {
        name: "sequences upsert",
        sql: "
            WITH s AS MATERIALIZED (
                SELECT DISTINCT ON (remote_id) *
                FROM staging_sequences
                WHERE remote_id IS NOT NULL
                ORDER BY remote_id, ctid DESC
            ), updated AS (
                UPDATE sequences t
                SET name = s.name
                FROM s
                WHERE t.source = $1 AND t.remote_id = s.remote_id
                  AND (t.name) IS DISTINCT FROM (s.name)
            )
            INSERT INTO sequences (source, remote_id, name)
            SELECT $1, s.remote_id, s.name
            FROM s
            WHERE NOT EXISTS (
                SELECT 1 FROM sequences t WHERE t.source = $1 AND t.remote_id = s.remote_id
            )
            ON CONFLICT (source, remote_id) DO NOTHING;
        ",
        params: Params::Source,
    },
    // -- 2. link tables: resolve remote ids -> local ids into a temp table, then merge --
    MergeStep {
        name: "d_book_authors create",
        sql: "
            CREATE TEMP TABLE d_book_authors ON COMMIT DROP AS
            SELECT DISTINCT b.id AS book, a.id AS author
            FROM staging_book_authors s
            JOIN books b ON b.source = $1 AND b.remote_id = s.book_remote_id
            JOIN authors a ON a.source = $1 AND a.remote_id = s.author_remote_id;
        ",
        params: Params::Source,
    },
    MergeStep {
        name: "d_book_authors index+analyze",
        sql: "
            CREATE INDEX ON d_book_authors (book, author);
            ANALYZE d_book_authors;
        ",
        params: Params::None,
    },
    MergeStep {
        name: "book_authors insert",
        sql: "
            INSERT INTO book_authors (book, author)
            SELECT d.book, d.author FROM d_book_authors d
            WHERE NOT EXISTS (
                SELECT 1 FROM book_authors ba WHERE ba.book = d.book AND ba.author = d.author
            );
        ",
        params: Params::None,
    },
    MergeStep {
        name: "d_book_genres create",
        sql: "
            CREATE TEMP TABLE d_book_genres ON COMMIT DROP AS
            SELECT DISTINCT b.id AS book, g.id AS genre
            FROM staging_book_genres s
            JOIN books b ON b.source = $1 AND b.remote_id = s.book_remote_id
            JOIN genres g ON g.source = $1 AND g.remote_id = s.genre_remote_id;
        ",
        params: Params::Source,
    },
    MergeStep {
        name: "d_book_genres index+analyze",
        sql: "
            CREATE INDEX ON d_book_genres (book, genre);
            ANALYZE d_book_genres;
        ",
        params: Params::None,
    },
    MergeStep {
        name: "book_genres insert",
        sql: "
            INSERT INTO book_genres (book, genre)
            SELECT d.book, d.genre FROM d_book_genres d
            WHERE NOT EXISTS (
                SELECT 1 FROM book_genres bg WHERE bg.book = d.book AND bg.genre = d.genre
            );
        ",
        params: Params::None,
    },
    MergeStep {
        name: "d_translations create",
        sql: "
            CREATE TEMP TABLE d_translations ON COMMIT DROP AS
            SELECT DISTINCT ON (b.id, a.id) b.id AS book, a.id AS author, s.position
            FROM staging_translations s
            JOIN books b ON b.source = $1 AND b.remote_id = s.book_remote_id
            JOIN authors a ON a.source = $1 AND a.remote_id = s.author_remote_id
            ORDER BY b.id, a.id, s.ctid DESC;
        ",
        params: Params::Source,
    },
    MergeStep {
        name: "d_translations index+analyze",
        sql: "
            CREATE INDEX ON d_translations (book, author);
            ANALYZE d_translations;
        ",
        params: Params::None,
    },
    MergeStep {
        name: "translations upsert",
        sql: "
            WITH updated AS (
                UPDATE translations t SET position = s.position
                FROM d_translations s
                WHERE t.book = s.book AND t.author = s.author
                  AND t.position IS DISTINCT FROM s.position
            )
            INSERT INTO translations (book, author, position)
            SELECT s.book, s.author, s.position FROM d_translations s
            WHERE NOT EXISTS (
                SELECT 1 FROM translations t WHERE t.book = s.book AND t.author = s.author
            )
            ON CONFLICT (book, author) DO NOTHING;
        ",
        params: Params::None,
    },
    MergeStep {
        name: "d_book_sequences create",
        sql: "
            CREATE TEMP TABLE d_book_sequences ON COMMIT DROP AS
            SELECT DISTINCT ON (b.id, sq.id) b.id AS book, sq.id AS sequence, s.position
            FROM staging_book_sequences s
            JOIN books b ON b.source = $1 AND b.remote_id = s.book_remote_id
            JOIN sequences sq ON sq.source = $1 AND sq.remote_id = s.sequence_remote_id
            ORDER BY b.id, sq.id, s.ctid DESC;
        ",
        params: Params::Source,
    },
    MergeStep {
        name: "d_book_sequences index+analyze",
        sql: "
            CREATE INDEX ON d_book_sequences (book, sequence);
            ANALYZE d_book_sequences;
        ",
        params: Params::None,
    },
    MergeStep {
        // Note: unlike translations, the legacy `update_book_sequence` PL/pgSQL
        // function wrapped position in ABS(). That asymmetry between
        // translations (no ABS) and book_sequences (ABS) is preserved here
        // as-is; fixing it is out of scope for this spec.
        name: "book_sequences upsert",
        sql: "
            WITH updated AS (
                UPDATE book_sequences t SET position = ABS(s.position)
                FROM d_book_sequences s
                WHERE t.book = s.book AND t.sequence = s.sequence
                  AND t.position IS DISTINCT FROM ABS(s.position)
            )
            INSERT INTO book_sequences (book, sequence, position)
            SELECT s.book, s.sequence, ABS(s.position) FROM d_book_sequences s
            WHERE NOT EXISTS (
                SELECT 1 FROM book_sequences t WHERE t.book = s.book AND t.sequence = s.sequence
            )
            ON CONFLICT (book, sequence) DO NOTHING;
        ",
        params: Params::None,
    },
    // -- 3. payload upserts that depend on resolved book/author ids ---------------
    MergeStep {
        name: "book_annotations upsert",
        sql: "
            WITH resolved AS MATERIALIZED (
                SELECT b.id AS book, s.title, s.text
                FROM (
                    SELECT DISTINCT ON (book_remote_id) *
                    FROM staging_book_annotations
                    WHERE book_remote_id IS NOT NULL AND text IS NOT NULL
                    ORDER BY book_remote_id, ctid DESC
                ) s
                JOIN books b ON b.source = $1 AND b.remote_id = s.book_remote_id
            ), updated AS (
                UPDATE book_annotations t SET title = s.title, text = s.text
                FROM resolved s
                WHERE t.book = s.book
                  AND (t.title, t.text) IS DISTINCT FROM (s.title, s.text)
            )
            INSERT INTO book_annotations (book, title, text)
            SELECT s.book, s.title, s.text FROM resolved s
            WHERE NOT EXISTS (
                SELECT 1 FROM book_annotations t WHERE t.book = s.book
            )
            ON CONFLICT (book) DO NOTHING;
        ",
        params: Params::Source,
    },
    MergeStep {
        // Pics are update-only (mirrors the legacy behaviour: a pic is only
        // attached to an annotation row that already exists).
        name: "book_annotation_pics update",
        sql: "
            UPDATE book_annotations
            SET file = s.file
            FROM (
                SELECT DISTINCT ON (book_remote_id) *
                FROM staging_book_annotation_pics
                WHERE book_remote_id IS NOT NULL
                ORDER BY book_remote_id, ctid DESC
            ) s
            JOIN books b ON b.source = $1 AND b.remote_id = s.book_remote_id
            WHERE book_annotations.book = b.id
              AND book_annotations.file IS DISTINCT FROM s.file;
        ",
        params: Params::Source,
    },
    MergeStep {
        name: "author_annotations upsert",
        sql: "
            WITH resolved AS MATERIALIZED (
                SELECT a.id AS author, s.title, s.text
                FROM (
                    SELECT DISTINCT ON (author_remote_id) *
                    FROM staging_author_annotations
                    WHERE author_remote_id IS NOT NULL AND text IS NOT NULL
                    ORDER BY author_remote_id, ctid DESC
                ) s
                JOIN authors a ON a.source = $1 AND a.remote_id = s.author_remote_id
            ), updated AS (
                UPDATE author_annotations t SET title = s.title, text = s.text
                FROM resolved s
                WHERE t.author = s.author
                  AND (t.title, t.text) IS DISTINCT FROM (s.title, s.text)
            )
            INSERT INTO author_annotations (author, title, text)
            SELECT s.author, s.title, s.text FROM resolved s
            WHERE NOT EXISTS (
                SELECT 1 FROM author_annotations t WHERE t.author = s.author
            )
            ON CONFLICT (author) DO NOTHING;
        ",
        params: Params::Source,
    },
    MergeStep {
        name: "author_annotation_pics update",
        sql: "
            UPDATE author_annotations
            SET file = s.file
            FROM (
                SELECT DISTINCT ON (author_remote_id) *
                FROM staging_author_annotation_pics
                WHERE author_remote_id IS NOT NULL
                ORDER BY author_remote_id, ctid DESC
            ) s
            JOIN authors a ON a.source = $1 AND a.remote_id = s.author_remote_id
            WHERE author_annotations.author = a.id
              AND author_annotations.file IS DISTINCT FROM s.file;
        ",
        params: Params::Source,
    },
    // -- 4. anti-join soft-delete of books removed upstream ------------------------
    MergeStep {
        name: "books soft-delete removed",
        sql: "
            UPDATE books SET is_deleted = true
            WHERE source = $1 AND is_deleted = false
              AND NOT EXISTS (
                  SELECT 1 FROM staging_books sb WHERE sb.remote_id = books.remote_id
              );
        ",
        params: Params::Source,
    },
    // -- 5. anti-join hard-delete of link rows removed upstream ---------------------
    MergeStep {
        name: "book_authors hard-delete removed",
        sql: "
            DELETE FROM book_authors ba USING books b
            WHERE ba.book = b.id AND b.source = $1
              AND NOT EXISTS (
                  SELECT 1 FROM d_book_authors d WHERE d.book = ba.book AND d.author = ba.author
              );
        ",
        params: Params::Source,
    },
    MergeStep {
        name: "book_genres hard-delete removed",
        sql: "
            DELETE FROM book_genres bg USING books b
            WHERE bg.book = b.id AND b.source = $1
              AND NOT EXISTS (
                  SELECT 1 FROM d_book_genres d WHERE d.book = bg.book AND d.genre = bg.genre
              );
        ",
        params: Params::Source,
    },
    MergeStep {
        name: "translations hard-delete removed",
        sql: "
            DELETE FROM translations t USING books b
            WHERE t.book = b.id AND b.source = $1
              AND NOT EXISTS (
                  SELECT 1 FROM d_translations d WHERE d.book = t.book AND d.author = t.author
              );
        ",
        params: Params::Source,
    },
    MergeStep {
        name: "book_sequences hard-delete removed",
        sql: "
            DELETE FROM book_sequences bs USING books b
            WHERE bs.book = b.id AND b.source = $1
              AND NOT EXISTS (
                  SELECT 1 FROM d_book_sequences d WHERE d.book = bs.book AND d.sequence = bs.sequence
              );
        ",
        params: Params::Source,
    },
];
