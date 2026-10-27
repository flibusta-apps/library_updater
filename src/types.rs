use std::cell::Cell;

use chrono::{NaiveDate, NaiveDateTime};
use sql_parse::Expression;
use tokio_postgres::types::{to_sql_checked, IsNull, ToSql, Type};
use tracing::log;

use crate::utils::{fix_annotation_text, parse_lang, remove_wrong_chars, unescape_mysql_string};

pub trait FromVecExpression<T> {
    fn from_vec_expression(value: &[Expression]) -> Result<T, ParseError>;
}

#[derive(Debug)]
pub struct ParseError {
    pub type_name: &'static str,
    pub field: &'static str,
    pub detail: String,
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "failed to parse {}.{}: {}",
            self.type_name, self.field, self.detail
        )
    }
}

impl std::error::Error for ParseError {}

/// Per-thread counters of "soft" data fixes applied while converting rows
/// (truncations, sentinel dates). A dump file is parsed on a single blocking
/// thread, so the producer reads them once per file with
/// [`take_row_counters`] and logs a summary instead of one line per row.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RowCounters {
    /// Text values truncated to the target column length.
    pub truncated: u64,
    /// `0000-00-00 ...` dates replaced by the sentinel date.
    pub zero_dates: u64,
    /// Other unparseable dates replaced by the sentinel date.
    pub bad_dates: u64,
}

thread_local! {
    static ROW_COUNTERS: Cell<RowCounters> = const { Cell::new(RowCounters { truncated: 0, zero_dates: 0, bad_dates: 0 }) };
}

/// Max number of per-row warnings (per thread/file) before only counting.
const MAX_ROW_WARNINGS: u64 = 20;

fn bump(f: impl FnOnce(&mut RowCounters)) -> RowCounters {
    ROW_COUNTERS.with(|c| {
        let mut v = c.get();
        f(&mut v);
        c.set(v);
        v
    })
}

/// Returns and resets this thread's counters.
pub fn take_row_counters() -> RowCounters {
    ROW_COUNTERS.with(|c| c.replace(RowCounters::default()))
}

/// Get column `i` of a row or a `ParseError` (never panics on short rows).
fn get<'v, 'a>(
    value: &'v [Expression<'a>],
    i: usize,
    type_name: &'static str,
    field: &'static str,
) -> Result<&'v Expression<'a>, ParseError> {
    value.get(i).ok_or_else(|| ParseError {
        type_name,
        field,
        detail: format!(
            "row has {} column(s), column index {} is required",
            value.len(),
            i
        ),
    })
}

fn unexpected(type_name: &'static str, field: &'static str, other: &Expression) -> ParseError {
    ParseError {
        type_name,
        field,
        detail: format!("unexpected value {:?}", other),
    }
}

fn get_int(
    value: &[Expression],
    i: usize,
    type_name: &'static str,
    field: &'static str,
) -> Result<u64, ParseError> {
    match get(value, i, type_name, field)? {
        Expression::Integer(v) => Ok(v.0),
        other => Err(unexpected(type_name, field, other)),
    }
}

fn get_str<'v>(
    value: &'v [Expression],
    i: usize,
    type_name: &'static str,
    field: &'static str,
) -> Result<&'v str, ParseError> {
    match get(value, i, type_name, field)? {
        Expression::String(v) => Ok(v.value.as_ref()),
        other => Err(unexpected(type_name, field, other)),
    }
}

fn get_opt_str<'v>(
    value: &'v [Expression],
    i: usize,
    type_name: &'static str,
    field: &'static str,
) -> Result<Option<&'v str>, ParseError> {
    match get(value, i, type_name, field)? {
        Expression::String(v) => Ok(Some(v.value.as_ref())),
        Expression::Null(_) => Ok(None),
        other => Err(unexpected(type_name, field, other)),
    }
}

/// Removes NUL characters (rejected by Postgres `text`) and, if `max_chars`
/// is given, truncates to that many *characters* (matching `VARCHAR(n)`
/// semantics), counting and warning about each truncation.
fn clean_text(
    s: &str,
    max_chars: Option<usize>,
    type_name: &'static str,
    field: &'static str,
    id: u64,
) -> String {
    let mut out = if s.contains('\0') {
        s.replace('\0', "")
    } else {
        s.to_owned()
    };

    if let Some(max) = max_chars {
        if let Some((idx, _)) = out.char_indices().nth(max) {
            let total_chars = out.chars().count();
            out.truncate(idx);
            let counters = bump(|c| c.truncated += 1);
            if counters.truncated <= MAX_ROW_WARNINGS {
                log::warn!(
                    "{type_name}(id={id}).{field}: value of {total_chars} chars truncated to {max}"
                );
            }
        }
    }

    out
}

/// Parse a Flibusta `Time` string into a `NaiveDate`.
///
/// Falls back to a sentinel date (`1970-01-01`) if the value doesn't match
/// the expected format. `0000-00-00 ...` (a known Flibusta oddity) is only
/// counted (see [`RowCounters::zero_dates`]); any other unparseable value is
/// counted and warned about (bounded).
fn parse_book_date(s: &str) -> NaiveDate {
    match NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        Ok(dt) => dt.date(),
        Err(_) => {
            if s.starts_with("0000-00-00") {
                bump(|c| c.zero_dates += 1);
            } else {
                let counters = bump(|c| c.bad_dates += 1);
                if counters.bad_dates <= MAX_ROW_WARNINGS {
                    log::warn!("Book.uploaded: unparseable date {:?}, using sentinel", s);
                }
            }
            NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()
        }
    }
}

/// A value that can be bound as a single COPY-in column. Enum wrapper so
/// `Staged::to_row` can return a homogeneous `Vec<Val>` for heterogeneous
/// column types.
#[derive(Debug)]
pub enum Val {
    I16(i16),
    I32(i32),
    Bool(bool),
    Date(NaiveDate),
    Str(String),
    OptStr(Option<String>),
}

impl ToSql for Val {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match self {
            Val::I16(v) => v.to_sql(ty, out),
            Val::I32(v) => v.to_sql(ty, out),
            Val::Bool(v) => v.to_sql(ty, out),
            Val::Date(v) => v.to_sql(ty, out),
            Val::Str(v) => v.to_sql(ty, out),
            Val::OptStr(v) => v.to_sql(ty, out),
        }
    }

    // `Val` can hold any of several underlying Postgres types depending on
    // which enum variant it is; the real type check happens at `to_sql`
    // time (delegated to the wrapped value's own `to_sql`), so `accepts`
    // always returns true here and `to_sql_checked!()` is relied on to
    // bypass the (otherwise-unusable) static accepts check.
    fn accepts(_ty: &Type) -> bool {
        true
    }

    to_sql_checked!();
}

/// Implemented by every dump-row type that gets bulk-loaded into a staging
/// table via `COPY ... FROM STDIN BINARY` (see `crate::updater::stage_file`).
/// Row-level range/type conversion failures (e.g. `u64` not fitting into the
/// target `i32`/`i16` column) are reported via `to_row` and cause that row to
/// be skipped, not the whole file to fail.
pub trait Staged: Sized {
    const STAGING_TABLE: &'static str;
    const COLUMNS: &'static [&'static str];

    fn column_types() -> Vec<Type>;

    fn to_row(&self) -> Result<Vec<Val>, ParseError>;
}

fn conv_i32(v: u64, type_name: &'static str, field: &'static str) -> Result<i32, ParseError> {
    i32::try_from(v).map_err(|e| ParseError {
        type_name,
        field,
        detail: e.to_string(),
    })
}

fn conv_i16(v: u64, type_name: &'static str, field: &'static str) -> Result<i16, ParseError> {
    i16::try_from(v).map_err(|e| ParseError {
        type_name,
        field,
        detail: e.to_string(),
    })
}

// Target column lengths, see book_library_server/migrations.
const AUTHOR_NAME_MAX: usize = 256;
const BOOK_TITLE_MAX: usize = 256;
const BOOK_LANG_MAX: usize = 3;
const BOOK_FILE_TYPE_MAX: usize = 4;
const SEQUENCE_NAME_MAX: usize = 256;
const ANNOTATION_TITLE_MAX: usize = 256;
const ANNOTATION_FILE_MAX: usize = 256;
const GENRE_CODE_MAX: usize = 45;
const GENRE_DESCRIPTION_MAX: usize = 99;
const GENRE_META_MAX: usize = 45;

/// Rejects (-> row skipped) a value that doesn't fit a short code column.
fn check_len(
    s: String,
    max: usize,
    type_name: &'static str,
    field: &'static str,
) -> Result<String, ParseError> {
    let len = s.chars().count();
    if len > max {
        return Err(ParseError {
            type_name,
            field,
            detail: format!("value {s:?} has {len} chars, column allows {max}"),
        });
    }
    Ok(s)
}

#[derive(Debug)]
pub struct Author {
    pub id: u64,
    pub last_name: String,
    pub first_name: String,
    pub middle_name: String,
}

impl FromVecExpression<Author> for Author {
    fn from_vec_expression(value: &[Expression]) -> Result<Author, ParseError> {
        const T: &str = "Author";
        let id = get_int(value, 0, T, "id")?;
        let last_name = clean_text(
            &remove_wrong_chars(get_str(value, 3, T, "last_name")?),
            Some(AUTHOR_NAME_MAX),
            T,
            "last_name",
            id,
        );
        let first_name = clean_text(
            &remove_wrong_chars(get_str(value, 1, T, "first_name")?),
            Some(AUTHOR_NAME_MAX),
            T,
            "first_name",
            id,
        );
        let middle_name = clean_text(
            &remove_wrong_chars(get_str(value, 2, T, "middle_name")?),
            Some(AUTHOR_NAME_MAX),
            T,
            "middle_name",
            id,
        );

        Ok(Author {
            id,
            last_name,
            first_name,
            middle_name,
        })
    }
}

impl Staged for Author {
    const STAGING_TABLE: &'static str = "staging_authors";
    const COLUMNS: &'static [&'static str] =
        &["remote_id", "first_name", "last_name", "middle_name"];

    fn column_types() -> Vec<Type> {
        vec![Type::INT4, Type::TEXT, Type::TEXT, Type::TEXT]
    }

    fn to_row(&self) -> Result<Vec<Val>, ParseError> {
        let id = conv_i32(self.id, "Author", "id")?;
        Ok(vec![
            Val::I32(id),
            Val::Str(self.first_name.clone()),
            Val::Str(self.last_name.clone()),
            Val::Str(self.middle_name.clone()),
        ])
    }
}

#[derive(Debug)]
pub struct Book {
    pub id: u64,
    pub title: String,
    pub lang: String,
    pub file_type: String,
    pub uploaded: NaiveDate,
    pub is_deleted: bool,
    pub pages: u64,
    pub year: u64,
}

impl FromVecExpression<Book> for Book {
    fn from_vec_expression(value: &[Expression]) -> Result<Book, ParseError> {
        const T: &str = "Book";
        let id = get_int(value, 0, T, "id")?;
        let title = clean_text(
            &remove_wrong_chars(get_str(value, 3, T, "title")?),
            Some(BOOK_TITLE_MAX),
            T,
            "title",
            id,
        );
        let lang = check_len(
            clean_text(
                &parse_lang(get_str(value, 5, T, "lang")?),
                None,
                T,
                "lang",
                id,
            ),
            BOOK_LANG_MAX,
            T,
            "lang",
        )?;
        let file_type = check_len(
            clean_text(
                &unescape_mysql_string(get_str(value, 8, T, "file_type")?),
                None,
                T,
                "file_type",
                id,
            ),
            BOOK_FILE_TYPE_MAX,
            T,
            "file_type",
        )?;
        let uploaded = parse_book_date(get_str(value, 2, T, "uploaded")?);
        let is_deleted = get_str(value, 11, T, "is_deleted")?.eq("1");
        let pages = get_int(value, 20, T, "pages")?;
        let year = match get(value, 10, T, "year")? {
            Expression::Integer(v) => v.0,
            Expression::Unary { .. } => 0,
            other => return Err(unexpected(T, "year", other)),
        };

        Ok(Book {
            id,
            title,
            lang,
            file_type,
            uploaded,
            is_deleted,
            pages,
            year,
        })
    }
}

impl Staged for Book {
    const STAGING_TABLE: &'static str = "staging_books";
    const COLUMNS: &'static [&'static str] = &[
        "remote_id",
        "title",
        "lang",
        "file_type",
        "uploaded",
        "is_deleted",
        "pages",
        "year",
    ];

    fn column_types() -> Vec<Type> {
        vec![
            Type::INT4,
            Type::TEXT,
            Type::TEXT,
            Type::TEXT,
            Type::DATE,
            Type::BOOL,
            Type::INT4,
            Type::INT2,
        ]
    }

    fn to_row(&self) -> Result<Vec<Val>, ParseError> {
        let id = conv_i32(self.id, "Book", "id")?;
        let pages = conv_i32(self.pages, "Book", "pages")?;
        let year = conv_i16(self.year, "Book", "year")?;
        Ok(vec![
            Val::I32(id),
            Val::Str(self.title.clone()),
            Val::Str(self.lang.clone()),
            Val::Str(self.file_type.clone()),
            Val::Date(self.uploaded),
            Val::Bool(self.is_deleted),
            Val::I32(pages),
            Val::I16(year),
        ])
    }
}

#[derive(Debug)]
pub struct BookAuthor {
    pub book_id: u64,
    pub author_id: u64,
    // TODO: position
}

impl FromVecExpression<BookAuthor> for BookAuthor {
    fn from_vec_expression(value: &[Expression]) -> Result<BookAuthor, ParseError> {
        const T: &str = "BookAuthor";
        Ok(BookAuthor {
            book_id: get_int(value, 0, T, "book_id")?,
            author_id: get_int(value, 1, T, "author_id")?,
        })
    }
}

impl Staged for BookAuthor {
    const STAGING_TABLE: &'static str = "staging_book_authors";
    const COLUMNS: &'static [&'static str] = &["book_remote_id", "author_remote_id"];

    fn column_types() -> Vec<Type> {
        vec![Type::INT4, Type::INT4]
    }

    fn to_row(&self) -> Result<Vec<Val>, ParseError> {
        let book_id = conv_i32(self.book_id, "BookAuthor", "book_id")?;
        let author_id = conv_i32(self.author_id, "BookAuthor", "author_id")?;
        Ok(vec![Val::I32(book_id), Val::I32(author_id)])
    }
}

#[derive(Debug)]
pub struct Translator {
    pub book_id: u64,
    pub author_id: u64,
    pub position: u64,
}

impl FromVecExpression<Translator> for Translator {
    fn from_vec_expression(value: &[Expression]) -> Result<Translator, ParseError> {
        const T: &str = "Translator";
        Ok(Translator {
            book_id: get_int(value, 0, T, "book_id")?,
            author_id: get_int(value, 1, T, "author_id")?,
            position: get_int(value, 2, T, "position")?,
        })
    }
}

impl Staged for Translator {
    const STAGING_TABLE: &'static str = "staging_translations";
    const COLUMNS: &'static [&'static str] = &["book_remote_id", "author_remote_id", "position"];

    fn column_types() -> Vec<Type> {
        vec![Type::INT4, Type::INT4, Type::INT2]
    }

    fn to_row(&self) -> Result<Vec<Val>, ParseError> {
        let book_id = conv_i32(self.book_id, "Translator", "book_id")?;
        let author_id = conv_i32(self.author_id, "Translator", "author_id")?;
        let position = conv_i16(self.position, "Translator", "position")?;
        Ok(vec![
            Val::I32(book_id),
            Val::I32(author_id),
            Val::I16(position),
        ])
    }
}

#[derive(Debug)]
pub struct Sequence {
    pub id: u64,
    pub name: String,
}

impl FromVecExpression<Sequence> for Sequence {
    fn from_vec_expression(value: &[Expression]) -> Result<Sequence, ParseError> {
        const T: &str = "Sequence";
        let id = get_int(value, 0, T, "id")?;
        let name = clean_text(
            &remove_wrong_chars(get_str(value, 1, T, "name")?),
            Some(SEQUENCE_NAME_MAX),
            T,
            "name",
            id,
        );

        Ok(Sequence { id, name })
    }
}

impl Staged for Sequence {
    const STAGING_TABLE: &'static str = "staging_sequences";
    const COLUMNS: &'static [&'static str] = &["remote_id", "name"];

    fn column_types() -> Vec<Type> {
        vec![Type::INT4, Type::TEXT]
    }

    fn to_row(&self) -> Result<Vec<Val>, ParseError> {
        let id = conv_i32(self.id, "Sequence", "id")?;
        Ok(vec![Val::I32(id), Val::Str(self.name.clone())])
    }
}

#[derive(Debug)]
pub struct SequenceInfo {
    pub book_id: u64,
    pub sequence_id: u64,
    pub position: u64,
}

impl FromVecExpression<SequenceInfo> for SequenceInfo {
    fn from_vec_expression(value: &[Expression]) -> Result<SequenceInfo, ParseError> {
        const T: &str = "SequenceInfo";
        let book_id = get_int(value, 0, T, "book_id")?;
        let sequence_id = get_int(value, 1, T, "sequence_id")?;
        let position = match get(value, 2, T, "position")? {
            Expression::Integer(v) => v.0,
            Expression::Unary {
                op,
                op_span: _,
                operand,
            } => match (op, operand.as_ref()) {
                (sql_parse::UnaryOperator::Minus, Expression::Integer(v)) => v.0,
                (_, _) => return Err(unexpected(T, "position", &value[2])),
            },
            other => return Err(unexpected(T, "position", other)),
        };

        Ok(SequenceInfo {
            book_id,
            sequence_id,
            position,
        })
    }
}

impl Staged for SequenceInfo {
    const STAGING_TABLE: &'static str = "staging_book_sequences";
    const COLUMNS: &'static [&'static str] = &["book_remote_id", "sequence_remote_id", "position"];

    fn column_types() -> Vec<Type> {
        vec![Type::INT4, Type::INT4, Type::INT2]
    }

    fn to_row(&self) -> Result<Vec<Val>, ParseError> {
        let book_id = conv_i32(self.book_id, "SequenceInfo", "book_id")?;
        let sequence_id = conv_i32(self.sequence_id, "SequenceInfo", "sequence_id")?;
        let position = conv_i16(self.position, "SequenceInfo", "position")?;
        Ok(vec![
            Val::I32(book_id),
            Val::I32(sequence_id),
            Val::I16(position),
        ])
    }
}

#[derive(Debug)]
pub struct BookAnnotation {
    pub book_id: u64,
    pub title: String,
    pub body: Option<String>,
}

impl FromVecExpression<BookAnnotation> for BookAnnotation {
    fn from_vec_expression(value: &[Expression]) -> Result<BookAnnotation, ParseError> {
        const T: &str = "BookAnnotation";
        let book_id = get_int(value, 0, T, "book_id")?;
        let title = clean_text(
            &unescape_mysql_string(get_str(value, 2, T, "title")?),
            Some(ANNOTATION_TITLE_MAX),
            T,
            "title",
            book_id,
        );
        let body = get_opt_str(value, 3, T, "body")?
            .map(|s| clean_text(&fix_annotation_text(s), None, T, "body", book_id));

        Ok(BookAnnotation {
            book_id,
            title,
            body,
        })
    }
}

impl Staged for BookAnnotation {
    const STAGING_TABLE: &'static str = "staging_book_annotations";
    const COLUMNS: &'static [&'static str] = &["book_remote_id", "title", "text"];

    fn column_types() -> Vec<Type> {
        vec![Type::INT4, Type::TEXT, Type::TEXT]
    }

    fn to_row(&self) -> Result<Vec<Val>, ParseError> {
        let book_id = conv_i32(self.book_id, "BookAnnotation", "book_id")?;
        Ok(vec![
            Val::I32(book_id),
            Val::Str(self.title.clone()),
            Val::OptStr(self.body.clone()),
        ])
    }
}

#[derive(Debug)]
pub struct BookAnnotationPic {
    pub book_id: u64,
    pub file: String,
}

impl FromVecExpression<BookAnnotationPic> for BookAnnotationPic {
    fn from_vec_expression(value: &[Expression]) -> Result<BookAnnotationPic, ParseError> {
        const T: &str = "BookAnnotationPic";
        let book_id = get_int(value, 0, T, "book_id")?;
        let file = clean_text(
            &unescape_mysql_string(get_str(value, 2, T, "file")?),
            Some(ANNOTATION_FILE_MAX),
            T,
            "file",
            book_id,
        );

        Ok(BookAnnotationPic { book_id, file })
    }
}

impl Staged for BookAnnotationPic {
    const STAGING_TABLE: &'static str = "staging_book_annotation_pics";
    const COLUMNS: &'static [&'static str] = &["book_remote_id", "file"];

    fn column_types() -> Vec<Type> {
        vec![Type::INT4, Type::TEXT]
    }

    fn to_row(&self) -> Result<Vec<Val>, ParseError> {
        let book_id = conv_i32(self.book_id, "BookAnnotationPic", "book_id")?;
        Ok(vec![Val::I32(book_id), Val::Str(self.file.clone())])
    }
}

#[derive(Debug)]
pub struct AuthorAnnotation {
    pub author_id: u64,
    pub title: String,
    pub body: Option<String>,
}

impl FromVecExpression<AuthorAnnotation> for AuthorAnnotation {
    fn from_vec_expression(value: &[Expression]) -> Result<AuthorAnnotation, ParseError> {
        const T: &str = "AuthorAnnotation";
        let author_id = get_int(value, 0, T, "author_id")?;
        let title = clean_text(
            &unescape_mysql_string(get_str(value, 2, T, "title")?),
            Some(ANNOTATION_TITLE_MAX),
            T,
            "title",
            author_id,
        );
        let body = get_opt_str(value, 3, T, "body")?
            .map(|s| clean_text(&fix_annotation_text(s), None, T, "body", author_id));

        Ok(AuthorAnnotation {
            author_id,
            title,
            body,
        })
    }
}

impl Staged for AuthorAnnotation {
    const STAGING_TABLE: &'static str = "staging_author_annotations";
    const COLUMNS: &'static [&'static str] = &["author_remote_id", "title", "text"];

    fn column_types() -> Vec<Type> {
        vec![Type::INT4, Type::TEXT, Type::TEXT]
    }

    fn to_row(&self) -> Result<Vec<Val>, ParseError> {
        let author_id = conv_i32(self.author_id, "AuthorAnnotation", "author_id")?;
        Ok(vec![
            Val::I32(author_id),
            Val::Str(self.title.clone()),
            Val::OptStr(self.body.clone()),
        ])
    }
}

#[derive(Debug)]
pub struct AuthorAnnotationPic {
    pub author_id: u64,
    pub file: String,
}

impl FromVecExpression<AuthorAnnotationPic> for AuthorAnnotationPic {
    fn from_vec_expression(value: &[Expression]) -> Result<AuthorAnnotationPic, ParseError> {
        const T: &str = "AuthorAnnotationPic";
        let author_id = get_int(value, 0, T, "author_id")?;
        let file = clean_text(
            &unescape_mysql_string(get_str(value, 2, T, "file")?),
            Some(ANNOTATION_FILE_MAX),
            T,
            "file",
            author_id,
        );

        Ok(AuthorAnnotationPic { author_id, file })
    }
}

impl Staged for AuthorAnnotationPic {
    const STAGING_TABLE: &'static str = "staging_author_annotation_pics";
    const COLUMNS: &'static [&'static str] = &["author_remote_id", "file"];

    fn column_types() -> Vec<Type> {
        vec![Type::INT4, Type::TEXT]
    }

    fn to_row(&self) -> Result<Vec<Val>, ParseError> {
        let author_id = conv_i32(self.author_id, "AuthorAnnotationPic", "author_id")?;
        Ok(vec![Val::I32(author_id), Val::Str(self.file.clone())])
    }
}

#[derive(Debug)]
pub struct Genre {
    pub id: u64,
    pub code: String,
    pub description: String,
    pub meta: String,
}

impl FromVecExpression<Genre> for Genre {
    fn from_vec_expression(value: &[Expression]) -> Result<Genre, ParseError> {
        const T: &str = "Genre";
        let id = get_int(value, 0, T, "id")?;
        let code = clean_text(
            &unescape_mysql_string(get_str(value, 1, T, "code")?),
            Some(GENRE_CODE_MAX),
            T,
            "code",
            id,
        );
        let description = clean_text(
            &unescape_mysql_string(get_str(value, 2, T, "description")?),
            Some(GENRE_DESCRIPTION_MAX),
            T,
            "description",
            id,
        );
        let meta = clean_text(
            &unescape_mysql_string(get_str(value, 3, T, "meta")?),
            Some(GENRE_META_MAX),
            T,
            "meta",
            id,
        );

        Ok(Genre {
            id,
            code,
            description,
            meta,
        })
    }
}

impl Staged for Genre {
    const STAGING_TABLE: &'static str = "staging_genres";
    const COLUMNS: &'static [&'static str] = &["remote_id", "code", "description", "meta"];

    fn column_types() -> Vec<Type> {
        vec![Type::INT4, Type::TEXT, Type::TEXT, Type::TEXT]
    }

    fn to_row(&self) -> Result<Vec<Val>, ParseError> {
        let id = conv_i32(self.id, "Genre", "id")?;
        Ok(vec![
            Val::I32(id),
            Val::Str(self.code.clone()),
            Val::Str(self.description.clone()),
            Val::Str(self.meta.clone()),
        ])
    }
}

#[derive(Debug)]
pub struct BookGenre {
    pub book_id: u64,
    pub genre_id: u64,
}

impl FromVecExpression<BookGenre> for BookGenre {
    fn from_vec_expression(value: &[Expression]) -> Result<BookGenre, ParseError> {
        const T: &str = "BookGenre";
        Ok(BookGenre {
            book_id: get_int(value, 1, T, "book_id")?,
            genre_id: get_int(value, 2, T, "genre_id")?,
        })
    }
}

impl Staged for BookGenre {
    const STAGING_TABLE: &'static str = "staging_book_genres";
    const COLUMNS: &'static [&'static str] = &["book_remote_id", "genre_remote_id"];

    fn column_types() -> Vec<Type> {
        vec![Type::INT4, Type::INT4]
    }

    fn to_row(&self) -> Result<Vec<Val>, ParseError> {
        let book_id = conv_i32(self.book_id, "BookGenre", "book_id")?;
        let genre_id = conv_i32(self.genre_id, "BookGenre", "genre_id")?;
        Ok(vec![Val::I32(book_id), Val::I32(genre_id)])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::updater::{parse_insert_values, parse_line, LineOutcome};

    #[test]
    fn parse_book_date_falls_back_on_zero_date() {
        let d = parse_book_date("0000-00-00 00:00:00");
        assert_eq!(d, NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
    }

    #[test]
    fn parse_book_date_falls_back_on_date_only() {
        let d = parse_book_date("2020-01-01");
        assert_eq!(d, NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
    }

    #[test]
    fn parse_book_date_parses_valid_datetime() {
        let d = parse_book_date("2020-01-01 12:00:00");
        assert_eq!(d, NaiveDate::from_ymd_opt(2020, 1, 1).unwrap());
    }

    // --- FromVecExpression fixture tests -----------------------------------
    //
    // Each fixture is a real MySQL/MariaDB `INSERT ... VALUES (...)` line as
    // produced by a Flibusta dump, run through the exact same
    // `parse_insert_values` (backed by `sql_parse`) that `stage_file_inner`
    // uses, so the `Expression` values fed to `from_vec_expression` below
    // are realistic rather than hand-rolled.

    /// Parses `line` and returns the first (and, for these fixtures, only)
    /// value row. Panics if the line doesn't parse as an `INSERT` with at
    /// least one row - a bug in the fixture itself, not the code under test.
    fn first_row(line: &str) -> Vec<Expression<'_>> {
        let mut rows = parse_insert_values(line);
        assert!(
            !rows.is_empty(),
            "fixture line didn't parse as INSERT: {line}"
        );
        rows.remove(0)
    }

    // -- Author ---------------------------------------------------------

    #[test]
    fn author_happy_path() {
        let row = first_row("INSERT INTO `libavtorname` VALUES (1,'John','J','Doe');");
        let author = Author::from_vec_expression(&row).unwrap();
        assert_eq!(author.id, 1);
        assert_eq!(author.first_name, "John");
        assert_eq!(author.middle_name, "J");
        assert_eq!(author.last_name, "Doe");
    }

    #[test]
    fn author_edge_case_escaped_newline_folded() {
        let a = one::<Author>("INSERT INTO `libavtorname` VALUES (2,'Conan','','O\\nBrien');");
        assert_eq!(
            a.last_name, "O Brien",
            "unescaped literal newline must be folded to a space by remove_wrong_chars"
        );
    }

    // -- end-to-end: raw INSERT line -> final field value ---------------------

    /// Parses a full `INSERT` line through the production path
    /// (`parse_line`: normalization + sql_parse + `from_vec_expression`) and
    /// returns the single resulting entity.
    fn one<T: FromVecExpression<T> + std::fmt::Debug>(line: &str) -> T {
        match parse_line::<T>(line) {
            LineOutcome::Rows(mut rows) => {
                assert_eq!(rows.len(), 1, "expected exactly one row for {line}");
                rows.remove(0).unwrap_or_else(|e| panic!("{e} for {line}"))
            }
            other => panic!("unexpected outcome {other:?} for {line}"),
        }
    }

    fn last_name(sql_literal: &str) -> String {
        one::<Author>(&format!(
            "INSERT INTO `libavtorname` VALUES (1,'F','M',{sql_literal});"
        ))
        .last_name
    }

    #[test]
    fn e2e_escaped_single_quote_keeps_following_char() {
        assert_eq!(last_name(r"'O\'Brien'"), "O'Brien");
        assert_eq!(last_name(r"'Д\'Артаньян'"), "Д'Артаньян");
        assert_eq!(last_name(r"'Can\'t'"), "Can't");
    }

    #[test]
    fn e2e_backslash_before_closing_quote() {
        // `'a\\'` is the literal `a\` (one backslash) - quote is NOT escaped.
        assert_eq!(last_name(r"'a\\'"), "a\\");
        // ...and the rest of the row/line still parses.
        let rows = match parse_line::<Author>(
            r"INSERT INTO `libavtorname` VALUES (1,'F','M','a\\'),(2,'G','N','b');",
        ) {
            LineOutcome::Rows(r) => r,
            other => panic!("{other:?}"),
        };
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[1].as_ref().unwrap().last_name, "b");
    }

    #[test]
    fn e2e_escaped_backslash_then_escaped_quote() {
        // literal: a\\\'b -> a, backslash, quote, b
        assert_eq!(last_name(r"'a\\\'b'"), "a\\'b");
    }

    #[test]
    fn e2e_doubled_quote() {
        assert_eq!(last_name("'It''s'"), "It's");
    }

    #[test]
    fn e2e_other_escapes() {
        assert_eq!(last_name(r"'a\tb'"), "a\tb");
        assert_eq!(last_name(r"'a\\b'"), "a\\b");
        assert_eq!(last_name(r#"'say \"hi\"'"#), "say \"hi\"");
        assert_eq!(last_name(r"'a\0b'"), "ab", "NUL must be stripped");
    }

    #[test]
    fn e2e_quote_inside_double_quoted_literal_and_neighbours() {
        // Neighbouring rows are unaffected by an escaped quote in the first.
        let rows = match parse_line::<Author>(
            r"INSERT INTO `libavtorname` VALUES (1,'A','B','O\'Neil'),(2,'C','D','Smith');",
        ) {
            LineOutcome::Rows(r) => r,
            other => panic!("{other:?}"),
        };
        assert_eq!(rows[0].as_ref().unwrap().last_name, "O'Neil");
        assert_eq!(rows[1].as_ref().unwrap().last_name, "Smith");
    }

    #[test]
    fn short_row_is_an_error_not_a_panic() {
        match parse_line::<Book>("INSERT INTO `libbook` VALUES (1,2,3);") {
            LineOutcome::Rows(rows) => {
                let err = rows.into_iter().next().unwrap().unwrap_err();
                assert_eq!(err.type_name, "Book");
            }
            other => panic!("{other:?}"),
        }
        match parse_line::<Genre>("INSERT INTO `libgenrelist` VALUES (1);") {
            LineOutcome::Rows(rows) => assert!(rows[0].is_err()),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn hex_literal_does_not_panic() {
        match parse_line::<Author>("INSERT INTO `libavtorname` VALUES (1,'F','M',X'4142');") {
            LineOutcome::Rows(rows) => assert!(rows[0].is_err()),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn non_insert_lines_are_not_failures() {
        for l in ["", "LOCK TABLES `x` WRITE;", "UNLOCK TABLES;", "-- comment"] {
            assert!(
                matches!(parse_line::<Author>(l), LineOutcome::NotInsert),
                "{l}"
            );
        }
    }

    #[test]
    fn malformed_insert_is_a_failure() {
        assert!(matches!(
            parse_line::<Author>("INSERT INTO `x` VALUES (1, 'unterminated"),
            LineOutcome::Failed { .. }
        ));
    }

    #[test]
    fn long_title_is_truncated_by_chars() {
        let title = "Я".repeat(300);
        let line = format!(
            "INSERT INTO `libbook` VALUES (5,0,'2020-01-01 12:00:00','{title}',0,'ru',0,0,'fb2',0,2020,'0',0,0,0,0,0,0,0,0,150);"
        );
        let book = one::<Book>(&line);
        assert_eq!(book.title.chars().count(), 256);
    }

    #[test]
    fn invalid_lang_or_file_type_skips_row() {
        let mk = |lang: &str, ft: &str| {
            format!(
                "INSERT INTO `libbook` VALUES (5,0,'2020-01-01 12:00:00','T',0,'{lang}',0,0,'{ft}',0,2020,'0',0,0,0,0,0,0,0,0,150);"
            )
        };
        for line in [mk("abcd", "fb2"), mk("ru", "fb2xx")] {
            match parse_line::<Book>(&line) {
                LineOutcome::Rows(rows) => assert!(rows[0].is_err(), "{line}"),
                other => panic!("{other:?}"),
            }
        }
    }

    #[test]
    fn long_genre_description_truncated() {
        let d = "ж".repeat(150);
        let g = one::<Genre>(&format!(
            "INSERT INTO `libgenrelist` VALUES (1,'c','{d}','m');"
        ));
        assert_eq!(g.description.chars().count(), 99);
    }

    // -- Book -------------------------------------------------------------

    #[test]
    fn book_happy_path() {
        let row = first_row(
            "INSERT INTO `libbook` VALUES \
             (5,0,'2020-01-01 12:00:00','Some Title',0,'ru',0,0,'fb2',0,2020,'0',0,0,0,0,0,0,0,0,150);",
        );
        let book = Book::from_vec_expression(&row).unwrap();
        assert_eq!(book.id, 5);
        assert_eq!(book.title, "Some Title");
        assert_eq!(book.lang, "ru");
        assert_eq!(book.file_type, "fb2");
        assert_eq!(book.uploaded, NaiveDate::from_ymd_opt(2020, 1, 1).unwrap());
        assert!(!book.is_deleted);
        assert_eq!(book.pages, 150);
        assert_eq!(book.year, 2020);
    }

    #[test]
    fn book_edge_case_malformed_date_and_deleted_flag() {
        let row = first_row(
            "INSERT INTO `libbook` VALUES \
             (6,0,'0000-00-00 00:00:00','Broken Date Book',0,'ru',0,0,'fb2',0,2020,'1',0,0,0,0,0,0,0,0,10);",
        );
        let book = Book::from_vec_expression(&row).unwrap();
        assert_eq!(book.uploaded, NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
        assert!(book.is_deleted);
    }

    #[test]
    fn book_edge_case_negative_year_via_unary_minus() {
        let row = first_row(
            "INSERT INTO `libbook` VALUES \
             (7,0,'2020-01-01 12:00:00','Negative Year Book',0,'ru',0,0,'fb2',0,-5,'0',0,0,0,0,0,0,0,0,10);",
        );
        let book = Book::from_vec_expression(&row).unwrap();
        assert_eq!(
            book.year, 0,
            "Unary(-N) year is treated as 0, per legacy dump quirk"
        );
    }

    // -- BookAuthor ---------------------------------------------------------

    #[test]
    fn book_author_happy_path() {
        let row = first_row("INSERT INTO `libavtor` VALUES (10,20,1);");
        let link = BookAuthor::from_vec_expression(&row).unwrap();
        assert_eq!(link.book_id, 10);
        assert_eq!(link.author_id, 20);
    }

    #[test]
    fn book_author_edge_case_non_integer_field_errors() {
        let row = first_row("INSERT INTO `libavtor` VALUES ('not-an-id',20,1);");
        let err = BookAuthor::from_vec_expression(&row).unwrap_err();
        assert_eq!(err.field, "book_id");
    }

    // -- Translator ---------------------------------------------------------

    #[test]
    fn translator_happy_path() {
        let row = first_row("INSERT INTO `libtranslator` VALUES (10,20,1);");
        let t = Translator::from_vec_expression(&row).unwrap();
        assert_eq!(t.book_id, 10);
        assert_eq!(t.author_id, 20);
        assert_eq!(t.position, 1);
    }

    #[test]
    fn translator_edge_case_zero_position() {
        let row = first_row("INSERT INTO `libtranslator` VALUES (10,20,0);");
        let t = Translator::from_vec_expression(&row).unwrap();
        assert_eq!(t.position, 0);
    }

    // -- Sequence -------------------------------------------------------

    #[test]
    fn sequence_happy_path() {
        let row = first_row("INSERT INTO `libseqname` VALUES (3,'Foundation');");
        let seq = Sequence::from_vec_expression(&row).unwrap();
        assert_eq!(seq.id, 3);
        assert_eq!(seq.name, "Foundation");
    }

    #[test]
    fn sequence_edge_case_escaped_newline_folded_to_space() {
        let row = first_row("INSERT INTO `libseqname` VALUES (4,'Foo\\nBar');");
        let seq = Sequence::from_vec_expression(&row).unwrap();
        assert_eq!(seq.name, "Foo Bar");
    }

    // -- SequenceInfo -----------------------------------------------------

    #[test]
    fn sequence_info_happy_path() {
        let row = first_row("INSERT INTO `libseq` VALUES (10,3,2);");
        let info = SequenceInfo::from_vec_expression(&row).unwrap();
        assert_eq!(info.book_id, 10);
        assert_eq!(info.sequence_id, 3);
        assert_eq!(info.position, 2);
    }

    #[test]
    fn sequence_info_edge_case_negative_position_via_unary_minus() {
        let row = first_row("INSERT INTO `libseq` VALUES (10,3,-2);");
        let info = SequenceInfo::from_vec_expression(&row).unwrap();
        assert_eq!(
            info.position, 2,
            "negative dump positions are stored as their absolute value"
        );
    }

    // -- BookAnnotation -----------------------------------------------------

    #[test]
    fn book_annotation_happy_path() {
        let row =
            first_row("INSERT INTO `book_annotations_dump` VALUES (1,0,'Title','Some body text');");
        let ann = BookAnnotation::from_vec_expression(&row).unwrap();
        assert_eq!(ann.book_id, 1);
        assert_eq!(ann.title, "Title");
        assert_eq!(ann.body, Some("Some body text".to_string()));
    }

    #[test]
    fn book_annotation_edge_case_null_body() {
        let row = first_row("INSERT INTO `book_annotations_dump` VALUES (1,0,'Title',NULL);");
        let ann = BookAnnotation::from_vec_expression(&row).unwrap();
        assert_eq!(ann.body, None);
    }

    // -- BookAnnotationPic --------------------------------------------------

    #[test]
    fn book_annotation_pic_happy_path() {
        let row = first_row("INSERT INTO `book_annotations_pics_dump` VALUES (1,0,'pic1.jpg');");
        let pic = BookAnnotationPic::from_vec_expression(&row).unwrap();
        assert_eq!(pic.book_id, 1);
        assert_eq!(pic.file, "pic1.jpg");
    }

    #[test]
    fn book_annotation_pic_edge_case_escaped_double_quote_in_file() {
        let row =
            first_row("INSERT INTO `book_annotations_pics_dump` VALUES (1,0,'weird\\\"name.jpg');");
        let pic = BookAnnotationPic::from_vec_expression(&row).unwrap();
        assert_eq!(pic.file, "weird\"name.jpg");
    }

    // -- AuthorAnnotation -----------------------------------------------------

    #[test]
    fn author_annotation_happy_path() {
        let row = first_row(
            "INSERT INTO `author_annotations_dump` VALUES (1,0,'Title','Some body text');",
        );
        let ann = AuthorAnnotation::from_vec_expression(&row).unwrap();
        assert_eq!(ann.author_id, 1);
        assert_eq!(ann.title, "Title");
        assert_eq!(ann.body, Some("Some body text".to_string()));
    }

    #[test]
    fn author_annotation_edge_case_null_body() {
        let row = first_row("INSERT INTO `author_annotations_dump` VALUES (1,0,'Title',NULL);");
        let ann = AuthorAnnotation::from_vec_expression(&row).unwrap();
        assert_eq!(ann.body, None);
    }

    // -- AuthorAnnotationPic --------------------------------------------------

    #[test]
    fn author_annotation_pic_happy_path() {
        let row = first_row("INSERT INTO `author_annotations_pics_dump` VALUES (1,0,'pic1.jpg');");
        let pic = AuthorAnnotationPic::from_vec_expression(&row).unwrap();
        assert_eq!(pic.author_id, 1);
        assert_eq!(pic.file, "pic1.jpg");
    }

    #[test]
    fn author_annotation_pic_edge_case_escaped_double_quote_in_file() {
        let row = first_row(
            "INSERT INTO `author_annotations_pics_dump` VALUES (1,0,'weird\\\"name.jpg');",
        );
        let pic = AuthorAnnotationPic::from_vec_expression(&row).unwrap();
        assert_eq!(pic.file, "weird\"name.jpg");
    }

    // -- Genre -------------------------------------------------------------

    #[test]
    fn genre_happy_path() {
        let row = first_row(
            "INSERT INTO `libgenrelist` VALUES (1,'sf_history','Историческая фантастика','sf');",
        );
        let genre = Genre::from_vec_expression(&row).unwrap();
        assert_eq!(genre.id, 1);
        assert_eq!(genre.code, "sf_history");
        assert_eq!(genre.description, "Историческая фантастика");
        assert_eq!(genre.meta, "sf");
    }

    #[test]
    fn genre_edge_case_escaped_double_quote_in_description() {
        let row = first_row(
            "INSERT INTO `libgenrelist` VALUES (2,'code','A \\\"quoted\\\" genre','meta');",
        );
        let genre = Genre::from_vec_expression(&row).unwrap();
        assert_eq!(genre.description, "A \"quoted\" genre");
    }

    // -- BookGenre ------------------------------------------------------

    #[test]
    fn book_genre_happy_path() {
        let row = first_row("INSERT INTO `libgenre` VALUES (1,10,5);");
        let bg = BookGenre::from_vec_expression(&row).unwrap();
        assert_eq!(bg.book_id, 10);
        assert_eq!(bg.genre_id, 5);
    }

    #[test]
    fn book_genre_edge_case_non_integer_field_errors() {
        let row = first_row("INSERT INTO `libgenre` VALUES (1,10,'not-an-id');");
        let err = BookGenre::from_vec_expression(&row).unwrap_err();
        assert_eq!(err.field, "genre_id");
    }
}
