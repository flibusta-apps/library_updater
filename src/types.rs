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

/// Parse a Flibusta `Time` string into a `NaiveDate`.
///
/// Falls back to a sentinel date (`1970-01-01`) with a logged warning if the
/// value doesn't match the expected format (known Flibusta oddities include
/// `0000-00-00 00:00:00` and date-only values).
fn parse_book_date(s: &str) -> NaiveDate {
    match NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        Ok(dt) => dt.date(),
        Err(_) => {
            log::warn!("Book.uploaded: unparseable date {:?}, using sentinel", s);
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

#[derive(Debug)]
pub struct Author {
    pub id: u64,
    pub last_name: String,
    pub first_name: String,
    pub middle_name: String,
}

impl FromVecExpression<Author> for Author {
    fn from_vec_expression(value: &[Expression]) -> Result<Author, ParseError> {
        let id = match &value[0] {
            sql_parse::Expression::Integer(v) => v.0,
            other => {
                return Err(ParseError {
                    type_name: "Author",
                    field: "id",
                    detail: format!("{:?}", other),
                })
            }
        };
        let last_name = match &value[3] {
            sql_parse::Expression::String(v) => remove_wrong_chars(&v.value),
            other => {
                return Err(ParseError {
                    type_name: "Author",
                    field: "last_name",
                    detail: format!("{:?}", other),
                })
            }
        };
        let first_name = match &value[1] {
            sql_parse::Expression::String(v) => remove_wrong_chars(&v.value),
            other => {
                return Err(ParseError {
                    type_name: "Author",
                    field: "first_name",
                    detail: format!("{:?}", other),
                })
            }
        };
        let middle_name = match &value[2] {
            sql_parse::Expression::String(v) => remove_wrong_chars(&v.value),
            other => {
                return Err(ParseError {
                    type_name: "Author",
                    field: "middle_name",
                    detail: format!("{:?}", other),
                })
            }
        };

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
        let id = match &value[0] {
            sql_parse::Expression::Integer(v) => v.0,
            other => {
                return Err(ParseError {
                    type_name: "Book",
                    field: "id",
                    detail: format!("{:?}", other),
                })
            }
        };
        let title = match &value[3] {
            sql_parse::Expression::String(v) => remove_wrong_chars(&v.value),
            other => {
                return Err(ParseError {
                    type_name: "Book",
                    field: "title",
                    detail: format!("{:?}", other),
                })
            }
        };
        let lang = match &value[5] {
            sql_parse::Expression::String(v) => parse_lang(&v.value),
            other => {
                return Err(ParseError {
                    type_name: "Book",
                    field: "lang",
                    detail: format!("{:?}", other),
                })
            }
        };
        let file_type = match &value[8] {
            sql_parse::Expression::String(v) => unescape_mysql_string(&v.value),
            other => {
                return Err(ParseError {
                    type_name: "Book",
                    field: "file_type",
                    detail: format!("{:?}", other),
                })
            }
        };
        let uploaded = match &value[2] {
            sql_parse::Expression::String(v) => parse_book_date(&v.value),
            other => {
                return Err(ParseError {
                    type_name: "Book",
                    field: "uploaded",
                    detail: format!("{:?}", other),
                })
            }
        };
        let is_deleted = match &value[11] {
            sql_parse::Expression::String(v) => v.value.eq("1"),
            other => {
                return Err(ParseError {
                    type_name: "Book",
                    field: "is_deleted",
                    detail: format!("{:?}", other),
                })
            }
        };
        let pages = match &value[20] {
            sql_parse::Expression::Integer(v) => v.0,
            other => {
                return Err(ParseError {
                    type_name: "Book",
                    field: "pages",
                    detail: format!("{:?}", other),
                })
            }
        };
        let year = match &value[10] {
            sql_parse::Expression::Integer(v) => v.0,
            sql_parse::Expression::Unary { .. } => 0,
            other => {
                return Err(ParseError {
                    type_name: "Book",
                    field: "year",
                    detail: format!("{:?}", other),
                })
            }
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
        let book_id = match &value[0] {
            sql_parse::Expression::Integer(v) => v.0,
            other => {
                return Err(ParseError {
                    type_name: "BookAuthor",
                    field: "book_id",
                    detail: format!("{:?}", other),
                })
            }
        };
        let author_id = match &value[1] {
            sql_parse::Expression::Integer(v) => v.0,
            other => {
                return Err(ParseError {
                    type_name: "BookAuthor",
                    field: "author_id",
                    detail: format!("{:?}", other),
                })
            }
        };

        Ok(BookAuthor { book_id, author_id })
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
        let book_id = match &value[0] {
            sql_parse::Expression::Integer(v) => v.0,
            other => {
                return Err(ParseError {
                    type_name: "Translator",
                    field: "book_id",
                    detail: format!("{:?}", other),
                })
            }
        };
        let author_id = match &value[1] {
            sql_parse::Expression::Integer(v) => v.0,
            other => {
                return Err(ParseError {
                    type_name: "Translator",
                    field: "author_id",
                    detail: format!("{:?}", other),
                })
            }
        };
        let position = match &value[2] {
            sql_parse::Expression::Integer(v) => v.0,
            other => {
                return Err(ParseError {
                    type_name: "Translator",
                    field: "position",
                    detail: format!("{:?}", other),
                })
            }
        };

        Ok(Translator {
            book_id,
            author_id,
            position,
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
        let id = match &value[0] {
            sql_parse::Expression::Integer(v) => v.0,
            other => {
                return Err(ParseError {
                    type_name: "Sequence",
                    field: "id",
                    detail: format!("{:?}", other),
                })
            }
        };
        let name = match &value[1] {
            sql_parse::Expression::String(v) => remove_wrong_chars(&v.value),
            other => {
                return Err(ParseError {
                    type_name: "Sequence",
                    field: "name",
                    detail: format!("{:?}", other),
                })
            }
        };

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
        let book_id = match &value[0] {
            sql_parse::Expression::Integer(v) => v.0,
            other => {
                return Err(ParseError {
                    type_name: "SequenceInfo",
                    field: "book_id",
                    detail: format!("{:?}", other),
                })
            }
        };
        let sequence_id = match &value[1] {
            sql_parse::Expression::Integer(v) => v.0,
            other => {
                return Err(ParseError {
                    type_name: "SequenceInfo",
                    field: "sequence_id",
                    detail: format!("{:?}", other),
                })
            }
        };
        let position = match &value[2] {
            sql_parse::Expression::Integer(v) => v.0,
            sql_parse::Expression::Unary {
                op,
                op_span: _,
                operand,
            } => match (op, operand.as_ref()) {
                (sql_parse::UnaryOperator::Minus, Expression::Integer(v)) => v.0,
                (_, _) => {
                    return Err(ParseError {
                        type_name: "SequenceInfo",
                        field: "position",
                        detail: format!("{:?}", &value[2]),
                    })
                }
            },
            other => {
                return Err(ParseError {
                    type_name: "SequenceInfo",
                    field: "position",
                    detail: format!("{:?}", other),
                })
            }
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
        let book_id = match &value[0] {
            sql_parse::Expression::Integer(v) => v.0,
            other => {
                return Err(ParseError {
                    type_name: "BookAnnotation",
                    field: "book_id",
                    detail: format!("{:?}", other),
                })
            }
        };
        let title = match &value[2] {
            sql_parse::Expression::String(v) => unescape_mysql_string(&v.value),
            other => {
                return Err(ParseError {
                    type_name: "BookAnnotation",
                    field: "title",
                    detail: format!("{:?}", other),
                })
            }
        };
        let body = match &value[3] {
            sql_parse::Expression::String(v) => Some(fix_annotation_text(&v.value)),
            sql_parse::Expression::Null(_) => None,
            other => {
                return Err(ParseError {
                    type_name: "BookAnnotation",
                    field: "body",
                    detail: format!("{:?}", other),
                })
            }
        };

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
        let book_id = match &value[0] {
            sql_parse::Expression::Integer(v) => v.0,
            other => {
                return Err(ParseError {
                    type_name: "BookAnnotationPic",
                    field: "book_id",
                    detail: format!("{:?}", other),
                })
            }
        };
        let file = match &value[2] {
            sql_parse::Expression::String(v) => unescape_mysql_string(&v.value),
            other => {
                return Err(ParseError {
                    type_name: "BookAnnotationPic",
                    field: "file",
                    detail: format!("{:?}", other),
                })
            }
        };

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
        let author_id = match &value[0] {
            sql_parse::Expression::Integer(v) => v.0,
            other => {
                return Err(ParseError {
                    type_name: "AuthorAnnotation",
                    field: "author_id",
                    detail: format!("{:?}", other),
                })
            }
        };
        let title = match &value[2] {
            sql_parse::Expression::String(v) => unescape_mysql_string(&v.value),
            other => {
                return Err(ParseError {
                    type_name: "AuthorAnnotation",
                    field: "title",
                    detail: format!("{:?}", other),
                })
            }
        };
        let body = match &value[3] {
            sql_parse::Expression::String(v) => Some(fix_annotation_text(&v.value)),
            sql_parse::Expression::Null(_) => None,
            other => {
                return Err(ParseError {
                    type_name: "AuthorAnnotation",
                    field: "body",
                    detail: format!("{:?}", other),
                })
            }
        };

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
        let author_id = match &value[0] {
            sql_parse::Expression::Integer(v) => v.0,
            other => {
                return Err(ParseError {
                    type_name: "AuthorAnnotationPic",
                    field: "author_id",
                    detail: format!("{:?}", other),
                })
            }
        };
        let file = match &value[2] {
            sql_parse::Expression::String(v) => unescape_mysql_string(&v.value),
            other => {
                return Err(ParseError {
                    type_name: "AuthorAnnotationPic",
                    field: "file",
                    detail: format!("{:?}", other),
                })
            }
        };

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
        let id = match &value[0] {
            sql_parse::Expression::Integer(v) => v.0,
            other => {
                return Err(ParseError {
                    type_name: "Genre",
                    field: "id",
                    detail: format!("{:?}", other),
                })
            }
        };
        let code = match &value[1] {
            sql_parse::Expression::String(v) => unescape_mysql_string(&v.value),
            other => {
                return Err(ParseError {
                    type_name: "Genre",
                    field: "code",
                    detail: format!("{:?}", other),
                })
            }
        };
        let description = match &value[2] {
            sql_parse::Expression::String(v) => unescape_mysql_string(&v.value),
            other => {
                return Err(ParseError {
                    type_name: "Genre",
                    field: "description",
                    detail: format!("{:?}", other),
                })
            }
        };
        let meta = match &value[3] {
            sql_parse::Expression::String(v) => unescape_mysql_string(&v.value),
            other => {
                return Err(ParseError {
                    type_name: "Genre",
                    field: "meta",
                    detail: format!("{:?}", other),
                })
            }
        };

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
        let book_id = match &value[1] {
            sql_parse::Expression::Integer(v) => v.0,
            other => {
                return Err(ParseError {
                    type_name: "BookGenre",
                    field: "book_id",
                    detail: format!("{:?}", other),
                })
            }
        };
        let genre_id = match &value[2] {
            sql_parse::Expression::Integer(v) => v.0,
            other => {
                return Err(ParseError {
                    type_name: "BookGenre",
                    field: "genre_id",
                    detail: format!("{:?}", other),
                })
            }
        };

        Ok(BookGenre { book_id, genre_id })
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
    use crate::updater::parse_insert_values;

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
    fn author_edge_case_escaped_newline_and_null_middle_name_folded() {
        // Note: `sql_parse` 0.9's MariaDB-dialect string lexer has a known
        // off-by-one quirk specifically for backslash-escaped *single
        // quotes* (`\'`) that drops the character immediately following the
        // escape - a pre-existing crate issue orthogonal to
        // `unescape_mysql_string`, so fixtures here deliberately exercise
        // other MySQL escapes (`\n`, `\"`, `\\`) which round-trip correctly
        // through the crate instead.
        let row = first_row("INSERT INTO `libavtorname` VALUES (2,'Conan','','O\\nBrien');");
        let author = Author::from_vec_expression(&row).unwrap();
        assert_eq!(
            author.last_name, "O Brien",
            "unescaped literal newline must be folded to a space by remove_wrong_chars"
        );
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
