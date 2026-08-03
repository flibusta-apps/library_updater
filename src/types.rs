use async_trait::async_trait;
use chrono::{NaiveDate, NaiveDateTime};
use sql_parse::Expression;
use tokio_postgres::Client;
use tracing::log;

use crate::utils::{fix_annotation_text, parse_lang, remove_wrong_chars};

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

#[async_trait]
pub trait Update {
    async fn before_update(client: &Client) -> Result<(), Box<dyn std::error::Error + Send>>;

    async fn update(
        &self,
        client: &Client,
        source_id: i16,
    ) -> Result<(), Box<dyn std::error::Error + Send>>;

    async fn after_update(client: &Client) -> Result<(), Box<dyn std::error::Error + Send>>;
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

#[async_trait]
impl Update for Author {
    async fn before_update(client: &Client) -> Result<(), Box<dyn std::error::Error + Send>> {
        match client.execute(
            "
            CREATE OR REPLACE FUNCTION update_author(
                source_ smallint, remote_id_ int, first_name_ varchar, last_name_ varchar, middle_name_ varchar
            ) RETURNS void AS $$
                BEGIN
                    IF EXISTS (SELECT * FROM authors WHERE source = source_ AND remote_id = remote_id_) THEN
                        UPDATE authors SET first_name = first_name_, last_name = last_name_, middle_name = middle_name_
                        WHERE source = source_ AND remote_id = remote_id_;
                        RETURN;
                    END IF;
                    INSERT INTO authors (source, remote_id, first_name, last_name, middle_name)
                        VALUES (source_, remote_id_, first_name_, last_name_, middle_name_);
                END;
            $$ LANGUAGE plpgsql;
            "
            , &[]).await {
                Ok(_) => Ok(()),
                Err(err) => Err(Box::new(err)),
        }
    }

    async fn update(
        &self,
        client: &Client,
        source_id: i16,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        let id =
            i32::try_from(self.id).map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)?;

        match client.execute(
            "SELECT update_author($1, $2, cast($3 as varchar), cast($4 as varchar), cast($5 as varchar));",
            &[&source_id, &id, &self.first_name, &self.last_name, &self.middle_name]
        ).await {
            Ok(_) => Ok(()),
            Err(err) => Err(Box::new(err)),
        }
    }

    async fn after_update(_client: &Client) -> Result<(), Box<dyn std::error::Error + Send>> {
        Ok(())
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
            sql_parse::Expression::String(v) => v.value.to_string(),
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

#[async_trait]
impl Update for Book {
    async fn before_update(client: &Client) -> Result<(), Box<dyn std::error::Error + Send>> {
        match client.execute(
            "
            CREATE OR REPLACE FUNCTION update_book(
                source_ smallint, remote_id_ int, title_ varchar, lang_ varchar,
                file_type_ varchar, uploaded_ date, is_deleted_ boolean, pages_ int,
                year_ smallint
            ) RETURNS void AS $$
                BEGIN
                    IF EXISTS (SELECT * FROM books WHERE source = source_ AND remote_id = remote_id_) THEN
                        UPDATE books SET title = title_, lang = lang_, file_type = file_type_,
                                         uploaded = uploaded_, is_deleted = is_deleted_, pages = pages_,
                                         year = year_
                        WHERE source = source_ AND remote_id = remote_id_;
                        RETURN;
                    END IF;
                    INSERT INTO books (source, remote_id, title, lang, file_type, uploaded, is_deleted, pages, year)
                        VALUES (source_, remote_id_, title_, lang_, file_type_, uploaded_, is_deleted_, pages_, year_);
                END;
            $$ LANGUAGE plpgsql;
            "
            , &[]).await {
                Ok(_) => Ok(()),
                Err(err) => Err(Box::new(err)),
        }
    }

    async fn update(
        &self,
        client: &Client,
        source_id: i16,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        let id =
            i32::try_from(self.id).map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)?;
        let pages = i32::try_from(self.pages)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)?;
        let year = i16::try_from(self.year)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)?;

        match client.execute(
            "SELECT update_book($1, $2, cast($3 as varchar), cast($4 as varchar), cast($5 as varchar), $6, $7, $8, $9);",
            &[&source_id, &id, &self.title, &self.lang, &self.file_type, &self.uploaded, &self.is_deleted, &pages, &year]
        ).await {
            Ok(_) => Ok(()),
            Err(err) => Err(Box::new(err)),
        }
    }

    async fn after_update(client: &Client) -> Result<(), Box<dyn std::error::Error + Send>> {
        match client
            .execute(
                "UPDATE books SET is_deleted = 't' WHERE lang NOT IN ('ru', 'be', 'uk');",
                &[],
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(err) => Err(Box::new(err)),
        }
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

#[async_trait]
impl Update for BookAuthor {
    async fn before_update(client: &Client) -> Result<(), Box<dyn std::error::Error + Send>> {
        match client.execute(
            "
            CREATE OR REPLACE FUNCTION update_book_author(source_ smallint, book_ integer, author_ integer) RETURNS void AS $$
                DECLARE
                    book_id integer := -1;
                    author_id integer := -1;
                BEGIN
                    SELECT id INTO book_id FROM books WHERE source = source_ AND remote_id = book_;
                    SELECT id INTO author_id FROM authors WHERE source = source_ AND remote_id = author_;

                    IF book_id IS NULL OR author_id IS NULL THEN
                        RETURN;
                    END IF;

                    IF EXISTS (SELECT * FROM book_authors WHERE book = book_id AND author = author_id) THEN
                        RETURN;
                    END IF;

                    INSERT INTO book_authors (book, author) VALUES (book_id, author_id);
                END;
            $$ LANGUAGE plpgsql;
            "
            , &[]).await {
                Ok(_) => Ok(()),
                Err(err) => Err(Box::new(err)),
        }
    }

    async fn update(
        &self,
        client: &Client,
        source_id: i16,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        let book_id = i32::try_from(self.book_id)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)?;
        let author_id = i32::try_from(self.author_id)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)?;

        match client
            .execute(
                "SELECT update_book_author($1, $2, $3);",
                &[&source_id, &book_id, &author_id],
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(err) => Err(Box::new(err)),
        }
    }

    async fn after_update(_client: &Client) -> Result<(), Box<dyn std::error::Error + Send>> {
        Ok(())
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

#[async_trait]
impl Update for Translator {
    async fn before_update(client: &Client) -> Result<(), Box<dyn std::error::Error + Send>> {
        match client.execute(
            "
            CREATE OR REPLACE FUNCTION update_translation(source_ smallint, book_ integer, author_ integer, position_ smallint) RETURNS void AS $$
                DECLARE
                    book_id integer := -1;
                    author_id integer := -1;
                BEGIN
                    SELECT id INTO book_id FROM books WHERE source = source_ AND remote_id = book_;
                    SELECT id INTO author_id FROM authors WHERE source = source_ AND remote_id = author_;

                    IF book_id IS NULL OR author_id IS NULL THEN
                        RETURN;
                    END IF;

                    IF EXISTS (SELECT * FROM translations WHERE book = book_id AND author = author_id) THEN
                        UPDATE translations SET position = position_
                        WHERE book = book_id AND author = author_id;
                        RETURN;
                    END IF;
                    INSERT INTO translations (book, author, position) VALUES (book_id, author_id, position_);
                END;
            $$ LANGUAGE plpgsql;
            "
            , &[]).await {
                Ok(_) => Ok(()),
                Err(err) => Err(Box::new(err)),
        }
    }

    async fn update(
        &self,
        client: &Client,
        source_id: i16,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        let book_id = i32::try_from(self.book_id)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)?;
        let author_id = i32::try_from(self.author_id)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)?;
        let position = i16::try_from(self.position)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)?;

        match client
            .execute(
                "SELECT update_translation($1, $2, $3, $4);",
                &[&source_id, &book_id, &author_id, &position],
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(err) => Err(Box::new(err)),
        }
    }

    async fn after_update(_client: &Client) -> Result<(), Box<dyn std::error::Error + Send>> {
        Ok(())
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

#[async_trait]
impl Update for Sequence {
    async fn before_update(client: &Client) -> Result<(), Box<dyn std::error::Error + Send>> {
        match client.execute(
            "
            CREATE OR REPLACE FUNCTION update_sequences(source_ smallint, remote_id_ int, name_ varchar) RETURNS void AS $$
                BEGIN
                    IF EXISTS (SELECT * FROM sequences WHERE source = source_ AND remote_id = remote_id_) THEN
                        UPDATE sequences SET name = name_ WHERE source = source_ AND remote_id = remote_id_;
                        RETURN;
                    END IF;
                    INSERT INTO sequences (source, remote_id, name) VALUES (source_, remote_id_, name_);
                END;
            $$ LANGUAGE plpgsql;
            "
            , &[]).await {
                Ok(_) => Ok(()),
                Err(err) => Err(Box::new(err)),
        }
    }

    async fn update(
        &self,
        client: &Client,
        source_id: i16,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        let id =
            i32::try_from(self.id).map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)?;

        match client
            .execute(
                "SELECT update_sequences($1, $2, cast($3 as varchar));",
                &[&source_id, &id, &self.name],
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(err) => Err(Box::new(err)),
        }
    }

    async fn after_update(_client: &Client) -> Result<(), Box<dyn std::error::Error + Send>> {
        Ok(())
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

#[async_trait]
impl Update for SequenceInfo {
    async fn before_update(client: &Client) -> Result<(), Box<dyn std::error::Error + Send>> {
        match client.execute(
            "
            CREATE OR REPLACE FUNCTION update_book_sequence(source_ smallint, book_ integer, sequence_ integer, position_ smallint) RETURNS void AS $$
                DECLARE
                    book_id integer := -1;
                    sequence_id integer := -1;
                BEGIN
                    SELECT id INTO book_id FROM books WHERE source = source_ AND remote_id = book_;

                    IF book_id IS NULL THEN
                        RETURN;
                    END IF;

                    SELECT id INTO sequence_id FROM sequences WHERE source = source_ AND remote_id = sequence_;

                    IF sequence_id IS NULL THEN
                        RETURN;
                    END IF;

                    IF EXISTS (SELECT * FROM book_sequences WHERE book = book_id AND sequence = sequence_id) THEN
                        UPDATE book_sequences SET position = ABS(position_) WHERE book = book_id AND sequence = sequence_id;
                        RETURN;
                    END IF;
                    INSERT INTO book_sequences (book, sequence, position) VALUES (book_id, sequence_id, ABS(position_));
                END;
            $$ LANGUAGE plpgsql;
            "
            , &[]).await {
                Ok(_) => Ok(()),
                Err(err) => Err(Box::new(err)),
        }
    }

    async fn update(
        &self,
        client: &Client,
        source_id: i16,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        let book_id = i32::try_from(self.book_id)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)?;
        let sequence_id = i32::try_from(self.sequence_id)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)?;
        let position = i16::try_from(self.position)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)?;

        match client
            .execute(
                "SELECT update_book_sequence($1, $2, $3, $4);",
                &[&source_id, &book_id, &sequence_id, &position],
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(err) => Err(Box::new(err)),
        }
    }

    async fn after_update(_client: &Client) -> Result<(), Box<dyn std::error::Error + Send>> {
        Ok(())
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
            sql_parse::Expression::String(v) => v.value.to_string(),
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

#[async_trait]
impl Update for BookAnnotation {
    async fn before_update(client: &Client) -> Result<(), Box<dyn std::error::Error + Send>> {
        match client.execute(
            "
            CREATE OR REPLACE FUNCTION update_book_annotation(source_ smallint, book_ integer, title_ varchar, text_ text) RETURNS void AS $$
                DECLARE
                    book_id integer := -1;
                BEGIN
                    SELECT id INTO book_id FROM books WHERE source = source_ AND remote_id = book_;

                    IF book_id IS NULL THEN
                        RETURN;
                    END IF;

                    IF EXISTS (SELECT * FROM book_annotations WHERE book = book_id) THEN
                        UPDATE book_annotations SET title = title_, text = text_ WHERE book = book_id;
                        RETURN;
                    END IF;

                    INSERT INTO book_annotations (book, title, text) VALUES (book_id, title_, text_);
                END;
            $$ LANGUAGE plpgsql;
            "
            , &[]).await {
                Ok(_) => Ok(()),
                Err(err) => Err(Box::new(err)),
        }
    }

    async fn update(
        &self,
        client: &Client,
        source_id: i16,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        let book_id = i32::try_from(self.book_id)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)?;

        match client
            .execute(
                "SELECT update_book_annotation($1, $2, cast($3 as varchar), cast($4 as text));",
                &[&source_id, &book_id, &self.title, &self.body],
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(err) => Err(Box::new(err)),
        }
    }

    async fn after_update(_client: &Client) -> Result<(), Box<dyn std::error::Error + Send>> {
        Ok(())
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
            sql_parse::Expression::String(v) => v.value.to_string(),
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

#[async_trait]
impl Update for BookAnnotationPic {
    async fn before_update(_client: &Client) -> Result<(), Box<dyn std::error::Error + Send>> {
        Ok(())
    }

    async fn update(
        &self,
        client: &Client,
        source_id: i16,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        let book_id = i32::try_from(self.book_id)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)?;

        match client
            .execute(
                "\
UPDATE book_annotations \
SET file = cast($3 as varchar) \
FROM (SELECT id FROM books WHERE source = $1 AND remote_id = $2) as books \
WHERE book = books.id;\
            ",
                &[&source_id, &book_id, &self.file],
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(err) => Err(Box::new(err)),
        }
    }

    async fn after_update(_client: &Client) -> Result<(), Box<dyn std::error::Error + Send>> {
        Ok(())
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
            sql_parse::Expression::String(v) => v.value.to_string(),
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

#[async_trait]
impl Update for AuthorAnnotation {
    async fn before_update(client: &Client) -> Result<(), Box<dyn std::error::Error + Send>> {
        match client.execute(
            "
            CREATE OR REPLACE FUNCTION update_author_annotation(source_ smallint, author_ integer, title_ varchar, text_ text) RETURNS void AS $$
                DECLARE
                    author_id integer := -1;
                BEGIN
                    SELECT id INTO author_id FROM authors WHERE source = source_ AND remote_id = author_;

                    IF author_id IS NULL THEN
                        RETURN;
                    END IF;

                    IF EXISTS (SELECT * FROM author_annotations WHERE author = author_id) THEN
                        UPDATE author_annotations SET title = title_, text = text_ WHERE author = author_id;
                        RETURN;
                    END IF;
                    INSERT INTO author_annotations (author, title, text) VALUES (author_id, title_, text_);
                END;
            $$ LANGUAGE plpgsql;
            "
            , &[]).await {
                Ok(_) => Ok(()),
                Err(err) => Err(Box::new(err)),
        }
    }

    async fn update(
        &self,
        client: &Client,
        source_id: i16,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        let author_id = i32::try_from(self.author_id)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)?;

        match client
            .execute(
                "SELECT update_author_annotation($1, $2, cast($3 as varchar), cast($4 as text));",
                &[&source_id, &author_id, &self.title, &self.body],
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(err) => Err(Box::new(err)),
        }
    }

    async fn after_update(_client: &Client) -> Result<(), Box<dyn std::error::Error + Send>> {
        Ok(())
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
            sql_parse::Expression::String(v) => v.value.to_string(),
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

#[async_trait]
impl Update for AuthorAnnotationPic {
    async fn before_update(_client: &Client) -> Result<(), Box<dyn std::error::Error + Send>> {
        Ok(())
    }

    async fn update(
        &self,
        client: &Client,
        source_id: i16,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        let author_id = i32::try_from(self.author_id)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)?;

        match client
            .execute(
                "\
UPDATE author_annotations \
SET file = cast($3 as varchar) \
FROM (SELECT id FROM authors WHERE source = $1 AND remote_id = $2) as authors \
WHERE author = authors.id;",
                &[&source_id, &author_id, &self.file],
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(err) => Err(Box::new(err)),
        }
    }

    async fn after_update(_client: &Client) -> Result<(), Box<dyn std::error::Error + Send>> {
        Ok(())
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
            sql_parse::Expression::String(v) => v.value.to_string(),
            other => {
                return Err(ParseError {
                    type_name: "Genre",
                    field: "code",
                    detail: format!("{:?}", other),
                })
            }
        };
        let description = match &value[2] {
            sql_parse::Expression::String(v) => v.value.to_string(),
            other => {
                return Err(ParseError {
                    type_name: "Genre",
                    field: "description",
                    detail: format!("{:?}", other),
                })
            }
        };
        let meta = match &value[3] {
            sql_parse::Expression::String(v) => v.value.to_string(),
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

#[async_trait]
impl Update for Genre {
    async fn before_update(client: &Client) -> Result<(), Box<dyn std::error::Error + Send>> {
        match client
            .execute(
                "
            CREATE OR REPLACE FUNCTION update_genre(
                source_ smallint, remote_id_ int, code_ varchar, description_ varchar, meta_ varchar
            ) RETURNS void AS $$
                BEGIN
                    INSERT INTO genres (source, remote_id, code, description, meta)
                        VALUES (source_, remote_id_, code_, description_, meta_)
                    ON CONFLICT (source, remote_id) DO UPDATE SET
                        code = EXCLUDED.code,
                        description = EXCLUDED.description,
                        meta = EXCLUDED.meta;
                END;
            $$ LANGUAGE plpgsql;
            ",
                &[],
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(err) => Err(Box::new(err)),
        }
    }

    async fn update(
        &self,
        client: &Client,
        source_id: i16,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        let id =
            i32::try_from(self.id).map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)?;

        match client
            .execute(
                "SELECT update_genre($1, $2, cast($3 as varchar), cast($4 as varchar), cast($5 as varchar));",
                &[&source_id, &id, &self.code, &self.description, &self.meta]
            ).await
        {
            Ok(_) => Ok(()),
            Err(err) => Err(Box::new(err)),
        }
    }

    async fn after_update(_client: &Client) -> Result<(), Box<dyn std::error::Error + Send>> {
        Ok(())
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

#[async_trait]
impl Update for BookGenre {
    async fn before_update(client: &Client) -> Result<(), Box<dyn std::error::Error + Send>> {
        match client.execute(
            "
            CREATE OR REPLACE FUNCTION update_book_genre(source_ smallint, book_ integer, genre_ integer) RETURNS void AS $$
                DECLARE
                    book_id integer := -1;
                    genre_id integer := -1;
                BEGIN
                    SELECT id INTO book_id FROM books WHERE source = source_ AND remote_id = book_;
                    SELECT id INTO genre_id FROM genres WHERE source = source_ AND remote_id = genre_;

                    IF book_id IS NULL OR genre_id IS NULL THEN
                        RETURN;
                    END IF;

                    IF EXISTS (SELECT * FROM book_genres WHERE book = book_id AND genre = genre_id) THEN
                        RETURN;
                    END IF;

                    INSERT INTO book_genres (book, genre) VALUES (book_id, genre_id);
                END;
            $$ LANGUAGE plpgsql;
            "
            , &[]).await {
                Ok(_) => Ok(()),
                Err(err) => Err(Box::new(err)),
        }
    }

    async fn update(
        &self,
        client: &Client,
        source_id: i16,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        let book_id = i32::try_from(self.book_id)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)?;
        let genre_id = i32::try_from(self.genre_id)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)?;

        match client
            .execute(
                "SELECT update_book_genre($1, $2, $3);",
                &[&source_id, &book_id, &genre_id],
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(err) => Err(Box::new(err)),
        }
    }

    async fn after_update(_client: &Client) -> Result<(), Box<dyn std::error::Error + Send>> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
