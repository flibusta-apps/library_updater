use async_trait::async_trait;
use chrono::{NaiveDate, NaiveDateTime};
use sql_parse::Expression;
use tokio_postgres::Client;

use crate::utils::{fix_annotation_text, parse_lang, remove_wrong_chars};

pub trait FromVecExpression<T> {
    fn from_vec_expression(value: &[Expression]) -> T;
}

#[async_trait]
pub trait Update {
    async fn before_update(client: &Client) -> Result<(), Box<tokio_postgres::Error>>;

    async fn update(
        &self,
        client: &Client,
        source_id: i16,
    ) -> Result<(), Box<tokio_postgres::Error>>;

    async fn after_update(client: &Client) -> Result<(), Box<tokio_postgres::Error>>;
}

#[derive(Debug)]
pub struct Author {
    pub id: u64,
    pub last_name: String,
    pub first_name: String,
    pub middle_name: String,
}

impl FromVecExpression<Author> for Author {
    fn from_vec_expression(value: &[Expression]) -> Author {
        Author {
            id: match &value[0] {
                sql_parse::Expression::Integer(v) => v.0,
                _ => panic!("Author.id"),
            },
            last_name: match &value[3] {
                sql_parse::Expression::String(v) => remove_wrong_chars(&v.value),
                _ => panic!("Author.last_name"),
            },
            first_name: match &value[1] {
                sql_parse::Expression::String(v) => remove_wrong_chars(&v.value),
                _ => panic!("Author.first_name"),
            },
            middle_name: match &value[2] {
                sql_parse::Expression::String(v) => remove_wrong_chars(&v.value),
                _ => panic!("Author.middle_name"),
            },
        }
    }
}

#[async_trait]
impl Update for Author {
    async fn before_update(client: &Client) -> Result<(), Box<tokio_postgres::Error>> {
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
    ) -> Result<(), Box<tokio_postgres::Error>> {
        match client.execute(
            "SELECT update_author($1, $2, cast($3 as varchar), cast($4 as varchar), cast($5 as varchar));",
            &[&source_id, &(self.id as i32), &self.first_name, &self.last_name, &self.middle_name]
        ).await {
            Ok(_) => Ok(()),
            Err(err) => Err(Box::new(err)),
        }
    }

    async fn after_update(_client: &Client) -> Result<(), Box<tokio_postgres::Error>> {
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
    fn from_vec_expression(value: &[Expression]) -> Book {
        Book {
            id: match &value[0] {
                sql_parse::Expression::Integer(v) => v.0,
                _ => panic!("Book.id"),
            },
            title: match &value[3] {
                sql_parse::Expression::String(v) => remove_wrong_chars(&v.value),
                _ => panic!("Book.title"),
            },
            lang: match &value[5] {
                sql_parse::Expression::String(v) => parse_lang(&v.value),
                _ => panic!("Book.lang"),
            },
            file_type: match &value[8] {
                sql_parse::Expression::String(v) => v.value.to_string(),
                _ => panic!("Book.file_type"),
            },
            uploaded: match &value[2] {
                sql_parse::Expression::String(v) => {
                    NaiveDateTime::parse_from_str(&v.value, "%Y-%m-%d %H:%M:%S")
                        .unwrap()
                        .date()
                }
                _ => panic!("Book.uploaded"),
            },
            is_deleted: match &value[11] {
                sql_parse::Expression::String(v) => v.value.eq("1"),
                _ => panic!("Book.is_deleted"),
            },
            pages: match &value[20] {
                sql_parse::Expression::Integer(v) => v.0,
                _ => panic!("Book.id"),
            },
            year: match &value[10] {
                sql_parse::Expression::Integer(v) => v.0,
                _ => panic!("Book.year"),
            },
        }
    }
}

#[async_trait]
impl Update for Book {
    async fn before_update(client: &Client) -> Result<(), Box<tokio_postgres::Error>> {
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
    ) -> Result<(), Box<tokio_postgres::Error>> {
        match client.execute(
            "SELECT update_book($1, $2, cast($3 as varchar), cast($4 as varchar), cast($5 as varchar), $6, $7, $8, $9);",
            &[&source_id, &(self.id as i32), &self.title, &self.lang, &self.file_type, &self.uploaded, &self.is_deleted, &(self.pages as i32), &(self.year as i16)]
        ).await {
            Ok(_) => Ok(()),
            Err(err) => Err(Box::new(err)),
        }
    }

    async fn after_update(client: &Client) -> Result<(), Box<tokio_postgres::Error>> {
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
    fn from_vec_expression(value: &[Expression]) -> BookAuthor {
        BookAuthor {
            book_id: match &value[0] {
                sql_parse::Expression::Integer(v) => v.0,
                _ => panic!("BookAuthor.book_id"),
            },
            author_id: match &value[1] {
                sql_parse::Expression::Integer(v) => v.0,
                _ => panic!("BookAuthor.author_id"),
            },
        }
    }
}

#[async_trait]
impl Update for BookAuthor {
    async fn before_update(client: &Client) -> Result<(), Box<tokio_postgres::Error>> {
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
    ) -> Result<(), Box<tokio_postgres::Error>> {
        match client
            .execute(
                "SELECT update_book_author($1, $2, $3);",
                &[&source_id, &(self.book_id as i32), &(self.author_id as i32)],
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(err) => Err(Box::new(err)),
        }
    }

    async fn after_update(_client: &Client) -> Result<(), Box<tokio_postgres::Error>> {
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
    fn from_vec_expression(value: &[Expression]) -> Translator {
        Translator {
            book_id: match &value[0] {
                sql_parse::Expression::Integer(v) => v.0,
                _ => panic!("Translator.book_id"),
            },
            author_id: match &value[1] {
                sql_parse::Expression::Integer(v) => v.0,
                _ => panic!("Translator.author_id"),
            },
            position: match &value[2] {
                sql_parse::Expression::Integer(v) => v.0,
                _ => panic!("Translator.pos"),
            },
        }
    }
}

#[async_trait]
impl Update for Translator {
    async fn before_update(client: &Client) -> Result<(), Box<tokio_postgres::Error>> {
        match client.execute(
            "
            CREATE OR REPLACE FUNCTION update_translation(source_ smallint, book_ integer, author_ integer, position_ smallint) RETURNS void AS $$
                DECLARE
                    book_id integer := -1;
                    author_id integer := -1;
                BEGIN
                    SELECT id INTO book_id FROM books WHERE source = source_ AND remote_id = book_;

                    IF book_id IS NULL OR author_id IS NULL THEN
                        RETURN;
                    END IF;

                    SELECT id INTO author_id FROM authors WHERE source = source_ AND remote_id = author_;
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
    ) -> Result<(), Box<tokio_postgres::Error>> {
        match client
            .execute(
                "SELECT update_translation($1, $2, $3, $4);",
                &[
                    &source_id,
                    &(self.book_id as i32),
                    &(self.author_id as i32),
                    &(self.position as i16),
                ],
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(err) => Err(Box::new(err)),
        }
    }

    async fn after_update(_client: &Client) -> Result<(), Box<tokio_postgres::Error>> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct Sequence {
    pub id: u64,
    pub name: String,
}

impl FromVecExpression<Sequence> for Sequence {
    fn from_vec_expression(value: &[Expression]) -> Sequence {
        Sequence {
            id: match &value[0] {
                sql_parse::Expression::Integer(v) => v.0,
                _ => panic!("Sequence.id"),
            },
            name: match &value[1] {
                sql_parse::Expression::String(v) => remove_wrong_chars(&v.value),
                _ => panic!("Sequence.name"),
            },
        }
    }
}

#[async_trait]
impl Update for Sequence {
    async fn before_update(client: &Client) -> Result<(), Box<tokio_postgres::Error>> {
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
    ) -> Result<(), Box<tokio_postgres::Error>> {
        match client
            .execute(
                "SELECT update_sequences($1, $2, cast($3 as varchar));",
                &[&source_id, &(self.id as i32), &self.name],
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(err) => Err(Box::new(err)),
        }
    }

    async fn after_update(_client: &Client) -> Result<(), Box<tokio_postgres::Error>> {
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
    fn from_vec_expression(value: &[Expression]) -> SequenceInfo {
        SequenceInfo {
            book_id: match &value[0] {
                sql_parse::Expression::Integer(v) => v.0,
                _ => panic!("SequenceInfo.book_id"),
            },
            sequence_id: match &value[1] {
                sql_parse::Expression::Integer(v) => v.0,
                _ => panic!("SequenceInfo.sequence_id"),
            },
            position: match &value[2] {
                sql_parse::Expression::Integer(v) => v.0,
                sql_parse::Expression::Unary {
                    op,
                    op_span: _,
                    operand,
                } => match (op, operand.as_ref()) {
                    (sql_parse::UnaryOperator::Minus, Expression::Integer(v)) => v.0,
                    (_, _) => panic!("SequenceInfo.position = {:?}", &value[2]),
                },
                _ => panic!("SequenceInfo.position = {:?}", &value[2]),
            },
        }
    }
}

#[async_trait]
impl Update for SequenceInfo {
    async fn before_update(client: &Client) -> Result<(), Box<tokio_postgres::Error>> {
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
    ) -> Result<(), Box<tokio_postgres::Error>> {
        match client
            .execute(
                "SELECT update_book_sequence($1, $2, $3, $4);",
                &[
                    &source_id,
                    &(self.book_id as i32),
                    &(self.sequence_id as i32),
                    &(self.position as i16),
                ],
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(err) => Err(Box::new(err)),
        }
    }

    async fn after_update(_client: &Client) -> Result<(), Box<tokio_postgres::Error>> {
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
    fn from_vec_expression(value: &[Expression]) -> BookAnnotation {
        BookAnnotation {
            book_id: match &value[0] {
                sql_parse::Expression::Integer(v) => v.0,
                _ => panic!("BookAnnotation.book_id"),
            },
            title: match &value[2] {
                sql_parse::Expression::String(v) => v.value.to_string(),
                _ => panic!("BookAnnotation.title"),
            },
            body: match &value[3] {
                sql_parse::Expression::String(v) => Some(fix_annotation_text(&v.value)),
                sql_parse::Expression::Null(_) => None,
                _ => panic!("BookAnnotation.body"),
            },
        }
    }
}

#[async_trait]
impl Update for BookAnnotation {
    async fn before_update(client: &Client) -> Result<(), Box<tokio_postgres::Error>> {
        match client.execute(
            "
            CREATE OR REPLACE FUNCTION update_book_annotation(source_ smallint, book_ integer, title_ varchar, text_ text) RETURNS void AS $$
                DECLARE
                    book_id integer := -1;
                BEGIN
                    SELECT id INTO book_id FROM books WHERE source = source_ AND remote_id = book_;
                    IF EXISTS (SELECT * FROM book_annotations WHERE book = book_id) THEN
                        UPDATE book_annotations SET title = title_, text = text_ WHERE book = book_id;
                        RETURN;
                    END IF;

                    IF book_id IS NULL THEN
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
    ) -> Result<(), Box<tokio_postgres::Error>> {
        match client
            .execute(
                "SELECT update_book_annotation($1, $2, cast($3 as varchar), cast($4 as text));",
                &[&source_id, &(self.book_id as i32), &self.title, &self.body],
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(err) => Err(Box::new(err)),
        }
    }

    async fn after_update(_client: &Client) -> Result<(), Box<tokio_postgres::Error>> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct BookAnnotationPic {
    pub book_id: u64,
    pub file: String,
}

impl FromVecExpression<BookAnnotationPic> for BookAnnotationPic {
    fn from_vec_expression(value: &[Expression]) -> BookAnnotationPic {
        BookAnnotationPic {
            book_id: match &value[0] {
                sql_parse::Expression::Integer(v) => v.0,
                _ => panic!("BookAnnotationPic.book_id"),
            },
            file: match &value[2] {
                sql_parse::Expression::String(v) => v.value.to_string(),
                _ => panic!("BookAnnotationPic.file"),
            },
        }
    }
}

#[async_trait]
impl Update for BookAnnotationPic {
    async fn before_update(_client: &Client) -> Result<(), Box<tokio_postgres::Error>> {
        Ok(())
    }

    async fn update(
        &self,
        client: &Client,
        source_id: i16,
    ) -> Result<(), Box<tokio_postgres::Error>> {
        match client
            .execute(
                "\
UPDATE book_annotations \
SET file = cast($3 as varchar) \
FROM (SELECT id FROM books WHERE source = $1 AND remote_id = $2) as books \
WHERE book = books.id;\
            ",
                &[&source_id, &(self.book_id as i32), &self.file],
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(err) => Err(Box::new(err)),
        }
    }

    async fn after_update(_client: &Client) -> Result<(), Box<tokio_postgres::Error>> {
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
    fn from_vec_expression(value: &[Expression]) -> AuthorAnnotation {
        AuthorAnnotation {
            author_id: match &value[0] {
                sql_parse::Expression::Integer(v) => v.0,
                _ => panic!("AuthorAnnotation.author_id"),
            },
            title: match &value[2] {
                sql_parse::Expression::String(v) => v.value.to_string(),
                _ => panic!("AuthorAnnotation.title"),
            },
            body: match &value[3] {
                sql_parse::Expression::String(v) => Some(fix_annotation_text(&v.value)),
                sql_parse::Expression::Null(_) => None,
                _ => panic!("AuthorAnnotation.body"),
            },
        }
    }
}

#[async_trait]
impl Update for AuthorAnnotation {
    async fn before_update(client: &Client) -> Result<(), Box<tokio_postgres::Error>> {
        match client.execute(
            "
            CREATE OR REPLACE FUNCTION update_author_annotation(source_ smallint, author_ integer, title_ varchar, text_ text) RETURNS void AS $$
                DECLARE
                    author_id integer := -1;
                BEGIN
                    SELECT id INTO author_id FROM authors WHERE source = source_ AND remote_id = author_;
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
    ) -> Result<(), Box<tokio_postgres::Error>> {
        match client
            .execute(
                "SELECT update_author_annotation($1, $2, cast($3 as varchar), cast($4 as text));",
                &[
                    &source_id,
                    &(self.author_id as i32),
                    &self.title,
                    &self.body,
                ],
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(err) => Err(Box::new(err)),
        }
    }

    async fn after_update(_client: &Client) -> Result<(), Box<tokio_postgres::Error>> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct AuthorAnnotationPic {
    pub author_id: u64,
    pub file: String,
}

impl FromVecExpression<AuthorAnnotationPic> for AuthorAnnotationPic {
    fn from_vec_expression(value: &[Expression]) -> AuthorAnnotationPic {
        AuthorAnnotationPic {
            author_id: match &value[0] {
                sql_parse::Expression::Integer(v) => v.0,
                _ => panic!("AuthorAnnotationPic.book_id"),
            },
            file: match &value[2] {
                sql_parse::Expression::String(v) => v.value.to_string(),
                _ => panic!("AuthorAnnotationPic.file"),
            },
        }
    }
}

#[async_trait]
impl Update for AuthorAnnotationPic {
    async fn before_update(_client: &Client) -> Result<(), Box<tokio_postgres::Error>> {
        Ok(())
    }

    async fn update(
        &self,
        client: &Client,
        source_id: i16,
    ) -> Result<(), Box<tokio_postgres::Error>> {
        match client
            .execute(
                "\
UPDATE author_annotations \
SET file = cast($3 as varchar) \
FROM (SELECT id FROM authors WHERE source = $1 AND remote_id = $2) as authors \
WHERE author = authors.id;",
                &[&source_id, &(self.author_id as i32), &self.file],
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(err) => Err(Box::new(err)),
        }
    }

    async fn after_update(_client: &Client) -> Result<(), Box<tokio_postgres::Error>> {
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
    fn from_vec_expression(value: &[Expression]) -> Genre {
        Genre {
            id: match &value[0] {
                sql_parse::Expression::Integer(v) => v.0,
                _ => panic!("Genre.id"),
            },
            code: match &value[1] {
                sql_parse::Expression::String(v) => v.value.to_string(),
                _ => panic!("Genre.code = {:?}", &value[1]),
            },
            description: match &value[2] {
                sql_parse::Expression::String(v) => v.value.to_string(),
                _ => panic!("Genre.description = {:?}", &value[2]),
            },
            meta: match &value[3] {
                sql_parse::Expression::String(v) => v.value.to_string(),
                _ => panic!("Genre.meta"),
            },
        }
    }
}

#[async_trait]
impl Update for Genre {
    async fn before_update(client: &Client) -> Result<(), Box<tokio_postgres::Error>> {
        match client.execute(
            "
            CREATE OR REPLACE FUNCTION update_book_sequence(source_ smallint, book_ integer, genre_ integer) RETURNS void AS $$
                DECLARE
                    book_id integer := -1;
                    genre_id integer := -1;
                BEGIN
                    SELECT id INTO book_id FROM books WHERE source = source_ AND remote_id = book_;

                    IF book_id IS NULL THEN
                        RETURN;
                    END IF;

                    SELECT id INTO genre_id FROM genres WHERE source = source_ AND remote_id = genre_;
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
    ) -> Result<(), Box<tokio_postgres::Error>> {
        match client
            .execute(
                "SELECT update_genre($1, $2, cast($3 as varchar), cast($4 as varchar), cast($5 as varchar));",
                &[&source_id, &(self.id as i32), &self.code, &self.description, &self.meta]
            ).await
        {
            Ok(_) => Ok(()),
            Err(err) => Err(Box::new(err)),
        }
    }

    async fn after_update(_client: &Client) -> Result<(), Box<tokio_postgres::Error>> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct BookGenre {
    pub book_id: u64,
    pub genre_id: u64,
}

impl FromVecExpression<BookGenre> for BookGenre {
    fn from_vec_expression(value: &[Expression]) -> BookGenre {
        BookGenre {
            book_id: match &value[1] {
                sql_parse::Expression::Integer(v) => v.0,
                _ => panic!("BookGenre.book_id"),
            },
            genre_id: match &value[2] {
                sql_parse::Expression::Integer(v) => v.0,
                _ => panic!("BookGenre.genre_id"),
            },
        }
    }
}

#[async_trait]
impl Update for BookGenre {
    async fn before_update(_client: &Client) -> Result<(), Box<tokio_postgres::Error>> {
        Ok(())
    }

    async fn update(
        &self,
        client: &Client,
        source_id: i16,
    ) -> Result<(), Box<tokio_postgres::Error>> {
        match client
            .execute(
                "SELECT update_book_sequence($1, $2, $3);",
                &[&source_id, &(self.book_id as i32), &(self.genre_id as i32)],
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(err) => Err(Box::new(err)),
        }
    }

    async fn after_update(_client: &Client) -> Result<(), Box<tokio_postgres::Error>> {
        Ok(())
    }
}
