import asyncio
from typing import Optional

from aiologger import Logger
import aiomysql
import asyncpg

from app.services.updaters.base import BaseUpdater
from core.config import env_config


async def run(cmd) -> tuple[bytes, bytes, Optional[int]]:
    proc = await asyncio.create_subprocess_shell(
        cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )

    stdout, stderr = await proc.communicate()
    return stdout, stderr, proc.returncode


def remove_wrong_ch(s: str):
    return s.replace(";", "").replace("\n", " ").replace("ё", "е")


def remove_dots(s: str):
    return s.replace(".", "")


class FlUpdater(BaseUpdater):
    SOURCE: int

    FILES = [
        "lib.libavtor.sql",
        "lib.libbook.sql",
        "lib.libavtorname.sql",
        "lib.libtranslator.sql",
        "lib.libseqname.sql",
        "lib.libseq.sql",
        "lib.libgenre.sql",
        "lib.libgenrelist.sql",
        "lib.b.annotations.sql",
        "lib.b.annotations_pics.sql",
        "lib.a.annotations.sql",
        "lib.a.annotations_pics.sql",
    ]

    postgres_pool: asyncpg.Pool
    mysql_pool: aiomysql.Pool

    authors_updated_event: asyncio.Event
    books_updated_event: asyncio.Event
    sequences_updated_event: asyncio.Event
    genres_updated_event: asyncio.Event

    logger: Logger

    async def _import_dump(self, filename: str):
        await run(
            f"wget -O - {env_config.FL_BASE_URL}/sql/{filename}.gz | gunzip | "
            f"mysql -h {env_config.MYSQL_HOST} -u {env_config.MYSQL_USER} "
            f'-p"{env_config.MYSQL_PASSWORD}" {env_config.MYSQL_DB_NAME}'
        )
        await self.logger.info(f"Imported {filename}")

    async def _prepare(self):
        posgres_pool = await asyncpg.create_pool(
            database=env_config.POSTGRES_DB_NAME,
            host=env_config.POSTGRES_HOST,
            port=env_config.POSTGRES_PORT,
            user=env_config.POSTGRES_USER,
            password=env_config.POSTGRES_PASSWORD,
        )

        self.mysql_pool = await aiomysql.create_pool(
            db=env_config.MYSQL_DB_NAME,
            host=env_config.MYSQL_HOST,
            port=env_config.MYSQL_PORT,
            user=env_config.MYSQL_USER,
            password=env_config.MYSQL_PASSWORD,
        )

        if not posgres_pool:
            raise asyncpg.exceptions.PostgresConnectionError()

        self.postgres_pool = posgres_pool

    async def _set_source(self):
        await self.logger.info("Set source...")

        source_row = await self.postgres_pool.fetchrow(
            "SELECT id FROM sources WHERE name = 'flibusta';"
        )

        if not source_row:
            await self.postgres_pool.execute(
                "INSERT INTO sources (name) VALUES ('flibusta');"
            )

            source_row = await self.postgres_pool.fetchrow(
                "SELECT id FROM sources WHERE name = 'flibusta';"
            )

        self.SOURCE = source_row["id"]

        await self.logger.info("Source has set!")

    async def _update_authors(self):
        def prepare_author(row: list):
            return [
                self.SOURCE,
                row[0],
                remove_wrong_ch(row[1]),
                remove_wrong_ch(row[2]),
                remove_wrong_ch(row[3]),
            ]

        await self.logger.info("Update authors...")

        await self.postgres_pool.execute(
            """
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
        """
        )

        async with self.mysql_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT COUNT(*) FROM libavtorname;")

                (rows_count,) = await cursor.fetchone()

                for offset in range(0, rows_count, 4096):
                    await cursor.execute(
                        "SELECT AvtorId, FirstName, LastName, MiddleName FROM libavtorname LIMIT 4096 OFFSET {offset};".format(
                            offset=offset
                        )
                    )

                    rows = await cursor.fetchall()

                    await self.postgres_pool.executemany(
                        "SELECT update_author($1, $2, cast($3 as varchar), cast($4 as varchar), cast($5 as varchar));",
                        [prepare_author(row) for row in rows],
                    )

        self.authors_updated_event.set()

        await self.logger.info("Authors updated!")

    async def _update_books(self):
        replace_dict = {"ru-": "ru", "ru~": "ru"}

        def fix_lang(lang: str) -> str:
            lower_lang = lang.lower()
            replaced_lang = replace_dict.get(lower_lang, lower_lang)
            return replaced_lang

        def prepare_book(row: list):
            return [
                self.SOURCE,
                row[0],
                remove_wrong_ch(row[1]),
                fix_lang(row[2]),
                row[3],
                row[4],
                row[5] == "1",
            ]

        await self.logger.info("Update books...")

        await self.postgres_pool.execute(
            """
            CREATE OR REPLACE FUNCTION update_book(
                source_ smallint, remote_id_ int, title_ varchar, lang_ varchar,
                file_type_ varchar, uploaded_ date, is_deleted_ boolean
            ) RETURNS void AS $$
                BEGIN
                    IF EXISTS (SELECT * FROM books WHERE source = source_ AND remote_id = remote_id_) THEN
                        UPDATE books SET title = title_, lang = lang_, file_type = file_type_,
                                         uploaded = uploaded_, is_deleted = is_deleted
                        WHERE source = source_ AND remote_id = remote_id_;
                        RETURN;
                    END IF;

                    INSERT INTO books (source, remote_id, title, lang, file_type, uploaded, is_deleted)
                        VALUES (source_, remote_id_, title_, lang_, file_type_, uploaded_, is_deleted_);
                END;
            $$ LANGUAGE plpgsql;
        """
        )

        async with self.mysql_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT COUNT(*) FROM libbook;")

                (rows_count,) = await cursor.fetchone()

                for offset in range(0, rows_count, 4096):
                    await cursor.execute(
                        "SELECT BookId, Title, Lang, FileType, Time, Deleted FROM libbook LIMIT 4096 OFFSET {offset};".format(
                            offset=offset
                        )
                    )

                    rows = await cursor.fetchall()

                    await self.postgres_pool.executemany(
                        "SELECT update_book($1, $2, cast($3 as varchar), cast($4 as varchar), cast($5 as varchar), $6, $7);",
                        [prepare_book(row) for row in rows],
                    )

        self.books_updated_event.set()

        await self.logger.info("Books updated!")

    async def _update_books_authors(self):
        await self.books_updated_event.wait()
        await self.authors_updated_event.wait()

        await self.logger.info("Update books_authors...")

        await self.postgres_pool.execute(
            """
            CREATE OR REPLACE FUNCTION update_book_author(source_ smallint, book_ integer, author_ integer) RETURNS void AS $$
                DECLARE
                    book_id integer := -1;
                    author_id integer := -1;
                BEGIN
                    SELECT id INTO book_id FROM books WHERE source = source_ AND remote_id = book_;
                    SELECT id INTO author_id FROM authors WHERE source = source_ AND remote_id = author_;

                    IF EXISTS (SELECT * FROM book_authors WHERE book = book_id AND author = author_id) THEN
                        RETURN;
                    END IF;

                    INSERT INTO book_authors (book, author) VALUES (book_id, author_id);
                END;
            $$ LANGUAGE plpgsql;
        """
        )

        async with self.mysql_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT COUNT(*) FROM libavtorname;")

                (rows_count,) = await cursor.fetchone()

                for offset in range(0, rows_count, 4096):
                    await cursor.execute(
                        "SELECT BookId, AvtorId FROM libavtor LIMIT 4096 OFFSET {offset};".format(
                            offset=offset
                        )
                    )

                    rows = await cursor.fetchall()

                    await self.postgres_pool.executemany(
                        "SELECT update_book_author($1, $2, $3);",
                        [(self.SOURCE, *row) for row in rows],
                    )

        await self.logger.info("Books_authors updated!")

    async def _update_translations(self):
        await self.books_updated_event.wait()
        await self.authors_updated_event.wait()

        await self.logger.info("Update translations...")

        await self.postgres_pool.execute(
            """
            CREATE OR REPLACE FUNCTION update_translation(source_ smallint, book_ integer, author_ integer, position_ smallint) RETURNS void AS $$
                DECLARE
                    book_id integer := -1;
                    author_id integer := -1;
                BEGIN
                    SELECT id INTO book_id FROM books WHERE source = source_ AND remote_id = book_;
                    SELECT id INTO author_id FROM authors WHERE source = source_ AND remote_id = author_;

                    IF EXISTS (SELECT * FROM translations WHERE book = book_id AND author = author_id) THEN
                        UPDATE translations SET position = position_
                        WHERE book = book_id AND author = author_id;
                        RETURN;
                    END IF;

                    INSERT INTO translations (book, author, position) VALUES (book_id, author_id, position_);
                END;
            $$ LANGUAGE plpgsql;
        """
        )

        async with self.mysql_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    "SELECT COUNT(*) FROM libtranslator "
                    "WHERE BookId IN (SELECT BookId FROM libbook);"
                )

                (rows_count,) = await cursor.fetchone()

                for offset in range(0, rows_count, 4096):
                    await cursor.execute(
                        "SELECT BookId, TranslatorId, Pos FROM libtranslator "
                        "WHERE BookId IN (SELECT BookId FROM libbook) "
                        "LIMIT 4096 OFFSET {offset};".format(offset=offset)
                    )

                    rows = await cursor.fetchall()

                    await self.postgres_pool.executemany(
                        "SELECT update_translation($1, $2, $3, $4)",
                        [(self.SOURCE, *row) for row in rows],
                    )

        await self.logger.info("Translations updated!")

    async def _update_sequences(self):
        def prepare_sequence(row: list):
            return [
                self.SOURCE,
                row[0],
                remove_wrong_ch(row[1]),
            ]

        await self.logger.info("Update sequences...")

        await self.postgres_pool.execute(
            """
            CREATE OR REPLACE FUNCTION update_sequences(source_ smallint, remote_id_ int, name_ varchar) RETURNS void AS $$
                BEGIN
                    IF EXISTS (SELECT * FROM sequences WHERE source = source_ AND remote_id = remote_id_) THEN
                        UPDATE sequences SET name = name_ WHERE source = source_ AND remote_id = remote_id_;
                        RETURN;
                    END IF;

                    INSERT INTO sequences (source, remote_id, name) VALUES (source_, remote_id_, name_);
                END;
            $$ LANGUAGE plpgsql;
        """
        )

        async with self.mysql_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT COUNT(*) FROM libseqname;")

                (rows_count,) = await cursor.fetchone()

                for offset in range(0, rows_count, 4096):
                    await cursor.execute(
                        "SELECT SeqId, SeqName FROM libseqname LIMIT 4096 OFFSET {offset};".format(
                            offset=offset
                        )
                    )

                    rows = await cursor.fetchall()

                    await self.postgres_pool.executemany(
                        "SELECT update_sequences($1, $2, cast($3 as varchar));",
                        [prepare_sequence(row) for row in rows],
                    )

        self.sequences_updated_event.set()

        await self.logger.info("Sequences updated!")

    async def _update_sequences_info(self):
        await self.sequences_updated_event.wait()
        await self.books_updated_event.wait()

        await self.logger.info("Update book_sequences...")

        await self.postgres_pool.execute(
            """
            CREATE OR REPLACE FUNCTION update_book_sequence(source_ smallint, book_ integer, sequence_ integer, position_ smallint) RETURNS void AS $$
                DECLARE
                    book_id integer := -1;
                    sequence_id integer := -1;
                BEGIN
                    SELECT id INTO book_id FROM books WHERE source = source_ AND remote_id = book_;
                    SELECT id INTO sequence_id FROM sequences WHERE source = source_ AND remote_id = sequence_;

                    IF EXISTS (SELECT * FROM book_sequences WHERE book = book_id AND sequence = sequence_id) THEN
                        UPDATE book_sequences SET position = position_ WHERE book = book_id AND sequence = sequence_id;
                        RETURN;
                    END IF;

                    INSERT INTO book_sequences (book, sequence, position) VALUES (book_id, sequence_id, position_);
                END;
            $$ LANGUAGE plpgsql;
        """
        )

        async with self.mysql_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    "SELECT COUNT(*) FROM libseq "
                    "WHERE "
                    "BookId IN (SELECT BookId FROM libbook) AND "
                    "SeqId IN (SELECT SeqId FROM libseqname);"
                )

                (rows_count,) = await cursor.fetchone()

                for offset in range(0, rows_count, 4096):
                    await cursor.execute(
                        "SELECT BookId, SeqId, level FROM libseq "
                        "WHERE "
                        "BookId IN (SELECT BookId FROM libbook) AND "
                        "SeqId IN (SELECT SeqId FROM libseqname) "
                        "LIMIT 4096 OFFSET {offset};".format(offset=offset)
                    )

                    rows = await cursor.fetchall()

                    await self.postgres_pool.executemany(
                        "SELECT update_book_sequence($1, $2, $3, $4);",
                        [[self.SOURCE, *row] for row in rows],
                    )

        await self.logger.info("Book_sequences updated!")

    async def _update_book_annotations(self):
        await self.books_updated_event.wait()

        await self.logger.info("Update book_annotations...")

        await self.postgres_pool.execute(
            """
            CREATE OR REPLACE FUNCTION update_book_annotation(source_ smallint, book_ integer, title_ varchar, text_ text) RETURNS void AS $$
                DECLARE
                    book_id integer := -1;
                BEGIN
                    SELECT id INTO book_id FROM books WHERE source = source_ AND remote_id = book_;

                    IF EXISTS (SELECT * FROM book_annotations WHERE book = book_id) THEN
                        UPDATE book_annotations SET title = title_, text = text_ WHERE book = book_id;
                        RETURN;
                    END IF;

                    INSERT INTO book_annotations (book, title, text) VALUES (book_id, title_, text_);
                END;
            $$ LANGUAGE plpgsql;
        """
        )

        async with self.mysql_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    "SELECT COUNT(*) FROM libbannotations "
                    "WHERE BookId IN (SELECT BookId FROM libbook);"
                )

                (rows_count,) = await cursor.fetchone()

                for offset in range(0, rows_count, 4096):
                    await cursor.execute(
                        "SELECT BookId, Title, Body FROM libbannotations "
                        "WHERE BookId IN (SELECT BookId FROM libbook) "
                        "LIMIT 4096 OFFSET {offset};".format(offset=offset)
                    )

                    rows = await cursor.fetchall()

                    await self.postgres_pool.executemany(
                        "SELECT update_book_annotation($1, $2, cast($3 as varchar), cast($4 as text));",
                        [[self.SOURCE, *row] for row in rows],
                    )

        await self.logger.info("Book_annotations updated!")

        await self._update_book_annotations_pic()

    async def _update_book_annotations_pic(self):
        await self.logger.info("Update book_annotations_pic...")

        async with self.mysql_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT COUNT(*) FROM libbpics;")

                (rows_count,) = await cursor.fetchone()

                for offset in range(0, rows_count, 4096):
                    await cursor.execute(
                        "SELECT BookId, File FROM libbpics LIMIT 4096 OFFSET {offset};".format(
                            offset=offset
                        )
                    )

                    rows = await cursor.fetchall()

                    await self.postgres_pool.executemany(
                        "UPDATE book_annotations "
                        "SET file = cast($3 as varchar) "
                        "FROM (SELECT id FROM books WHERE source = $1 AND remote_id = $2) as books "
                        "WHERE book = books.id;",
                        [[self.SOURCE, *row] for row in rows],
                    )

        await self.logger.info("Book_annotation_pics updated!")

    async def _update_author_annotations(self):
        await self.authors_updated_event.wait()

        await self.logger.info("Update author_annotations...")

        await self.postgres_pool.execute(
            """
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
        """
        )

        async with self.mysql_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT COUNT(*) FROM libaannotations;")

                (rows_count,) = await cursor.fetchone()

                for offset in range(0, rows_count, 4096):
                    await cursor.execute(
                        "SELECT AvtorId, Title, Body FROM libaannotations LIMIT 4096 OFFSET {offset};".format(
                            offset=offset
                        )
                    )

                    rows = await cursor.fetchall()

                    await self.postgres_pool.executemany(
                        "SELECT update_author_annotation($1, $2, cast($3 as varchar), cast($4 as text));",
                        [[self.SOURCE, *row] for row in rows],
                    )

        await self.logger.info("Author_annotation_updated!")

        await self._update_author_annotations_pics()

    async def _update_author_annotations_pics(self):
        await self.logger.info("Update author_annotations_pic...")

        async with self.mysql_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT COUNT(*) FROM libapics;")

                (rows_count,) = await cursor.fetchone()

                for offset in range(0, rows_count, 4096):
                    await cursor.execute(
                        "SELECT AvtorId, File FROM libapics LIMIT 4096 OFFSET {offset};".format(
                            offset=offset
                        )
                    )

                    rows = await cursor.fetchall()

                    await self.postgres_pool.executemany(
                        "UPDATE author_annotations "
                        "SET file = cast($3 as varchar) "
                        "FROM (SELECT id FROM authors WHERE source = $1 AND remote_id = $2) as authors "
                        "WHERE author = authors.id;",
                        [[self.SOURCE, *row] for row in rows],
                    )

        await self.logger.info("Author_annotatioins_pic updated!")

    async def _update_genres(self):
        await self.logger.info("Update genres...")

        await self.postgres_pool.execute(
            """
            CREATE OR REPLACE FUNCTION update_genre(
                source_ smallint, remote_id_ int, code_ varchar, description_ varchar, meta_ varchar
            ) RETURNS void AS $$
                BEGIN
                    IF EXISTS (SELECT * FROM genres WHERE source = source_ AND remote_id = remote_id_) THEN
                        UPDATE genres SET code = code_, description = description_, meta = meta_
                        WHERE source = source_ AND remote_id = remote_id_;
                        RETURN;
                    END IF;

                    INSERT INTO authors (source, remote_id, code, description, meta)
                        VALUES (source_, remote_id_, code_, description_, meta_);
                END;
            $$ LANGUAGE plpgsql;
        """
        )

        async with self.mysql_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT COUNT(*) FROM libgenrelist;")

                (rows_count,) = await cursor.fetchone()

                for offset in range(0, rows_count, 4096):
                    await cursor.execute(
                        "SELECT GenreId, GenreCode, GenreDesc, GenreMeta FROM libgenrelist LIMIT 4096 OFFSET {offset};".format(
                            offset=offset
                        )
                    )

                    rows = await cursor.fetchall()
                    await self.postgres_pool.executemany(
                        "SELECT update_genre($1, $2, cast($3 as varchar), cast($4 as varchar), cast($5 as varchar));",
                        [[self.SOURCE, *row] for row in rows],
                    )

        await self.logger.info("Genres updated!")

    async def _update_books_genres(self):
        await self.books_updated_event.wait()
        await self.genres_updated_event.wait()

        await self.logger.info("Update book_genres...")

        await self.postgres_pool.execute(
            """
            CREATE OR REPLACE FUNCTION update_book_sequence(source_ smallint, book_ integer, genre_ integer) RETURNS void AS $$
                DECLARE
                    book_id integer := -1;
                    genre_id integer := -1;
                BEGIN
                    SELECT id INTO book_id FROM books WHERE source = source_ AND remote_id = book_;
                    SELECT id INTO genre_id FROM genres WHERE source = source_ AND remote_id = genre_;

                    IF EXISTS (SELECT * FROM book_genres WHERE book = book_id AND genre = genre_id) THEN
                        RETURN;
                    END IF;

                    INSERT INTO book_genres (book, genre) VALUES (book_id, genre_id);
                END;
            $$ LANGUAGE plpgsql;
        """
        )

        async with self.mysql_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT COUNT(*) FROM libgenre;")

                (rows_count,) = await cursor.fetchone()

                for offset in range(0, rows_count, 4096):
                    await cursor.execute(
                        "SELECT BookId, GenreId FROM libgenre LIMIT 4096 OFFSET {offset};".format(
                            offset=offset
                        )
                    )

                    rows = await cursor.fetchall()

                    await self.postgres_pool.executemany(
                        "SELECT update_book_sequence($1, $2, $3);",
                        [(self.SOURCE, *row) for row in rows],
                    )

        await self.logger.info("Book_genres updated!")

    async def _update(self) -> bool:
        self.logger = Logger.with_default_handlers()

        await self._prepare()

        await asyncio.gather(*[self._import_dump(filename) for filename in self.FILES])

        await self._set_source()

        self.authors_updated_event = asyncio.Event()
        self.books_updated_event = asyncio.Event()
        self.sequences_updated_event = asyncio.Event()
        self.genres_updated_event = asyncio.Event()

        await asyncio.gather(
            self._update_authors(),
            self._update_books(),
            self._update_books_authors(),
            self._update_translations(),
            self._update_sequences(),
            self._update_sequences_info(),
            self._update_book_annotations(),
            self._update_author_annotations(),
            self._update_genres(),
            self._update_books_genres(),
        )

        return True

    @classmethod
    async def update(cls) -> bool:
        updater = cls()
        return await updater._update()
