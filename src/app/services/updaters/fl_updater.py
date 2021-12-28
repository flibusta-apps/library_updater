from typing import Optional
import asyncio
import platform

import asyncpg
import aiomysql
from aiologger import Logger

from core.config import env_config
from app.services.updaters.base import BaseUpdater
from app.services.webhook import WebhookSender


async def run(cmd) -> tuple[bytes, bytes, Optional[int]]:
    proc = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE)

    stdout, stderr = await proc.communicate()
    return stdout, stderr, proc.returncode


def remove_wrong_ch(s: str):
    return s.replace(";", "").replace("\n", " ").replace('ё', 'е')


def remove_dots(s: str):
    return s.replace('.', '')


class FlUpdater(BaseUpdater):
    SOURCE: int

    FILES = [
        'lib.libavtor.sql',
        'lib.libbook.sql',
        'lib.libavtorname.sql',
        'lib.libtranslator.sql',
        'lib.libseqname.sql',
        'lib.libseq.sql',
        'lib.libgenre.sql',
        'lib.libgenrelist.sql',
        'lib.b.annotations.sql',
        'lib.b.annotations_pics.sql',
        'lib.a.annotations.sql',
        'lib.a.annotations_pics.sql',
    ]

    postgres_pool: asyncpg.Pool
    mysql_pool: aiomysql.Pool

    authors_updated_event: asyncio.Event
    books_updated_event: asyncio.Event
    sequences_updated_event: asyncio.Event
    genres_updated_event: asyncio.Event

    platform: str
    logger: Logger

    async def log(self, message):
        if 'windows' in self.platform.lower():
            print(message)
        else:
            await self.logger.info(message)

    async def _drop_tables(self):
        async with self.mysql_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                cursor.execute(
                    "DROP TABLE IF EXISTS libaannotations;"
                    "DROP TABLE IF EXISTS libapics;"
                    "DROP TABLE IF EXISTS libavtor;"
                    "DROP TABLE IF EXISTS libavtorname;"
                    "DROP TABLE IF EXISTS libbannotations;"
                    "DROP TABLE IF EXISTS libbook;"
                    "DROP TABLE IF EXISTS libbpics;"
                    "DROP TABLE IF EXISTS libgenre;"
                    "DROP TABLE IF EXISTS libgenrelist;"
                    "DROP TABLE IF EXISTS libseq;"
                    "DROP TABLE IF EXISTS libseqname;"
                    "DROP TABLE IF EXISTS libtranslator;"
                )

    async def _import_dump(self, filename: str):
        result = await run(
            f"wget -O - {env_config.FL_BASE_URL}/sql/{filename}.gz | gunzip | "
            f"mysql -h {env_config.MYSQL_HOST} -u {env_config.MYSQL_USER} "
            f"-p\"{env_config.MYSQL_PASSWORD}\" {env_config.MYSQL_DB_NAME}"
        )
        await self.log(f"Imported {filename}: {result}.")

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
        await self.log('Set source...')

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

        self.SOURCE = source_row['id']

        await self.log("Source has set!")

    async def _update_authors(self):
        def prepare_author(row: list):
            return [
                self.SOURCE,
                row[0],
                remove_wrong_ch(row[1]),
                remove_wrong_ch(row[2]),
                remove_wrong_ch(row[3])
            ]

        await self.log("Update authors...")

        async with self.mysql_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    "SELECT AvtorId, FirstName, LastName, MiddleName FROM libavtorname;"
                )

                while (rows := await cursor.fetchmany(32)):
                    await self.postgres_pool.executemany(
                        "INSERT INTO authors (source, remote_id, first_name, last_name, middle_name) "
                        "VALUES ($1, $2, cast($3 as varchar), cast($4 as varchar), cast($5 as varchar)) "
                        "ON CONFLICT (source, remote_id) "
                        "DO UPDATE SET first_name = EXCLUDED.first_name, last_name = EXCLUDED.last_name, middle_name = EXCLUDED.middle_name;",
                        [prepare_author(row) for row in rows]
                    )
        
        self.authors_updated_event.set()

        await self.log("Authors updated!")

    async def _update_books(self):
        replace_dict = {
            "ru-": "ru",
            "ru~": "ru"
        }

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
                row[5] == '1'
            ]

        await self.log("Update books...")

        async with self.mysql_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    "SELECT BookId, Title, Lang, FileType, Time, Deleted FROM libbook;"
                )

                while (rows := await cursor.fetchmany(32)):
                    await self.postgres_pool.executemany(
                        "INSERT INTO books (source, remote_id, title, lang, file_type, uploaded, is_deleted) "
                        "VALUES ($1, $2, cast($3 as varchar), cast($4 as varchar), cast($5 as varchar), $6, $7) "
                        "ON CONFLICT (source, remote_id) "
                        "DO UPDATE SET title = EXCLUDED.title, lang = EXCLUDED.lang, file_type = EXCLUDED.file_type, "
                        "uploaded = EXCLUDED.uploaded, is_deleted = EXCLUDED.is_deleted;",
                        [prepare_book(row) for row in rows]
                    )
        
        self.books_updated_event.set()

        await self.log("Books updated!")

    async def _update_books_authors(self):
        await self.books_updated_event.wait()
        await self.authors_updated_event.wait()

        await self.log("Update books_authors...")

        async with self.mysql_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    "SELECT BookId, AvtorId FROM libavtor;"
                )

                while (rows := await cursor.fetchmany(32)):
                    await self.postgres_pool.executemany(
                        "INSERT INTO book_authors (book, author) "
                        "SELECT "
                        "(SELECT id FROM books WHERE source = $1 AND remote_id = $2), "
                        "(SELECT id FROM authors WHERE source = $1 AND remote_id = $3) "
                        "ON CONFLICT (book, author) DO NOTHING;",
                        [(self.SOURCE, *row) for row in rows]
                    )

        await self.log("Books_authors updated!")

    async def _update_translations(self):
        await self.books_updated_event.wait()
        await self.authors_updated_event.wait()

        await self.log("Update translations...")

        async with self.mysql_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    "SELECT BookId, TranslatorId, Pos FROM libtranslator "
                    "WHERE BookId IN (SELECT BookId FROM libbook);"
                )

                while (rows := await cursor.fetchmany(32)):
                    await self.postgres_pool.executemany(
                        "INSERT INTO translations (book, author, position) "
                        "SELECT "
                        "(SELECT id FROM books WHERE source = $1 AND remote_id = $2), "
                        "(SELECT id FROM authors WHERE source = $1 AND remote_id = $3), "
                        "$4 "
                        "ON CONFLICT (book, author) "
                        "DO UPDATE SET position = EXCLUDED.position;",
                        [(self.SOURCE, *row) for row in rows]
                    )

        await self.log("Translations updated!")

    async def _update_sequences(self):
        def prepare_sequence(row: list):
            return [
                self.SOURCE,
                row[0],
                remove_wrong_ch(row[1]),
            ]

        await self.log("Update sequences...")

        async with self.mysql_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    "SELECT SeqId, SeqName FROM libseqname;"
                )

                while (rows := await cursor.fetchmany(32)):
                    await self.postgres_pool.executemany(
                        "INSERT INTO sequences (source, remote_id, name) "
                        "VALUES ($1, $2, cast($3 as varchar)) "
                        "ON CONFLICT (source, remote_id) "
                        "DO UPDATE SET name = EXCLUDED.name;",
                        [prepare_sequence(row) for row in rows]
                    )

        self.sequences_updated_event.set()

        await self.log("Sequences updated!")

    async def _update_sequences_info(self):
        await self.sequences_updated_event.wait()
        await self.books_updated_event.wait()

        await self.log("Update book_sequences...")

        async with self.mysql_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    "SELECT BookId, SeqId, level FROM libseq "
                    "WHERE "
                    "BookId IN (SELECT BookId FROM libbook) AND "
                    "SeqId IN (SELECT SeqId FROM libseqname);"
                )

                while (rows := await cursor.fetchmany(32)):
                    await self.postgres_pool.executemany(
                        "INSERT INTO book_sequences (book, sequence, position) "
                        "SELECT "
                        "(SELECT id FROM books WHERE source = $1 AND remote_id = $2), "
                        "(SELECT id FROM sequences WHERE source = $1 AND remote_id = $3), "
                        "$4 "
                        "ON CONFLICT (book, sequence) "
                        "DO UPDATE SET position = EXCLUDED.position;",
                        [[self.SOURCE, *row] for row in rows]
                    )

        await self.log("Book_sequences updated!")

    async def _update_book_annotations(self):
        await self.books_updated_event.wait()

        await self.log("Update book_annotations...")

        async with self.mysql_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    "SELECT BookId, Title, Body FROM libbannotations "
                    "WHERE BookId IN (SELECT BookId FROM libbook);"
                )

                while (rows := await cursor.fetchmany(32)):
                    await self.postgres_pool.executemany(
                        "INSERT INTO book_annotations (book, title, text) "
                        "SELECT "
                        "(SELECT id FROM books WHERE source = $1 AND remote_id = $2), "
                        "$3, $4 "
                        "ON CONFLICT (book) "
                        "DO UPDATE SET title = EXCLUDED.title, text = EXCLUDED.text;",
                        [[self.SOURCE, *row] for row in rows]
                    )

        await self.log("Book_annotations updated!")

        await self._update_book_annotations_pic()

    async def _update_book_annotations_pic(self):
        await self.log("Update book_annotations_pic...")

        async with self.mysql_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    "SELECT BookId, File FROM libbpics;"
                )

                while (rows := await cursor.fetchmany(32)):
                    await self.postgres_pool.executemany(
                        "UPDATE book_annotations "
                        "SET file = cast($3 as varchar) "
                        "FROM (SELECT id FROM books WHERE source = $1 AND remote_id = $2) as books "
                        "WHERE book = books.id;",
                        [[self.SOURCE, *row] for row in rows]
                    )

        await self.log("Book_annotation_pics updated!")

    async def _update_author_annotations(self):
        await self.authors_updated_event.wait()

        await self.log("Update author_annotations...")

        async with self.mysql_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    "SELECT AvtorId, Title, Body FROM libaannotations;"
                )

                while (rows := await cursor.fetchmany(32)):
                    await self.postgres_pool.executemany(
                        "INSERT INTO author_annotations (author, title, text) "
                        "SELECT "
                        "(SELECT id FROM authors WHERE source = $1 AND remote_id = $2), "
                        "$3, $4 "
                        "ON CONFLICT (author) "
                        "DO UPDATE SET title = EXCLUDED.title, text = EXCLUDED.text;",
                        [[self.SOURCE, *row] for row in rows]
                    )

        await self.log("Author_annotation_updated!")

        await self._update_author_annotations_pics()

    async def _update_author_annotations_pics(self):
        await self.log("Update author_annotations_pic...")

        async with self.mysql_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    "SELECT AvtorId, File FROM libapics;"
                )

                while (rows := await cursor.fetchmany(32)):
                    await self.postgres_pool.executemany(
                        "UPDATE author_annotations "
                        "SET file = cast($3 as varchar) "
                        "FROM (SELECT id FROM authors WHERE source = $1 AND remote_id = $2) as authors "
                        "WHERE author = authors.id;",
                        [[self.SOURCE, *row] for row in rows]
                    )

        await self.log("Author_annotatioins_pic updated!")

    async def _update_genres(self):
        await self.log("Update genres...")

        async with self.mysql_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    "SELECT GenreId, GenreCode, GenreDesc, GenreMeta FROM libgenrelist;"
                )

                while (rows := await cursor.fetchmany(32)):
                    await self.postgres_pool.executemany(
                        "INSERT INTO genres (source, remote_id, code, description, meta) "
                        "VALUES ($1, $2, cast($3 as varchar), cast($4 as varchar), cast($5 as varchar)) "
                        "ON CONFLICT (source, remote_id) "
                        "DO UPDATE SET code = EXCLUDED.code, description = EXCLUDED.description, meta = EXCLUDED.meta;",
                        [[self.SOURCE, *row] for row in rows]
                    )

        await self.log("Genres updated!")

    async def _update_books_genres(self):
        await self.books_updated_event.wait()
        await self.genres_updated_event.wait()

        await self.log("Update book_genres...")

        async with self.mysql_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    "SELECT BookId, GenreId FROM libgenre;"
                )

                while (rows := await cursor.fetchmany(32)):
                    await self.postgres_pool.executemany(
                        "INSERT INTO book_genres (book, genre) "
                        "SELECT "
                        "(SELECT id FROM books WHERE source = $1 AND remote_id = $2), "
                        "(SELECT id FROM genres WHERE source = $1 AND remote_id = $3) "
                        "ON CONFLICT (book, author) DO NOTHING;",
                        [(self.SOURCE, *row) for row in rows]
                    )

        await self.log("Book_genres updated!")

    async def _update(self) -> bool:
        self.platform = platform.platform()
        self.logger = Logger.with_default_handlers()

        await self._prepare()

        await self._drop_tables()

        await asyncio.gather(
            *[self._import_dump(filename) for filename in self.FILES]
        )

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
            self._update_books_genres()
        )

        await WebhookSender.send()

        return True

    @classmethod
    async def update(cls) -> bool:
        updater = cls()
        return await updater._update()
