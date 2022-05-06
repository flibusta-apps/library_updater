from enum import Enum
import logging
import time
from typing import Optional

import aiomysql
from arq.connections import ArqRedis
from arq.worker import Retry
import asyncpg
import httpx

from app.services.updaters.utils.cmd import run_cmd
from app.services.updaters.utils.tasks import is_jobs_complete
from app.services.updaters.utils.text import remove_wrong_ch, fix_annotation_text
from core.config import env_config


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

logger.addHandler(ch)


class JobId(Enum):
    import_libavtor = "import_libavtor"
    import_libbook = "import_libbook"
    import_libavtorname = "import_libavtorname"
    import_libtranslator = "import_libtranslator"
    import_libseqname = "import_libseqname"
    import_libseq = "import_libseq"
    import_libgenre = "import_libgenre"
    import_libgenrelist = "import_libgenrelsit"
    import_lib_b_annotations = "import_lib_b_annotations"
    import_lib_b_annotations_pics = "import_lib_b_annotations_pics"
    import_lib_a_annotations = "import_lib_a_annotations"
    import_lib_a_annotations_pics = "import_lib_a_annotations_pics"

    update_authors = "update_fl_authors"
    update_books = "update_fl_books"
    update_books_authors = "update_fl_books_authors"
    update_translations = "update_fl_translations"
    update_sequences = "update_fl_sequences"
    update_sequences_info = "update_fl_sequences_info"
    update_book_annotations = "update_fl_book_annotations"
    update_book_annotations_pic = "update_fl_book_annotations_pic"
    update_author_annotations = "update_fl_author_annotations"
    update_author_annotations_pics = "update_fl_author_annotations_pics"
    update_genres = "update_fl_genres"
    update_books_genres = "update_fl_books_genres"

    webhook = "update_fl_webhook"


async def import_fl_dump(ctx: dict, filename: str, *args, **kwargs):
    stdout, stderr, return_code = await run_cmd(
        f"wget -O - {env_config.FL_BASE_URL}/sql/{filename}.gz | gunzip | "
        f"mysql -h {env_config.MYSQL_HOST} -u {env_config.MYSQL_USER} "
        f'-p"{env_config.MYSQL_PASSWORD}" {env_config.MYSQL_DB_NAME}'
    )

    if return_code != 0:
        raise InterruptedError(stdout, stderr)


async def get_db_cons() -> tuple[asyncpg.Connection, aiomysql.Connection]:
    postgres = await asyncpg.connect(
        database=env_config.POSTGRES_DB_NAME,
        host=env_config.POSTGRES_HOST,
        port=env_config.POSTGRES_PORT,
        user=env_config.POSTGRES_USER,
        password=env_config.POSTGRES_PASSWORD,
    )

    mysql = await aiomysql.connect(
        db=env_config.MYSQL_DB_NAME,
        host=env_config.MYSQL_HOST,
        port=env_config.MYSQL_PORT,
        user=env_config.MYSQL_USER,
        password=env_config.MYSQL_PASSWORD,
    )

    assert postgres

    return postgres, mysql


async def get_source(postgres: asyncpg.Connection) -> int:
    source_row = await postgres.fetchrow(
        "SELECT id FROM sources WHERE name = 'flibusta';"
    )

    if not source_row:
        await postgres.execute("INSERT INTO sources (name) VALUES ('flibusta');")

        source_row = await postgres.fetchrow(
            "SELECT id FROM sources WHERE name = 'flibusta';"
        )

    assert source_row

    return source_row["id"]


async def update_fl_authors(ctx: dict, *args, prefix: Optional[str] = None, **kwargs):
    arq_pool: ArqRedis = ctx["arq_pool"]

    is_deps_complete, not_complete_count = await is_jobs_complete(
        arq_pool, [JobId.import_libavtorname.value], prefix=prefix
    )

    if not is_deps_complete:
        raise Retry(defer=60 * not_complete_count)

    postgres, mysql = await get_db_cons()

    source = await get_source(postgres)

    def prepare_author(row: list):
        return [
            source,
            row[0],
            remove_wrong_ch(row[1]),
            remove_wrong_ch(row[2]),
            remove_wrong_ch(row[3]),
        ]

    await postgres.execute(
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

    async with mysql.cursor(aiomysql.SSCursor) as cursor:
        await cursor.execute(
            "SELECT AvtorId, FirstName, LastName, MiddleName FROM libavtorname;"
        )

        while rows := await cursor.fetchmany(4096):
            await postgres.executemany(
                "SELECT update_author($1, $2, cast($3 as varchar), cast($4 as varchar), cast($5 as varchar));",
                (prepare_author(row) for row in rows),
            )

    await postgres.close()
    mysql.close()


async def update_fl_books(ctx: dict, *args, prefix: Optional[str] = None, **kwargs):
    arq_pool: ArqRedis = ctx["arq_pool"]

    is_deps_complete, not_complete_count = await is_jobs_complete(
        arq_pool, [JobId.import_libbook.value], prefix=prefix
    )

    if not is_deps_complete:
        raise Retry(defer=60 * not_complete_count)

    postgres, mysql = await get_db_cons()

    source = await get_source(postgres)

    replace_dict = {"ru-": "ru", "ru~": "ru"}

    def fix_lang(lang: str) -> str:
        lower_lang = lang.lower()
        replaced_lang = replace_dict.get(lower_lang, lower_lang)
        return replaced_lang

    def prepare_book(row: list):
        return [
            source,
            row[0],
            remove_wrong_ch(row[1]),
            fix_lang(row[2]),
            row[3],
            row[4],
            row[5] == "1",
        ]

    await postgres.execute(
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

    async with mysql.cursor(aiomysql.SSCursor) as cursor:
        await cursor.execute(
            "SELECT BookId, Title, Lang, FileType, Time, Deleted FROM libbook;"
        )

        while rows := await cursor.fetchmany(1024):
            await postgres.executemany(
                "SELECT update_book($1, $2, cast($3 as varchar), cast($4 as varchar), cast($5 as varchar), $6, $7);",
                (prepare_book(row) for row in rows),
            )

    await postgres.close()
    mysql.close()


async def update_fl_books_authors(
    ctx: dict, *arsg, prefix: Optional[str] = None, **kwargs
):
    arq_pool: ArqRedis = ctx["arq_pool"]

    is_deps_complete, not_complete_count = await is_jobs_complete(
        arq_pool,
        [
            JobId.import_libavtor.value,
            JobId.update_authors.value,
            JobId.update_books.value,
        ],
        prefix=prefix,
    )

    if not is_deps_complete:
        raise Retry(defer=60 * not_complete_count)

    postgres, mysql = await get_db_cons()

    source = await get_source(postgres)

    await postgres.execute(
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

    async with mysql.cursor(aiomysql.SSCursor) as cursor:
        await cursor.execute("SELECT BookId, AvtorId FROM libavtor;")

        while rows := await cursor.fetchmany(4096):
            await postgres.executemany(
                "SELECT update_book_author($1, $2, $3);",
                ((source, *row) for row in rows),
            )

    await postgres.close()
    mysql.close()


async def update_fl_translations(
    ctx: dict, *args, prefix: Optional[str] = None, **kwargs
):
    arq_pool: ArqRedis = ctx["arq_pool"]

    is_deps_complete, not_complete_count = await is_jobs_complete(
        arq_pool,
        [
            JobId.import_libtranslator.value,
            JobId.update_authors.value,
            JobId.update_books.value,
        ],
        prefix=prefix,
    )

    if not is_deps_complete:
        raise Retry(defer=60 * not_complete_count)

    postgres, mysql = await get_db_cons()

    source = await get_source(postgres)

    await postgres.execute(
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

    async with mysql.cursor(aiomysql.SSCursor) as cursor:
        await cursor.execute(
            "SELECT BookId, TranslatorId, Pos FROM libtranslator "
            "WHERE BookId IN (SELECT BookId FROM libbook);"
        )

        while rows := await cursor.fetchmany(4096):
            await postgres.executemany(
                "SELECT update_translation($1, $2, $3, $4)",
                ((source, *row) for row in rows),
            )

    await postgres.close()
    mysql.close()


async def update_fl_sequences(ctx: dict, *args, prefix: Optional[str] = None, **kwargs):
    arq_pool: ArqRedis = ctx["arq_pool"]

    is_deps_complete, not_complete_count = await is_jobs_complete(
        arq_pool,
        [
            JobId.import_libseqname.value,
        ],
        prefix=prefix,
    )

    if not is_deps_complete:
        raise Retry(defer=60 * not_complete_count)

    postgres, mysql = await get_db_cons()

    source = await get_source(postgres)

    def prepare_sequence(row: list):
        return [
            source,
            row[0],
            remove_wrong_ch(row[1]),
        ]

    await postgres.execute(
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

    async with mysql.cursor(aiomysql.SSCursor) as cursor:
        await cursor.execute("SELECT SeqId, SeqName FROM libseqname;")

        while rows := await cursor.fetchmany(4096):
            await postgres.executemany(
                "SELECT update_sequences($1, $2, cast($3 as varchar));",
                (prepare_sequence(row) for row in rows),
            )

    await postgres.close()
    mysql.close()


async def update_fl_sequences_info(
    ctx: dict, *args, prefix: Optional[str] = None, **kwargs
):
    arq_pool: ArqRedis = ctx["arq_pool"]

    is_deps_complete, not_complete_count = await is_jobs_complete(
        arq_pool,
        [
            JobId.import_libseq.value,
            JobId.update_sequences.value,
            JobId.update_books.value,
        ],
        prefix=prefix,
    )

    if not is_deps_complete:
        raise Retry(defer=60 * not_complete_count)

    postgres, mysql = await get_db_cons()

    source = await get_source(postgres)

    await postgres.execute(
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

    async with mysql.cursor(aiomysql.SSCursor) as cursor:
        await cursor.execute(
            "SELECT BookId, SeqId, level FROM libseq "
            "WHERE "
            "BookId IN (SELECT BookId FROM libbook) AND "
            "SeqId IN (SELECT SeqId FROM libseqname);"
        )

        while rows := await cursor.fetchmany(4096):
            await postgres.executemany(
                "SELECT update_book_sequence($1, $2, $3, $4);",
                ([source, *row] for row in rows),
            )

    await postgres.close()
    mysql.close()


async def update_fl_book_annotations(
    ctx: dict, *args, prefix: Optional[str] = None, **kwargs
):
    arq_pool: ArqRedis = ctx["arq_pool"]

    is_deps_complete, not_complete_count = await is_jobs_complete(
        arq_pool,
        [
            JobId.import_lib_b_annotations.value,
            JobId.update_books.value,
        ],
        prefix=prefix,
    )

    if not is_deps_complete:
        raise Retry(defer=60 * not_complete_count)

    postgres, mysql = await get_db_cons()

    source = await get_source(postgres)

    await postgres.execute(
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

    def fix_annotation(row) -> list:
        return [
            source,
            row[0],
            row[1],
            fix_annotation_text(row[2]),
        ]

    async with mysql.cursor(aiomysql.SSCursor) as cursor:
        await cursor.execute(
            "SELECT BookId, Title, Body FROM libbannotations "
            "WHERE BookId IN (SELECT BookId FROM libbook);"
        )

        while rows := await cursor.fetchmany(4096):
            await postgres.executemany(
                "SELECT update_book_annotation($1, $2, cast($3 as varchar), cast($4 as text));",
                (fix_annotation(row) for row in rows),
            )

    await postgres.close()
    mysql.close()


async def update_fl_book_annotations_pic(
    ctx: dict, *args, prefix: Optional[str] = None, **kwargs
):
    arq_pool: ArqRedis = ctx["arq_pool"]

    is_deps_complete, not_complete_count = await is_jobs_complete(
        arq_pool,
        [
            JobId.import_lib_b_annotations_pics.value,
            JobId.update_book_annotations.value,
        ],
        prefix=prefix,
    )

    if not is_deps_complete:
        raise Retry(defer=60 * not_complete_count)

    postgres, mysql = await get_db_cons()

    source = await get_source(postgres)

    def fix_link(row):
        return [source, row[0], f"{env_config.FL_BASE_URL}/i/{row[1]}"]

    async with mysql.cursor(aiomysql.SSCursor) as cursor:
        await cursor.execute("SELECT BookId, File FROM libbpics;")

        while rows := await cursor.fetchmany(4096):
            await postgres.executemany(
                "UPDATE book_annotations "
                "SET file = cast($3 as varchar) "
                "FROM (SELECT id FROM books WHERE source = $1 AND remote_id = $2) as books "
                "WHERE book = books.id;",
                (fix_link(row) for row in rows),
            )

    await postgres.close()
    mysql.close()


async def update_fl_author_annotations(
    ctx: dict, *args, prefix: Optional[str] = None, **kwargs
):
    arq_pool: ArqRedis = ctx["arq_pool"]

    is_deps_complete, not_complete_count = await is_jobs_complete(
        arq_pool,
        [
            JobId.import_lib_a_annotations.value,
            JobId.update_authors.value,
        ],
        prefix=prefix,
    )

    if not is_deps_complete:
        raise Retry(defer=60 * not_complete_count)

    postgres, mysql = await get_db_cons()

    source = await get_source(postgres)

    await postgres.execute(
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

    def fix_annotation(row) -> list:
        return [
            source,
            row[0],
            row[1],
            fix_annotation_text(row[2]),
        ]

    async with mysql.cursor(aiomysql.SSCursor) as cursor:
        await cursor.execute("SELECT AvtorId, Title, Body FROM libaannotations;")

        while rows := await cursor.fetchmany(4096):
            await postgres.executemany(
                "SELECT update_author_annotation($1, $2, cast($3 as varchar), cast($4 as text));",
                (fix_annotation(row) for row in rows),
            )

    await postgres.close()
    mysql.close()


async def update_fl_author_annotations_pics(
    ctx: dict, *args, prefix: Optional[str] = None, **kwargs
):
    arq_pool: ArqRedis = ctx["arq_pool"]

    if not await is_jobs_complete(
        arq_pool,
        [
            JobId.import_lib_a_annotations_pics.value,
            JobId.update_author_annotations.value,
        ],
        prefix=prefix,
    ):
        raise Retry(defer=60)

    postgres, mysql = await get_db_cons()

    source = await get_source(postgres)

    def fix_link(row):
        return [source, row[0], f"{env_config.FL_BASE_URL}/ia/{row[1]}"]

    async with mysql.cursor(aiomysql.SSCursor) as cursor:
        await cursor.execute("SELECT AvtorId, File FROM libapics;")

        while rows := await cursor.fetchmany(4096):
            await postgres.executemany(
                "UPDATE author_annotations "
                "SET file = cast($3 as varchar) "
                "FROM (SELECT id FROM authors WHERE source = $1 AND remote_id = $2) as authors "
                "WHERE author = authors.id;",
                (fix_link(row) for row in rows),
            )

    await postgres.close()
    mysql.close()


async def update_fl_genres(ctx: dict, *args, prefix: Optional[str] = None, **kwargs):
    arq_pool: ArqRedis = ctx["arq_pool"]

    is_deps_complete, not_complete_count = await is_jobs_complete(
        arq_pool,
        [
            JobId.import_libgenrelist.value,
        ],
        prefix=prefix,
    )

    if not is_deps_complete:
        raise Retry(defer=60 * not_complete_count)

    postgres, mysql = await get_db_cons()

    source = await get_source(postgres)

    await postgres.execute(
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

    async with mysql.cursor(aiomysql.SSCursor) as cursor:
        await cursor.execute(
            "SELECT GenreId, GenreCode, GenreDesc, GenreMeta FROM libgenrelist;"
        )

        while rows := await cursor.fetchmany(4096):
            await postgres.executemany(
                "SELECT update_genre($1, $2, cast($3 as varchar), cast($4 as varchar), cast($5 as varchar));",
                ([source, *row] for row in rows),
            )

    await postgres.close()
    mysql.close()


async def update_fl_books_genres(
    ctx: dict, *args, prefix: Optional[str] = None, **kwargs
):
    arq_pool: ArqRedis = ctx["arq_pool"]

    is_deps_complete, not_complete_count = await is_jobs_complete(
        arq_pool,
        [
            JobId.import_libgenre.value,
            JobId.update_books.value,
            JobId.update_genres.value,
        ],
        prefix=prefix,
    )

    if not is_deps_complete:
        raise Retry(defer=60 * not_complete_count)

    postgres, mysql = await get_db_cons()

    source = await get_source(postgres)

    await postgres.execute(
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

    async with mysql.cursor(aiomysql.SSCursor) as cursor:
        await cursor.execute("SELECT BookId, GenreId FROM libgenre;")

        while rows := await cursor.fetchmany(4096):
            await postgres.executemany(
                "SELECT update_book_sequence($1, $2, $3);",
                ((source, *row) for row in rows),
            )

    await postgres.close()
    mysql.close()


async def update_fl_webhook(
    ctx: dict,
    *args,
    prefix: Optional[str] = None,
    **kwargs,
):
    arq_pool: ArqRedis = ctx["arq_pool"]

    is_deps_complete, not_complete_count = await is_jobs_complete(
        arq_pool, [e.value for e in JobId if e != JobId.webhook], prefix=prefix
    )

    if not is_deps_complete:
        raise Retry(defer=60 * not_complete_count)

    all_success = True

    for webhook in env_config.WEBHOOKS:
        async with httpx.AsyncClient() as client:
            response: httpx.Response = await getattr(client, webhook.method)(
                webhook.url, headers=webhook.headers
            )

            if response.status_code != 200:
                all_success = False

    return all_success


async def run_fl_update(ctx: dict, *args, **kwargs) -> bool:
    IMPORTS = {
        JobId.import_libbook: "lib.libbook.sql",
        JobId.import_libavtor: "lib.libavtor.sql",
        JobId.import_libavtorname: "lib.libavtorname.sql",
        JobId.import_libtranslator: "lib.libtranslator.sql",
        JobId.import_libseqname: "lib.libseqname.sql",
        JobId.import_libseq: "lib.libseq.sql",
        JobId.import_libgenre: "lib.libgenre.sql",
        JobId.import_libgenrelist: "lib.libgenrelist.sql",
        JobId.import_lib_b_annotations: "lib.b.annotations.sql",
        JobId.import_lib_b_annotations_pics: "lib.b.annotations_pics.sql",
        JobId.import_lib_a_annotations: "lib.a.annotations.sql",
        JobId.import_lib_a_annotations_pics: "lib.a.annotations_pics.sql",
    }

    UPDATES = (
        JobId.update_books,
        JobId.update_book_annotations,
        JobId.update_book_annotations_pic,
        JobId.update_books_genres,
        JobId.update_authors,
        JobId.update_author_annotations,
        JobId.update_author_annotations_pics,
        JobId.update_books_authors,
        JobId.update_translations,
        JobId.update_sequences,
        JobId.update_sequences_info,
        JobId.update_genres,
        JobId.webhook,
    )

    arq_pool: ArqRedis = ctx["arq_pool"]
    prefix = str(int(time.time()) // (5 * 60))

    for job_id, filename in IMPORTS.items():
        await arq_pool.enqueue_job(
            "import_fl_dump", filename, _job_id=f"{prefix}_{job_id.value}"
        )

    for job_id in UPDATES:
        await arq_pool.enqueue_job(
            job_id.value, prefix=prefix, _job_id=f"{prefix}_{job_id.value}"
        )

    return True


__tasks__ = [
    run_fl_update,
    import_fl_dump,
    update_fl_authors,
    update_fl_books,
    update_fl_books_authors,
    update_fl_translations,
    update_fl_sequences,
    update_fl_sequences_info,
    update_fl_book_annotations,
    update_fl_book_annotations_pic,
    update_fl_author_annotations,
    update_fl_author_annotations_pics,
    update_fl_genres,
    update_fl_books_genres,
    update_fl_webhook,
]
