from arq.connections import ArqRedis
from arq.cron import cron

from app.services.updaters.fl_updater import __tasks__ as fl_tasks
from app.services.updaters.fl_updater import run_fl_update
from core.arq_pool import get_redis_settings, get_arq_pool
import core.sentry  # noqa: F401


async def startup(ctx):
    ctx["arq_pool"] = await get_arq_pool()


async def shutdown(ctx):
    arq_pool: ArqRedis = ctx["arq_pool"]

    arq_pool.close()
    await arq_pool.wait_closed()


class WorkerSettings:
    functions = [*fl_tasks]
    on_startup = startup
    on_shutdown = shutdown
    redis_settings = get_redis_settings()
    max_jobs = 2
    max_tries = 20
    job_timeout = 5 * 60
    cron_jobs = [cron(run_fl_update, hour={5}, minute=0)]
