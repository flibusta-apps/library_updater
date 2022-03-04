from app.services.updaters.fl_updater import run_fl_update, run_fl_update2
from core.arq_pool import get_redis_settings, get_arq_pool


async def startup(ctx):
    ctx["arq_pool"] = await get_arq_pool()


class WorkerSettings:
    functions = [run_fl_update, run_fl_update2]
    on_startup = startup
    redis_settings = get_redis_settings()
    max_jobs = 1
    job_timeout = 30 * 60
