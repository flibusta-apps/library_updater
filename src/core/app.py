from fastapi import FastAPI

from app.views import router
from core.arq_pool import get_arq_pool


def start_app() -> FastAPI:
    app = FastAPI()

    app.include_router(router)

    @app.on_event("startup")
    async def startup() -> None:
        app.state.arq_pool = await get_arq_pool()

    return app
