from fastapi import APIRouter, Depends, Request

from arq.connections import ArqRedis

from app.depends import check_token
from app.services.updaters import UpdaterTypes, UPDATERS


router = APIRouter(tags=["updater"], dependencies=[Depends(check_token)])


@router.post("/update/{updater}")
async def update(request: Request, updater: UpdaterTypes):
    arq_pool: ArqRedis = request.app.state.arq_pool
    await arq_pool.enqueue_job(UPDATERS[updater])

    return "Ok!"


@router.get("/healthcheck")
async def healthcheck():
    return "Ok!"
