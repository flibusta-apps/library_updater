from fastapi import APIRouter, BackgroundTasks, Depends

from app.depends import check_token
from app.services.updaters import UpdaterTypes, UPDATERS


router = APIRouter(tags=["updater"], dependencies=[Depends(check_token)])


@router.post("/update/{updater}")
async def update(updater: UpdaterTypes, background_tasks: BackgroundTasks):
    updater_ = UPDATERS[updater]

    background_tasks.add_task(updater_.update)

    return True
