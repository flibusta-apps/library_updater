from fastapi import APIRouter, BackgroundTasks, Depends

from app.services.updaters import UpdaterTypes, UPDATERS
from app.depends import check_token


router = APIRouter(
    tags=["updater"],
    dependencies=[Depends(check_token)]
)


@router.post("/update/{updater}")
async def update(updater: UpdaterTypes, background_tasks: BackgroundTasks):
    updater_ = UPDATERS[updater]

    background_tasks.add_task(
        updater_.update
    )

    return True
