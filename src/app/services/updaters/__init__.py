from enum import Enum


class UpdaterTypes(Enum):
    FL = "fl"


UPDATERS: dict[UpdaterTypes, str] = {UpdaterTypes.FL: "run_fl_update"}
