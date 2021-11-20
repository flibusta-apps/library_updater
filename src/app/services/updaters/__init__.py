from enum import Enum

from app.services.updaters.base import BaseUpdater
from app.services.updaters.fl_updater import FlUpdater


class UpdaterTypes(Enum):
    FL = 'fl'


UPDATERS: dict[UpdaterTypes, BaseUpdater] = {
    UpdaterTypes.FL: FlUpdater
}
