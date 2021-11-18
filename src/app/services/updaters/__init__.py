from enum import Enum

from app.services.updaters.base import BaseUpdater
from app.services.updaters.flibusta_updater import FlibustaUpdater


class UpdaterTypes(Enum):
    FLIBUSTA = 'flibusta'


UPDATERS: dict[UpdaterTypes, BaseUpdater] = {
    UpdaterTypes.FLIBUSTA: FlibustaUpdater
}
