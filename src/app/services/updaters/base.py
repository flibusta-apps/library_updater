from typing import Protocol


class BaseUpdater(Protocol):
    @classmethod
    async def update(cls) -> bool:
        ...
