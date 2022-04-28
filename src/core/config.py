from typing import Union, Literal

from pydantic import BaseModel, BaseSettings


class WebhookConfig(BaseModel):
    method: Union[Literal["get"], Literal["post"]]
    url: str
    headers: dict[str, str]


class EnvConfig(BaseSettings):
    API_KEY: str

    POSTGRES_DB_NAME: str
    POSTGRES_HOST: str
    POSTGRES_PORT: int
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str

    MYSQL_DB_NAME: str
    MYSQL_HOST: str
    MYSQL_PORT: int
    MYSQL_USER: str
    MYSQL_PASSWORD: str

    REDIS_HOST: str
    REDIS_PORT: int
    REDIS_DB: int

    FL_BASE_URL: str

    SENTRY_DSN: str

    WEBHOOKS: list[WebhookConfig]


env_config = EnvConfig()
