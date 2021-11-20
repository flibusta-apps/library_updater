from pydantic import BaseSettings


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

    FL_BASE_URL: str


env_config = EnvConfig()
