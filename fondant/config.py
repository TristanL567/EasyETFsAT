from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: str = "postgresql+asyncpg://easyetfsat:easyetfsat@localhost:5432/easyetfsat"
    oekb_base_url: str = "https://my.oekb.at/fond-info/rest/public"
    ecb_base_url: str = "https://data-api.ecb.europa.eu/service"
    log_level: str = "INFO"
    oekb_rate_limit_per_second: float = 4.0
    oekb_timeout_seconds: float = 30.0
    ecb_rate_limit_per_second: float = 4.0
    ecb_timeout_seconds: float = 30.0

    @property
    def alembic_database_url(self) -> str:
        return (
            self.database_url.replace("+asyncpg", "+psycopg")
            .replace("+aiosqlite", "+pysqlite")
        )


@lru_cache
def get_settings() -> Settings:
    return Settings()
