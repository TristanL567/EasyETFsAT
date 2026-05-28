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
    web_auth_username: str = "admin"
    web_auth_password_hash: str = (
        "pbkdf2_sha256$260000$easyetfsat-dev$"
        "9b6e71b435fb984697a06792fa9b553be7249f70d1443b5adcd8a5aaedfa10b1"
    )
    web_session_secret: str = "change-me-for-deployed-web-sessions"
    web_session_cookie_name: str = "easyetfsat_session"
    web_session_max_age_seconds: int = 8 * 60 * 60

    @property
    def alembic_database_url(self) -> str:
        return (
            self.database_url.replace("+asyncpg", "+psycopg")
            .replace("+aiosqlite", "+pysqlite")
        )


@lru_cache
def get_settings() -> Settings:
    return Settings()
