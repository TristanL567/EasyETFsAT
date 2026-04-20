import logging

import structlog
from fastapi import FastAPI

from fondant.api.routes.etf import router as etf_router
from fondant.api.routes.health import router as health_router
from fondant.config import get_settings


def _configure_logging() -> None:
    settings = get_settings()
    logging.basicConfig(level=getattr(logging, settings.log_level.upper(), logging.INFO))
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, settings.log_level.upper(), logging.INFO)
        )
    )


def create_app() -> FastAPI:
    _configure_logging()
    app = FastAPI(title="EasyETFsAT API", version="0.1.0")
    app.include_router(health_router)
    app.include_router(etf_router)
    return app


app = create_app()

