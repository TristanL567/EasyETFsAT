from datetime import date
from decimal import Decimal

from pydantic import BaseModel, ConfigDict


class ECBRatePoint(BaseModel):
    model_config = ConfigDict(extra="ignore")

    rate_date: date
    currency_code: str
    rate: Decimal
