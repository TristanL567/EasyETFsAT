from __future__ import annotations

import asyncio
import csv
from datetime import date
from decimal import Decimal, InvalidOperation
from io import StringIO

import httpx

from fondant.config import get_settings
from fondant.ecb.models import ECBRatePoint


class ECBClient:
    def __init__(self, *, client: httpx.AsyncClient | None = None) -> None:
        self._settings = get_settings()
        self._client = client
        self._owns_client = client is None
        self._lock = asyncio.Lock()
        self._last_call = 0.0

    async def __aenter__(self) -> ECBClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self._settings.ecb_base_url,
                timeout=self._settings.ecb_timeout_seconds,
                headers={"Accept": "text/csv"},
            )
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _rate_limit(self) -> None:
        if self._settings.ecb_rate_limit_per_second <= 0:
            return
        min_interval = 1.0 / self._settings.ecb_rate_limit_per_second
        loop = asyncio.get_running_loop()
        async with self._lock:
            now = loop.time()
            elapsed = now - self._last_call
            if elapsed < min_interval:
                await asyncio.sleep(min_interval - elapsed)
            self._last_call = loop.time()

    async def _get_csv(self, path: str, *, params: dict[str, str]) -> str:
        if self._client is None:
            raise RuntimeError("ECBClient must be entered via 'async with'.")

        await self._rate_limit()
        response = await self._client.get(path, params=params)
        response.raise_for_status()
        return response.text

    async def get_reference_rates(
        self,
        *,
        currency_codes: list[str],
        start_date: date,
        end_date: date,
    ) -> list[ECBRatePoint]:
        if not currency_codes:
            return []

        currencies = sorted({code.upper() for code in currency_codes if code})
        key = f"D.{'+'.join(currencies)}.EUR.SP00.A"
        csv_text = await self._get_csv(
            f"/data/EXR/{key}",
            params={
                "startPeriod": start_date.isoformat(),
                "endPeriod": end_date.isoformat(),
                "format": "csvdata",
            },
        )
        return _parse_rates_csv(csv_text)


def _parse_rates_csv(csv_text: str) -> list[ECBRatePoint]:
    reader = csv.DictReader(StringIO(csv_text))
    points: list[ECBRatePoint] = []

    for row in reader:
        date_raw = (row.get("TIME_PERIOD") or "").strip()
        ccy_raw = (row.get("CURRENCY") or "").strip().upper()
        value_raw = (row.get("OBS_VALUE") or "").strip()
        if not date_raw or not ccy_raw or not value_raw:
            continue

        try:
            parsed_date = date.fromisoformat(date_raw)
            parsed_rate = Decimal(value_raw)
        except (ValueError, InvalidOperation):
            continue

        points.append(
            ECBRatePoint(
                rate_date=parsed_date,
                currency_code=ccy_raw,
                rate=parsed_rate,
            )
        )

    return points
