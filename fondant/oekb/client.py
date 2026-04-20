from __future__ import annotations

import asyncio
from collections.abc import Mapping
from typing import Any

import httpx

from fondant.config import get_settings
from fondant.oekb.models import OeKBReportDetailResponse, OeKBReportListItem


class OeKBClient:
    def __init__(self, *, client: httpx.AsyncClient | None = None) -> None:
        self._settings = get_settings()
        self._client = client
        self._owns_client = client is None
        self._lock = asyncio.Lock()
        self._last_call = 0.0

    async def __aenter__(self) -> OeKBClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self._settings.oekb_base_url,
                timeout=self._settings.oekb_timeout_seconds,
                headers=self._default_headers(),
            )
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None

    def _default_headers(self) -> dict[str, str]:
        return {
            "Accept": "application/json",
            "Accept-Language": "de",
            "OeKB-Platform-Context": "=",
        }

    async def _rate_limit(self) -> None:
        if self._settings.oekb_rate_limit_per_second <= 0:
            return
        min_interval = 1.0 / self._settings.oekb_rate_limit_per_second
        loop = asyncio.get_running_loop()
        async with self._lock:
            now = loop.time()
            elapsed = now - self._last_call
            if elapsed < min_interval:
                await asyncio.sleep(min_interval - elapsed)
            self._last_call = loop.time()

    async def _get(self, path: str, *, params: Mapping[str, Any] | None = None) -> Any:
        if self._client is None:
            raise RuntimeError("OeKBClient must be entered via 'async with'.")

        await self._rate_limit()
        response = await self._client.get(path, params=params)
        response.raise_for_status()
        return response.json()

    async def get_report_list(
        self,
        isin: str,
        *,
        offset: int = 0,
        limit: int = 50,
        ctx_list_art: str = "ALLE",
        meldg_nur_guelt: bool = True,
        meldg_jahres_m: bool = True,
        sort_field: str = "isinBez",
        sort_order: int = 1,
    ) -> list[OeKBReportListItem]:
        payload = await self._get(
            "/steuerMeldung/liste",
            params={
                "offset": offset,
                "limit": limit,
                "ctxListArt": ctx_list_art,
                "ctxEqIsin": isin,
                "meldgNurGuelt": str(meldg_nur_guelt).lower(),
                "meldgJahresM": str(meldg_jahres_m).lower(),
                "sortField": sort_field,
                "sortOrder": sort_order,
            },
        )
        return [OeKBReportListItem.model_validate(item) for item in _extract_list_payload(payload)]

    async def get_report_detail(self, stm_id: int) -> OeKBReportDetailResponse:
        payload = await self._get(f"/steuerMeldung/stmId/{stm_id}/ertrStBeh")
        if not isinstance(payload, dict):
            payload = {"data": payload}
        return OeKBReportDetailResponse.model_validate(
            {
                "stmId": payload.get("stmId", stm_id),
                "statusCode": payload.get("statusCode"),
                "versionsNr": payload.get("versionsNr"),
                "waehrung": payload.get("waehrung"),
                "payload": payload,
            }
        )


def _extract_list_payload(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]

    if isinstance(payload, dict):
        for key in ("items", "content", "steuerMeldungen", "steuerMeldungListe", "list"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]

    return []
