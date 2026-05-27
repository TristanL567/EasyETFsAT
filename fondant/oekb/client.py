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
        reports: list[OeKBReportListItem] = []
        next_offset: int | None = offset

        while next_offset is not None:
            payload = await self._get(
                "/steuerMeldung/liste",
                params={
                    "offset": next_offset,
                    "limit": limit,
                    "ctxListArt": ctx_list_art,
                    "ctxEqIsin": isin,
                    "meldgNurGuelt": str(meldg_nur_guelt).lower(),
                    "meldgJahresM": str(meldg_jahres_m).lower(),
                    "sortField": sort_field,
                    "sortOrder": sort_order,
                },
            )
            page = _extract_list_page(payload)
            reports.extend(OeKBReportListItem.model_validate(item) for item in page.items)
            next_offset = page.next_offset(current_offset=next_offset, requested_limit=limit, collected=len(reports))

        return reports

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


class _ReportListPage:
    def __init__(
        self,
        *,
        items: list[dict[str, Any]],
        total_elements: int | None = None,
        total_pages: int | None = None,
        page_number: int | None = None,
        page_size: int | None = None,
    ) -> None:
        self.items = items
        self.total_elements = total_elements
        self.total_pages = total_pages
        self.page_number = page_number
        self.page_size = page_size

    def next_offset(self, *, current_offset: int, requested_limit: int, collected: int) -> int | None:
        if not self.items:
            return None

        if self.total_elements is not None:
            return current_offset + requested_limit if collected < self.total_elements else None

        if self.total_pages is not None and self.page_number is not None:
            next_page_number = self.page_number + 1
            return current_offset + requested_limit if next_page_number < self.total_pages else None

        if len(self.items) < requested_limit:
            return None

        return None


def _extract_list_page(payload: Any) -> _ReportListPage:
    if isinstance(payload, list):
        return _ReportListPage(items=[item for item in payload if isinstance(item, dict)])

    if isinstance(payload, dict):
        items = _extract_list_payload(payload)
        return _ReportListPage(
            items=items,
            total_elements=_int_or_none(payload.get("totalElements")),
            total_pages=_int_or_none(payload.get("totalPages")),
            page_number=_int_or_none(payload.get("number")),
            page_size=_int_or_none(payload.get("size")),
        )

    return _ReportListPage(items=[])


def _int_or_none(value: Any) -> int | None:
    return value if isinstance(value, int) else None


def _extract_list_payload(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]

    if isinstance(payload, dict):
        for key in ("items", "content", "steuerMeldungen", "steuerMeldungListe", "list"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]

    return []
