import httpx
import pytest
import respx

from fondant.config import get_settings
from fondant.oekb.client import OeKBClient


@pytest.mark.asyncio
async def test_get_report_list_uses_expected_params_and_headers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OEKB_BASE_URL", "https://example-oekb.test/fond-info/rest/public")
    get_settings.cache_clear()

    route_url = "https://example-oekb.test/fond-info/rest/public/steuerMeldung/liste"
    with respx.mock(assert_all_called=True) as router:
        route = router.get(route_url).mock(
            return_value=httpx.Response(
                status_code=200,
                json=[
                    {
                        "stmId": 12345,
                        "isin": "IE00BMTX1Y45",
                        "statusCode": "FIN",
                        "versionsNr": 2,
                    }
                ],
            )
        )
        async with OeKBClient() as client:
            result = await client.get_report_list("IE00BMTX1Y45")

    assert len(result) == 1
    assert result[0].stm_id == 12345
    assert result[0].status_code == "FIN"

    request = route.calls[0].request
    assert request.headers["Accept"] == "application/json"
    assert request.headers["Accept-Language"] == "de"
    assert request.headers["OeKB-Platform-Context"] == "="
    assert request.url.params["ctxEqIsin"] == "IE00BMTX1Y45"
    assert request.url.params["meldgNurGuelt"] == "true"
    assert request.url.params["meldgJahresM"] == "true"


@pytest.mark.asyncio
async def test_get_report_list_reads_all_pages(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OEKB_BASE_URL", "https://example-oekb.test/fond-info/rest/public")
    get_settings.cache_clear()

    route_url = "https://example-oekb.test/fond-info/rest/public/steuerMeldung/liste"
    with respx.mock(assert_all_called=True) as router:
        route = router.get(route_url).mock(
            side_effect=[
                httpx.Response(
                    status_code=200,
                    json={
                        "content": [
                            {
                                "stmId": 111,
                                "isin": "IE00BMTX1Y45",
                                "statusCode": "FIN",
                                "versionsNr": 1,
                            }
                        ],
                        "totalElements": 2,
                        "totalPages": 2,
                        "number": 0,
                        "size": 1,
                    },
                ),
                httpx.Response(
                    status_code=200,
                    json={
                        "content": [
                            {
                                "stmId": 222,
                                "isin": "IE00BMTX1Y45",
                                "statusCode": "FIN",
                                "versionsNr": 1,
                            }
                        ],
                        "totalElements": 2,
                        "totalPages": 2,
                        "number": 1,
                        "size": 1,
                    },
                ),
            ]
        )
        async with OeKBClient() as client:
            result = await client.get_report_list("IE00BMTX1Y45", limit=1)

    assert [item.stm_id for item in result] == [111, 222]
    assert len(route.calls) == 2
    assert route.calls[0].request.url.params["offset"] == "0"
    assert route.calls[0].request.url.params["limit"] == "1"
    assert route.calls[1].request.url.params["offset"] == "1"
    assert route.calls[1].request.url.params["limit"] == "1"


@pytest.mark.asyncio
async def test_get_report_list_advances_offset_by_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OEKB_BASE_URL", "https://example-oekb.test/fond-info/rest/public")
    get_settings.cache_clear()

    route_url = "https://example-oekb.test/fond-info/rest/public/steuerMeldung/liste"
    with respx.mock(assert_all_called=True) as router:
        route = router.get(route_url).mock(
            side_effect=[
                httpx.Response(
                    status_code=200,
                    json={
                        "content": [
                            {
                                "stmId": stm_id,
                                "isin": "IE00BMTX1Y45",
                                "statusCode": "FIN",
                                "versionsNr": 1,
                            }
                            for stm_id in range(100, 150)
                        ],
                        "totalElements": 51,
                        "totalPages": 2,
                        "number": 0,
                        "size": 50,
                    },
                ),
                httpx.Response(
                    status_code=200,
                    json={
                        "content": [
                            {
                                "stmId": 222,
                                "isin": "IE00BMTX1Y45",
                                "statusCode": "FIN",
                                "versionsNr": 1,
                            }
                        ],
                        "totalElements": 51,
                        "totalPages": 2,
                        "number": 1,
                        "size": 50,
                    },
                ),
            ]
        )
        async with OeKBClient() as client:
            result = await client.get_report_list("IE00BMTX1Y45", limit=50)

    assert [item.stm_id for item in result] == [*range(100, 150), 222]
    assert len(route.calls) == 2
    assert route.calls[1].request.url.params["offset"] == "50"
    assert route.calls[1].request.url.params["limit"] == "50"


@pytest.mark.asyncio
async def test_get_report_detail_fetches_stmid_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OEKB_BASE_URL", "https://example-oekb.test/fond-info/rest/public")
    get_settings.cache_clear()

    route_url = "https://example-oekb.test/fond-info/rest/public/steuerMeldung/stmId/999/ertrStBeh"
    with respx.mock(assert_all_called=True) as router:
        router.get(route_url).mock(
            return_value=httpx.Response(
                status_code=200,
                json={"stmId": 999, "statusCode": "FIN", "versionsNr": 3, "waehrung": "EUR"},
            )
        )
        async with OeKBClient() as client:
            detail = await client.get_report_detail(999)

    assert detail.stm_id == 999
    assert detail.status_code == "FIN"
    assert detail.versions_nr == 3
    assert detail.waehrung == "EUR"
