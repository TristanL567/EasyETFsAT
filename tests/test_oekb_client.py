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

