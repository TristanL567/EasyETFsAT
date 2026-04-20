from datetime import date
from decimal import Decimal

import httpx
import pytest
import respx

from fondant.config import get_settings
from fondant.ecb.client import ECBClient


@pytest.mark.asyncio
async def test_get_reference_rates_parses_csv(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ECB_BASE_URL", "https://example-ecb.test/service")
    get_settings.cache_clear()

    route_url = (
        "https://example-ecb.test/service/data/EXR/"
        "D.CHF+USD.EUR.SP00.A"
    )

    csv_payload = "\n".join(
        [
            "KEY,FREQ,CURRENCY,CURRENCY_DENOM,EXR_TYPE,EXR_SUFFIX,TIME_PERIOD,OBS_VALUE",
            "EXR.D.CHF.EUR.SP00.A,D,CHF,EUR,SP00,A,2026-04-01,0.9191",
            "EXR.D.USD.EUR.SP00.A,D,USD,EUR,SP00,A,2026-04-01,1.1782",
        ]
    )

    with respx.mock(assert_all_called=True) as router:
        route = router.get(route_url).mock(return_value=httpx.Response(status_code=200, text=csv_payload))
        async with ECBClient() as client:
            points = await client.get_reference_rates(
                currency_codes=["usd", "chf"],
                start_date=date(2026, 4, 1),
                end_date=date(2026, 4, 1),
            )

    assert len(points) == 2
    assert points[0].rate_date == date(2026, 4, 1)
    assert points[0].currency_code == "CHF"
    assert points[0].rate == Decimal("0.9191")
    assert points[1].currency_code == "USD"
    assert points[1].rate == Decimal("1.1782")

    request = route.calls[0].request
    assert request.url.params["startPeriod"] == "2026-04-01"
    assert request.url.params["endPeriod"] == "2026-04-01"
    assert request.url.params["format"] == "csvdata"
