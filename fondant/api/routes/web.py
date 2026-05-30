import base64
import csv
import hashlib
import hmac
import json
import re
import time
from decimal import Decimal, InvalidOperation
from io import StringIO
from pathlib import Path
from typing import Annotated, Any, cast

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession

from fondant.business_query import BusinessQueryInput, BusinessQueryResult, execute_business_query
from fondant.config import Settings, get_settings
from fondant.db.models import INGJOB
from fondant.db.session import get_session
from fondant.search import FundSearchResult, has_available_fund_data, search_available_funds
from fondant.tax_registry import TAX_LINES

TEMPLATE_DIR = Path(__file__).resolve().parents[1] / "templates"

templates = Jinja2Templates(directory=TEMPLATE_DIR)
router = APIRouter(include_in_schema=False)

LEGAL_ENTITY_TYPES = ("natural person", "business", "Stiftung")
ISIN_PATTERN = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")
BUSINESS_QUERY_CSV_HEADERS = (
    "query_name",
    "isin",
    "tax_year",
    "tax_field_code",
    "tax_field_label",
    "legal_entity_category",
    "base_eur_value",
    "amount_multiplier",
    "calculated_eur_value",
)

APP_SECTIONS = {
    "business-query": {
        "label": "BusinessQuery",
        "path": "/app/business-query",
        "title": "BusinessQuery",
        "summary": "Run structured Austrian ETF tax queries for authenticated review.",
    },
    "search": {
        "label": "Search",
        "path": "/app/search",
        "title": "Search",
        "summary": "Discover available fund tax data by ISIN or security name.",
    },
    "update-data": {
        "label": "Update Data",
        "path": "/app/update-data",
        "title": "Update Data",
        "summary": "Prepare future authenticated fund data refresh workflows.",
    },
    "documentation": {
        "label": "Documentation",
        "path": "/app/documentation",
        "title": "Documentation",
        "summary": "Quick reference for authenticated BusinessQuery and Search use.",
    },
}


def _empty_business_query_form() -> dict[str, str]:
    return {
        "query_name": "",
        "isins": "",
        "legal_entity_type": LEGAL_ENTITY_TYPES[0],
        "amount": "",
    }


def _prefilled_business_query_form(isins: str) -> dict[str, str]:
    form = _empty_business_query_form()
    normalized_isins = _normalize_isin_input(isins)
    if normalized_isins:
        form["isins"] = "\n".join(normalized_isins)
    return form


def _normalize_isin_input(value: str) -> list[str]:
    candidates = re.split(r"[\s,;]+", value.upper())
    return [candidate for candidate in candidates if candidate]


def _validate_business_query_form(
    form_values: dict[str, str],
) -> tuple[dict[str, str], dict[str, object] | None]:
    errors: dict[str, str] = {}
    query_name = form_values["query_name"].strip()
    normalized_isins = _normalize_isin_input(form_values["isins"])
    legal_entity_type = form_values["legal_entity_type"]
    amount_text = form_values["amount"].strip()

    if not query_name:
        errors["query_name"] = "Enter a custom query name."

    if not normalized_isins:
        errors["isins"] = "Enter at least one ISIN."
    else:
        invalid_isins = [isin for isin in normalized_isins if not ISIN_PATTERN.fullmatch(isin)]
        if invalid_isins:
            errors["isins"] = "Enter ISIN-like values such as IE00BMTX1Y45."

    if legal_entity_type not in LEGAL_ENTITY_TYPES:
        errors["legal_entity_type"] = "Choose a supported legal entity type."

    amount: Decimal | None = None
    if not amount_text:
        errors["amount"] = "Enter a positive amount."
    else:
        try:
            amount = Decimal(amount_text)
        except InvalidOperation:
            errors["amount"] = "Enter a numeric amount."
        else:
            if not amount.is_finite():
                errors["amount"] = "Enter a numeric amount."
            elif amount <= 0:
                errors["amount"] = "Enter a positive amount."

    if errors:
        return errors, None

    return errors, {
        "query_name": query_name,
        "isins": normalized_isins,
        "isins_text": "\n".join(normalized_isins),
        "legal_entity_type": legal_entity_type,
        "amount": str(amount),
    }


def _business_query_form_values(form: Any) -> dict[str, str]:
    return {
        "query_name": str(form.get("query_name", "")),
        "isins": str(form.get("isins", "")),
        "legal_entity_type": str(form.get("legal_entity_type", "")),
        "amount": str(form.get("amount", "")),
    }


def _normalized_business_query_form(preview: dict[str, object]) -> dict[str, str]:
    return {
        "query_name": str(preview["query_name"]),
        "isins": str(preview["isins_text"]),
        "legal_entity_type": str(preview["legal_entity_type"]),
        "amount": str(preview["amount"]),
    }


def _business_query_input_from_preview(preview: dict[str, object]) -> BusinessQueryInput:
    return BusinessQueryInput(
        query_name=str(preview["query_name"]),
        isins=tuple(cast(list[str], preview["isins"])),
        legal_entity_type=str(preview["legal_entity_type"]),
        amount_multiplier=Decimal(str(preview["amount"])),
    )


def _business_query_result_to_csv(result: BusinessQueryResult) -> str:
    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=BUSINESS_QUERY_CSV_HEADERS, lineterminator="\n")
    writer.writeheader()
    for row in result.rows:
        writer.writerow(
            {
                "query_name": row.query_name,
                "isin": row.isin,
                "tax_year": row.tax_year,
                "tax_field_code": row.tax_field_code,
                "tax_field_label": row.tax_field_label,
                "legal_entity_category": row.legal_entity_category,
                "base_eur_value": row.base_eur_value,
                "amount_multiplier": row.amount_multiplier,
                "calculated_eur_value": row.calculated_eur_value,
            }
        )
    return output.getvalue()


@router.get("/", response_class=HTMLResponse)
async def home(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="home.html",
        context={"page_title": "EasyETFsAT"},
    )


def _b64encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _b64decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + padding)


def _verify_password(password: str, password_hash: str) -> bool:
    try:
        algorithm, iterations, salt, expected_hash = password_hash.split("$", 3)
    except ValueError:
        return False
    if algorithm != "pbkdf2_sha256":
        return False

    candidate = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt.encode("utf-8"),
        int(iterations),
    ).hex()
    return hmac.compare_digest(candidate, expected_hash)


def _sign_payload(payload: str, secret: str) -> str:
    return hmac.new(secret.encode("utf-8"), payload.encode("ascii"), hashlib.sha256).hexdigest()


def _create_session_cookie(username: str, settings: Settings) -> str:
    expires_at = int(time.time()) + settings.web_session_max_age_seconds
    payload = _b64encode(json.dumps({"sub": username, "exp": expires_at}).encode("utf-8"))
    signature = _sign_payload(payload, settings.web_session_secret)
    return f"{payload}.{signature}"


def _authenticated_username(request: Request) -> str | None:
    settings = get_settings()
    cookie = request.cookies.get(settings.web_session_cookie_name)
    if not cookie or "." not in cookie:
        return None

    payload, signature = cookie.rsplit(".", 1)
    expected_signature = _sign_payload(payload, settings.web_session_secret)
    if not hmac.compare_digest(signature, expected_signature):
        return None

    try:
        session = json.loads(_b64decode(payload))
    except (ValueError, json.JSONDecodeError):
        return None

    if not isinstance(session, dict):
        return None
    expires_at = session.get("exp")
    username = session.get("sub")
    if not isinstance(expires_at, int) or not isinstance(username, str):
        return None
    if expires_at < int(time.time()):
        return None
    if username != settings.web_auth_username:
        return None
    return username


@router.get("/login", response_class=HTMLResponse)
async def login_form(request: Request) -> HTMLResponse:
    if _authenticated_username(request):
        return RedirectResponse(url="/app", status_code=303)
    return templates.TemplateResponse(
        request=request,
        name="login.html",
        context={"page_title": "Log in - EasyETFsAT", "login_failed": False},
    )


@router.post("/login", response_class=HTMLResponse)
async def login(request: Request) -> HTMLResponse:
    settings = get_settings()
    form = await request.form()
    username = str(form.get("username", ""))
    password = str(form.get("password", ""))

    if username == settings.web_auth_username and _verify_password(
        password, settings.web_auth_password_hash
    ):
        response = RedirectResponse(url="/app", status_code=303)
        response.set_cookie(
            key=settings.web_session_cookie_name,
            value=_create_session_cookie(username, settings),
            max_age=settings.web_session_max_age_seconds,
            httponly=True,
            samesite="lax",
        )
        return response

    return templates.TemplateResponse(
        request=request,
        name="login.html",
        context={"page_title": "Log in - EasyETFsAT", "login_failed": True},
        status_code=401,
    )


@router.post("/logout")
async def logout() -> RedirectResponse:
    settings = get_settings()
    response = RedirectResponse(url="/login", status_code=303)
    response.delete_cookie(settings.web_session_cookie_name, httponly=True, samesite="lax")
    return response


def _render_app_shell(
    request: Request,
    section_key: str,
    *,
    business_query_form: dict[str, str] | None = None,
    business_query_errors: dict[str, str] | None = None,
    business_query_result: BusinessQueryResult | None = None,
    search_query: str = "",
    search_results: tuple[FundSearchResult, ...] = (),
    search_submitted: bool = False,
    search_database_has_records: bool = True,
    update_data_input: str = "",
    update_data_errors: dict[str, str] | None = None,
    update_data_preview_isins: tuple[str, ...] = (),
    update_data_job_results: tuple[dict[str, str], ...] = (),
) -> HTMLResponse:
    username = _authenticated_username(request)
    if username is None:
        return RedirectResponse(url="/login", status_code=303)
    section = APP_SECTIONS[section_key]
    return templates.TemplateResponse(
        request=request,
        name="app.html",
        context={
            "page_title": f"{section['title']} - EasyETFsAT",
            "username": username,
            "section": section,
            "sections": APP_SECTIONS.values(),
            "legal_entity_types": LEGAL_ENTITY_TYPES,
            "business_query_form": business_query_form or _empty_business_query_form(),
            "business_query_errors": business_query_errors or {},
            "business_query_result": business_query_result,
            "search_query": search_query,
            "search_results": search_results,
            "search_submitted": search_submitted,
            "search_database_has_records": search_database_has_records,
            "update_data_input": update_data_input,
            "update_data_errors": update_data_errors or {},
            "update_data_preview_isins": update_data_preview_isins,
            "update_data_job_results": update_data_job_results,
            "tax_lines": TAX_LINES,
        },
    )


@router.get("/app", response_class=HTMLResponse)
async def app_home(request: Request) -> HTMLResponse:
    return _render_app_shell(request, "business-query")


@router.get("/app/business-query", response_class=HTMLResponse)
async def app_business_query(request: Request, isins: str = "") -> HTMLResponse:
    return _render_app_shell(
        request,
        "business-query",
        business_query_form=_prefilled_business_query_form(isins),
    )


@router.post("/app/business-query", response_class=HTMLResponse)
async def submit_business_query(
    request: Request,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> HTMLResponse:
    username = _authenticated_username(request)
    if username is None:
        return RedirectResponse(url="/login", status_code=303)

    form = await request.form()
    form_values = _business_query_form_values(form)
    errors, preview = _validate_business_query_form(form_values)
    if preview is not None:
        form_values = _normalized_business_query_form(preview)
        result = await execute_business_query(session, _business_query_input_from_preview(preview))
    else:
        result = None

    return _render_app_shell(
        request,
        "business-query",
        business_query_form=form_values,
        business_query_errors=errors,
        business_query_result=result,
    )


@router.post("/app/business-query/export")
async def export_business_query(
    request: Request,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> Response:
    username = _authenticated_username(request)
    if username is None:
        return RedirectResponse(url="/login", status_code=303)

    form = await request.form()
    form_values = _business_query_form_values(form)
    errors, preview = _validate_business_query_form(form_values)
    if preview is None:
        return _render_app_shell(
            request,
            "business-query",
            business_query_form=form_values,
            business_query_errors=errors,
        )

    result = await execute_business_query(session, _business_query_input_from_preview(preview))
    return Response(
        content=_business_query_result_to_csv(result),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="business-query.csv"'},
    )


@router.get("/app/search", response_class=HTMLResponse)
async def app_search(request: Request, q: str = "") -> HTMLResponse:
    username = _authenticated_username(request)
    if username is None:
        return RedirectResponse(url="/login", status_code=303)

    search_query = q.strip()
    search_results: tuple[FundSearchResult, ...] = ()
    database_has_records = False

    async for session in get_session():
        database_has_records = await has_available_fund_data(session)
        if search_query and database_has_records:
            search_results = await search_available_funds(session, search_query)
        break

    return _render_app_shell(
        request,
        "search",
        search_query=search_query,
        search_results=search_results,
        search_submitted=bool(search_query),
        search_database_has_records=database_has_records,
    )


@router.get("/app/update-data", response_class=HTMLResponse)
async def app_update_data(request: Request) -> HTMLResponse:
    return _render_app_shell(request, "update-data")


@router.post("/app/update-data", response_class=HTMLResponse)
async def submit_update_data(
    request: Request,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> HTMLResponse:
    username = _authenticated_username(request)
    if username is None:
        return RedirectResponse(url="/login", status_code=303)

    form = await request.form()
    raw_isins = str(form.get("isins", ""))
    errors, normalized_isins = _validate_update_data_isins(raw_isins)
    job_results: tuple[dict[str, str], ...] = ()
    if not errors:
        job_results = await _queue_update_data_jobs(session, normalized_isins, username)

    return _render_app_shell(
        request,
        "update-data",
        update_data_input=raw_isins if errors else "\n".join(normalized_isins),
        update_data_errors=errors,
        update_data_preview_isins=normalized_isins,
        update_data_job_results=job_results,
    )


async def _queue_update_data_jobs(
    session: AsyncSession,
    normalized_isins: tuple[str, ...],
    username: str,
) -> tuple[dict[str, str], ...]:
    results: list[dict[str, str]] = []
    jobs_to_queue: list[INGJOB] = []

    for isin in normalized_isins:
        with session.no_autoflush:
            active_job = await session.scalar(INGJOB.active_for_isin(isin).limit(1))
        if active_job is not None:
            results.append(
                {
                    "isin": isin,
                    "status": "skipped",
                    "message": "Skipped: active update job already exists.",
                }
            )
            continue

        job = INGJOB(
            isin=isin,
            requested_user=username,
            status="queued",
            message="Queued for update.",
        )
        session.add(job)
        jobs_to_queue.append(job)
        results.append({"isin": isin, "status": "queued", "message": "Queued for update."})

    if jobs_to_queue:
        await session.commit()

    return tuple(results)


@router.get("/app/documentation", response_class=HTMLResponse)
async def app_documentation(request: Request) -> HTMLResponse:
    return _render_app_shell(request, "documentation")


def _validate_update_data_isins(raw_isins: str) -> tuple[dict[str, str], tuple[str, ...]]:
    normalized_isins = _normalize_update_data_isin_input(raw_isins)
    if not normalized_isins:
        return {"isins": "Enter at least one ISIN."}, ()

    invalid_isins = [isin for isin in normalized_isins if not _has_valid_isin_checksum(isin)]
    if invalid_isins:
        invalid_list = ", ".join(invalid_isins[:3])
        if len(invalid_isins) > 3:
            invalid_list = f"{invalid_list}, ..."
        return {"isins": f"Enter valid ISINs. Invalid values: {invalid_list}."}, ()

    return {}, tuple(normalized_isins)


def _normalize_update_data_isin_input(raw_isins: str) -> list[str]:
    seen: set[str] = set()
    normalized_isins: list[str] = []
    for candidate in _normalize_isin_input(raw_isins):
        if candidate not in seen:
            normalized_isins.append(candidate)
            seen.add(candidate)
    return normalized_isins


def _has_valid_isin_checksum(isin: str) -> bool:
    if not ISIN_PATTERN.fullmatch(isin):
        return False

    digits = "".join(str(int(char, 36)) for char in isin)
    checksum = 0
    parity = len(digits) % 2
    for index, digit_text in enumerate(digits):
        digit = int(digit_text)
        if index % 2 == parity:
            digit *= 2
        checksum += digit // 10 + digit % 10
    return checksum % 10 == 0
