import base64
import csv
import hashlib
import hmac
import json
import logging
import re
import time
from datetime import date
from decimal import Decimal, InvalidOperation
from io import StringIO
from pathlib import Path
from typing import Annotated, Any, cast

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from fondant import update_data
from fondant.business_query import (
    ALL_AVAILABLE_YEARS,
    DEFAULT_SUBCATEGORY_KEYS,
    BusinessQueryInput,
    BusinessQueryResult,
    execute_business_query,
)
from fondant.config import Settings, get_settings
from fondant.db.models import BQGROUP, BQSAVED, INGJOB
from fondant.db.session import get_session
from fondant.search import FundSearchResult, has_available_fund_data, search_available_funds
from fondant.tax_registry import TAX_LINES

TEMPLATE_DIR = Path(__file__).resolve().parents[1] / "templates"

templates = Jinja2Templates(directory=TEMPLATE_DIR)
router = APIRouter(include_in_schema=False)
logger = logging.getLogger(__name__)

LEGAL_ENTITY_TYPES = ("natural person", "business", "Stiftung")
BUSINESS_QUERY_SUBCATEGORY_OPTIONS = {
    "natural person": (
        ("natural_person_pa_with_option", "PA mit Option"),
        ("natural_person_pa_without_option", "PA ohne Option"),
        ("natural_person_all", "All private investor categories"),
    ),
    "business": (
        ("business_bv_with_option", "BV mit Option"),
        ("business_bv_without_option", "BV ohne Option"),
        ("business_bv_legal_person", "BV jur. Person"),
        ("business_all", "All business categories"),
    ),
    "Stiftung": (("stiftung", "Stiftung"),),
}
BUSINESS_QUERY_SUBCATEGORY_LABELS = {
    key: label
    for options in BUSINESS_QUERY_SUBCATEGORY_OPTIONS.values()
    for key, label in options
}
BUSINESS_QUERY_TAX_YEAR_OPTIONS = tuple(
    str(year) for year in range(date.today().year, date.today().year - 8, -1)
)
BUSINESS_QUERY_TAX_FIELD_METADATA = {tax_line.line_code: tax_line for tax_line in TAX_LINES}
ISIN_PATTERN = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")
RECENT_UPDATE_DATA_JOB_LIMIT = 20
BACKGROUND_UPDATE_DATA_JOB_LIMIT = 10
BUSINESS_QUERY_CSV_HEADERS = (
    "query_name",
    "isin",
    "tax_year",
    "tax_field_code",
    "tax_field_label",
    "legal_entity_category",
    "original_home_currency",
    "base_home_currency_value",
    "calculated_home_currency_value",
    "base_eur_value",
    "amount_multiplier",
    "calculated_eur_value",
    "fx_rate",
    "fx_date",
)

APP_SECTIONS = {
    "business-query": {
        "section_key": "business-query",
        "label": "BusinessQuery",
        "path": "/app/business-query",
        "title": "BusinessQuery",
        "summary": "Run structured Austrian ETF tax queries for authenticated review.",
        "children": (
            {
                "label": "Add New Query",
                "path": "/app/business-query/new",
                "section_key": "business-query",
            },
            {
                "label": "Queries",
                "path": "/app/business-query/queries",
                "section_key": "business-query-queries",
            },
        ),
    },
    "business-query-queries": {
        "section_key": "business-query-queries",
        "label": "Queries",
        "path": "/app/business-query/queries",
        "title": "Queries",
        "summary": "Manage saved BusinessQuery rules for authenticated review.",
        "nav_parent": "business-query",
        "hide_from_primary_nav": True,
    },
    "business-query-edit": {
        "section_key": "business-query-edit",
        "label": "Edit Query",
        "path": "/app/business-query/queries",
        "title": "Edit Query",
        "summary": "Update a saved BusinessQuery rule.",
        "nav_parent": "business-query",
        "hide_from_primary_nav": True,
    },
    "search": {
        "section_key": "search",
        "label": "Search",
        "path": "/app/search",
        "title": "Search",
        "summary": "Discover available fund tax data by ISIN or security name.",
    },
    "update-data": {
        "section_key": "update-data",
        "label": "Update Data",
        "path": "/app/update-data",
        "title": "Update Data",
        "summary": "Prepare future authenticated fund data refresh workflows.",
    },
    "documentation": {
        "section_key": "documentation",
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
        "subcategory_key": DEFAULT_SUBCATEGORY_KEYS[LEGAL_ENTITY_TYPES[0]],
        "tax_year_filter": ALL_AVAILABLE_YEARS,
        "amount": "",
        "note": "",
        "group_id": "",
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
    *,
    require_isins: bool = True,
) -> tuple[dict[str, str], dict[str, object] | None]:
    errors: dict[str, str] = {}
    query_name = form_values["query_name"].strip()
    normalized_isins = _normalize_isin_input(form_values["isins"])
    legal_entity_type = form_values["legal_entity_type"]
    subcategory_key = form_values["subcategory_key"].strip()
    tax_year_filter = form_values["tax_year_filter"].strip()
    amount_text = form_values["amount"].strip()
    note = form_values["note"].strip()

    if not query_name:
        errors["query_name"] = "Enter a custom query name."

    if not normalized_isins and require_isins:
        errors["isins"] = "Enter at least one ISIN."
    elif normalized_isins:
        invalid_isins = [isin for isin in normalized_isins if not ISIN_PATTERN.fullmatch(isin)]
        if invalid_isins:
            errors["isins"] = "Enter ISIN-like values such as IE00BMTX1Y45."

    if legal_entity_type not in LEGAL_ENTITY_TYPES:
        errors["legal_entity_type"] = "Choose a supported legal entity type."
    else:
        allowed_subcategory_keys = {
            key for key, _label in BUSINESS_QUERY_SUBCATEGORY_OPTIONS[legal_entity_type]
        }
        if not subcategory_key:
            errors["subcategory_key"] = "Choose a category for the selected legal entity type."
        elif subcategory_key not in allowed_subcategory_keys:
            errors["subcategory_key"] = "Choose a category that matches the selected legal entity type."

    if tax_year_filter == "":
        errors["tax_year_filter"] = "Choose a tax year."
    elif tax_year_filter != ALL_AVAILABLE_YEARS and tax_year_filter not in BUSINESS_QUERY_TAX_YEAR_OPTIONS:
        errors["tax_year_filter"] = "Choose All available years or one of the listed tax years."

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
        "subcategory_key": subcategory_key,
        "tax_year_filter": tax_year_filter,
        "amount": str(amount),
        "note": note,
    }


def _business_query_form_values(form: Any) -> dict[str, str]:
    legal_entity_type = str(form.get("legal_entity_type", ""))
    return {
        "query_name": str(form.get("query_name", "")),
        "isins": str(form.get("isins", "")),
        "legal_entity_type": legal_entity_type,
        "subcategory_key": str(
            form.get("subcategory_key", "") or DEFAULT_SUBCATEGORY_KEYS.get(legal_entity_type, "")
        ),
        "tax_year_filter": str(form.get("tax_year_filter", "") or ALL_AVAILABLE_YEARS),
        "amount": str(form.get("amount", "")),
        "note": str(form.get("note", "")),
    }


def _normalized_business_query_form(preview: dict[str, object]) -> dict[str, str]:
    return {
        "query_name": str(preview["query_name"]),
        "isins": str(preview["isins_text"]),
        "legal_entity_type": str(preview["legal_entity_type"]),
        "subcategory_key": str(preview["subcategory_key"]),
        "tax_year_filter": str(preview["tax_year_filter"]),
        "amount": str(preview["amount"]),
        "note": str(preview["note"]),
    }


def _business_query_form_from_saved_query(saved_query: BQSAVED) -> dict[str, str]:
    form = _empty_business_query_form()
    form.update(
        {
            "query_name": saved_query.query_name,
            "isins": "\n".join(saved_query.default_isins or []),
            "legal_entity_type": saved_query.legal_entity_type,
            "subcategory_key": saved_query.subcategory_key,
            "tax_year_filter": saved_query.tax_year_filter,
            "amount": str(saved_query.amount),
            "note": saved_query.note or "",
            "group_id": str(saved_query.group_id or ""),
        }
    )
    return form


def _business_query_input_from_preview(preview: dict[str, object]) -> BusinessQueryInput:
    return BusinessQueryInput(
        query_name=str(preview["query_name"]),
        isins=tuple(cast(list[str], preview["isins"])),
        legal_entity_type=str(preview["legal_entity_type"]),
        subcategory_key=str(preview["subcategory_key"]),
        tax_year_filter=str(preview["tax_year_filter"]),
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
                "original_home_currency": (
                    row.home_currency_code or row.original_currency_code or row.fund_currency
                ),
                "base_home_currency_value": row.base_home_currency_value,
                "calculated_home_currency_value": row.calculated_home_currency_value,
                "base_eur_value": row.base_eur_value,
                "amount_multiplier": row.amount_multiplier,
                "calculated_eur_value": row.calculated_eur_value,
                "fx_rate": row.fx_rate,
                "fx_date": row.fx_date or row.report_date,
            }
        )
    return output.getvalue()


def _format_business_query_tax_year_filter(value: str) -> str:
    if value == ALL_AVAILABLE_YEARS:
        return "All available years"
    return value


def _format_optional_timestamp(value: object) -> str:
    if value is None:
        return "-"
    return str(value)


async def _saved_business_queries(
    session: AsyncSession,
    username: str,
    *,
    selected_group_id: str = "",
) -> tuple[dict[str, str], ...]:
    query = select(BQSAVED).where(BQSAVED.owner_username == username)
    if selected_group_id == "ungrouped":
        query = query.where(BQSAVED.group_id.is_(None))
    elif selected_group_id:
        try:
            group_id = int(selected_group_id)
        except ValueError:
            group_id = 0
        query = query.where(BQSAVED.group_id == group_id)

    saved_queries = (
        await session.scalars(
            query.order_by(BQSAVED.updated_at.desc(), BQSAVED.id.desc())
        )
    ).all()
    group_names = await _business_query_group_names(session, username)

    return tuple(
        {
            "id": str(saved_query.id),
            "query_name": saved_query.query_name,
            "legal_entity_type": saved_query.legal_entity_type,
            "subcategory_label": BUSINESS_QUERY_SUBCATEGORY_LABELS.get(
                saved_query.subcategory_key,
                saved_query.subcategory_key,
            ),
            "tax_year_filter": _format_business_query_tax_year_filter(
                saved_query.tax_year_filter
            ),
            "amount": str(saved_query.amount),
            "group_id": str(saved_query.group_id or ""),
            "group_name": group_names.get(saved_query.group_id or 0, "Ungrouped"),
            "updated_at": _format_optional_timestamp(saved_query.updated_at),
        }
        for saved_query in saved_queries
    )


async def _business_query_group_names(session: AsyncSession, username: str) -> dict[int, str]:
    groups = (
        await session.scalars(
            select(BQGROUP)
            .where(BQGROUP.owner_username == username)
            .order_by(BQGROUP.group_name.asc(), BQGROUP.id.asc())
        )
    ).all()
    return {group.id: group.group_name for group in groups}


async def _business_query_group_options(
    session: AsyncSession,
    username: str,
) -> tuple[dict[str, str], ...]:
    groups = (
        await session.scalars(
            select(BQGROUP)
            .where(BQGROUP.owner_username == username)
            .order_by(BQGROUP.group_name.asc(), BQGROUP.id.asc())
        )
    ).all()
    return tuple(
        {
            "id": str(group.id),
            "group_name": group.group_name,
            "description": group.description or "",
        }
        for group in groups
    )


def _group_saved_business_queries(
    saved_queries: tuple[dict[str, str], ...],
    group_options: tuple[dict[str, str], ...],
    selected_group_id: str,
) -> tuple[dict[str, object], ...]:
    if selected_group_id == "ungrouped":
        return (
            {
                "id": "ungrouped",
                "group_name": "Ungrouped",
                "description": "",
                "queries": saved_queries,
            },
        )

    if selected_group_id:
        group = next(
            (option for option in group_options if option["id"] == selected_group_id),
            None,
        )
        if group is None:
            return ()
        return (
            {
                "id": group["id"],
                "group_name": group["group_name"],
                "description": group["description"],
                "queries": saved_queries,
            },
        )

    grouped_queries: list[dict[str, object]] = []
    for group in group_options:
        group_queries = tuple(
            saved_query
            for saved_query in saved_queries
            if saved_query["group_id"] == group["id"]
        )
        if group_queries:
            grouped_queries.append(
                {
                    "id": group["id"],
                    "group_name": group["group_name"],
                    "description": group["description"],
                    "queries": group_queries,
                }
            )

    ungrouped_queries = tuple(
        saved_query for saved_query in saved_queries if not saved_query["group_id"]
    )
    if ungrouped_queries:
        grouped_queries.append(
            {
                "id": "ungrouped",
                "group_name": "Ungrouped",
                "description": "",
                "queries": ungrouped_queries,
            }
        )

    return tuple(grouped_queries)


def _business_query_group_form_values(form: Any) -> dict[str, str]:
    return {
        "group_name": str(form.get("group_name", "")),
        "description": str(form.get("description", "")),
    }


def _validate_business_query_group_form(form_values: dict[str, str]) -> dict[str, str]:
    errors: dict[str, str] = {}
    if not form_values["group_name"].strip():
        errors["group_name"] = "Enter a group name."
    return errors


async def _saved_business_query_for_owner(
    session: AsyncSession,
    username: str,
    saved_query_id: int,
) -> BQSAVED | None:
    return await session.scalar(
        select(BQSAVED).where(
            BQSAVED.id == saved_query_id,
            BQSAVED.owner_username == username,
        )
    )


async def _validate_business_query_group_assignment(
    session: AsyncSession,
    username: str,
    group_id_text: str,
) -> tuple[str | None, int | None]:
    if not group_id_text:
        return None, None
    try:
        group_id = int(group_id_text)
    except ValueError:
        return "Choose one of your saved query groups.", None

    group = await session.scalar(
        select(BQGROUP).where(
            BQGROUP.id == group_id,
            BQGROUP.owner_username == username,
        )
    )
    if group is None:
        return "Choose one of your saved query groups.", None
    return None, group_id


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
    business_query_status: str = "",
    business_query_result: BusinessQueryResult | None = None,
    saved_business_queries: tuple[dict[str, str], ...] = (),
    business_query_group_options: tuple[dict[str, str], ...] = (),
    grouped_saved_business_queries: tuple[dict[str, object], ...] = (),
    selected_business_query_group_id: str = "",
    business_query_group_form: dict[str, str] | None = None,
    business_query_group_errors: dict[str, str] | None = None,
    edit_saved_query_id: int | None = None,
    search_query: str = "",
    search_results: tuple[FundSearchResult, ...] = (),
    search_submitted: bool = False,
    search_database_has_records: bool = True,
    update_data_input: str = "",
    update_data_errors: dict[str, str] | None = None,
    update_data_preview_isins: tuple[str, ...] = (),
    update_data_job_results: tuple[dict[str, str], ...] = (),
    update_data_recent_jobs: tuple[dict[str, str], ...] = (),
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
            "section_key": section_key,
            "sections": tuple(
                item
                for item in APP_SECTIONS.values()
                if not item.get("hide_from_primary_nav")
            ),
            "legal_entity_types": LEGAL_ENTITY_TYPES,
            "business_query_subcategory_options": BUSINESS_QUERY_SUBCATEGORY_OPTIONS,
            "business_query_subcategory_labels": BUSINESS_QUERY_SUBCATEGORY_LABELS,
            "all_available_years": ALL_AVAILABLE_YEARS,
            "business_query_tax_year_options": BUSINESS_QUERY_TAX_YEAR_OPTIONS,
            "business_query_tax_field_metadata": BUSINESS_QUERY_TAX_FIELD_METADATA,
            "business_query_form": business_query_form or _empty_business_query_form(),
            "business_query_errors": business_query_errors or {},
            "business_query_status": business_query_status,
            "business_query_result": business_query_result,
            "saved_business_queries": saved_business_queries,
            "business_query_group_options": business_query_group_options,
            "grouped_saved_business_queries": grouped_saved_business_queries,
            "selected_business_query_group_id": selected_business_query_group_id,
            "business_query_group_form": business_query_group_form
            or {"group_name": "", "description": ""},
            "business_query_group_errors": business_query_group_errors or {},
            "edit_saved_query_id": edit_saved_query_id,
            "search_query": search_query,
            "search_results": search_results,
            "search_submitted": search_submitted,
            "search_database_has_records": search_database_has_records,
            "update_data_input": update_data_input,
            "update_data_errors": update_data_errors or {},
            "update_data_preview_isins": update_data_preview_isins,
            "update_data_job_results": update_data_job_results,
            "update_data_recent_jobs": update_data_recent_jobs,
            "tax_lines": TAX_LINES,
        },
    )


@router.get("/app", response_class=HTMLResponse)
async def app_home(
    request: Request,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> HTMLResponse:
    username = _authenticated_username(request)
    if username is None:
        return RedirectResponse(url="/login", status_code=303)
    return _render_app_shell(
        request,
        "business-query",
        saved_business_queries=await _saved_business_queries(session, username),
    )


@router.get("/app/business-query", response_class=HTMLResponse)
async def app_business_query(
    request: Request,
    session: Annotated[AsyncSession, Depends(get_session)],
    isins: str = "",
) -> HTMLResponse:
    username = _authenticated_username(request)
    if username is None:
        return RedirectResponse(url="/login", status_code=303)
    return _render_app_shell(
        request,
        "business-query",
        business_query_form=_prefilled_business_query_form(isins),
        saved_business_queries=await _saved_business_queries(session, username),
    )


@router.get("/app/business-query/new", response_class=HTMLResponse)
async def app_business_query_new(
    request: Request,
    session: Annotated[AsyncSession, Depends(get_session)],
    isins: str = "",
) -> HTMLResponse:
    username = _authenticated_username(request)
    if username is None:
        return RedirectResponse(url="/login", status_code=303)
    return _render_app_shell(
        request,
        "business-query",
        business_query_form=_prefilled_business_query_form(isins),
        saved_business_queries=await _saved_business_queries(session, username),
    )


@router.post("/app/business-query", response_class=HTMLResponse)
async def submit_business_query(
    request: Request,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> HTMLResponse:
    return await submit_business_query_new(request, session)


@router.post("/app/business-query/new", response_class=HTMLResponse)
async def submit_business_query_new(
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
        saved_business_queries=await _saved_business_queries(session, username),
    )


@router.get("/app/business-query/queries", response_class=HTMLResponse)
async def app_business_query_queries(
    request: Request,
    session: Annotated[AsyncSession, Depends(get_session)],
    group_id: str = "",
) -> HTMLResponse:
    username = _authenticated_username(request)
    if username is None:
        return RedirectResponse(url="/login", status_code=303)
    group_options = await _business_query_group_options(session, username)
    selected_group_id = group_id.strip()
    if selected_group_id and selected_group_id != "ungrouped":
        selected_group_id = (
            selected_group_id
            if any(group["id"] == selected_group_id for group in group_options)
            else ""
        )
    saved_queries = await _saved_business_queries(
        session,
        username,
        selected_group_id=selected_group_id,
    )
    return _render_app_shell(
        request,
        "business-query-queries",
        saved_business_queries=saved_queries,
        business_query_group_options=group_options,
        grouped_saved_business_queries=_group_saved_business_queries(
            saved_queries,
            group_options,
            selected_group_id,
        ),
        selected_business_query_group_id=selected_group_id,
    )


@router.post("/app/business-query/groups", response_class=HTMLResponse)
async def create_business_query_group(
    request: Request,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> HTMLResponse:
    username = _authenticated_username(request)
    if username is None:
        return RedirectResponse(url="/login", status_code=303)

    form = await request.form()
    group_form = _business_query_group_form_values(form)
    errors = _validate_business_query_group_form(group_form)
    status = ""
    if not errors:
        group_form = {
            "group_name": group_form["group_name"].strip(),
            "description": group_form["description"].strip(),
        }
        session.add(
            BQGROUP(
                owner_username=username,
                group_name=group_form["group_name"],
                description=group_form["description"] or None,
            )
        )
        try:
            await session.commit()
        except IntegrityError:
            await session.rollback()
            errors["group_name"] = "A group with this name already exists."
        else:
            status = "Group created."
            group_form = {"group_name": "", "description": ""}

    group_options = await _business_query_group_options(session, username)
    saved_queries = await _saved_business_queries(session, username)
    return _render_app_shell(
        request,
        "business-query-queries",
        business_query_status=status,
        saved_business_queries=saved_queries,
        business_query_group_options=group_options,
        grouped_saved_business_queries=_group_saved_business_queries(
            saved_queries,
            group_options,
            "",
        ),
        business_query_group_form=group_form,
        business_query_group_errors=errors,
    )


@router.get("/app/business-query/queries/{saved_query_id}/edit", response_class=HTMLResponse)
async def edit_business_query_form(
    request: Request,
    session: Annotated[AsyncSession, Depends(get_session)],
    saved_query_id: int,
) -> HTMLResponse:
    username = _authenticated_username(request)
    if username is None:
        return RedirectResponse(url="/login", status_code=303)

    saved_query = await _saved_business_query_for_owner(session, username, saved_query_id)
    if saved_query is None:
        raise HTTPException(status_code=404, detail="Saved query not found")

    return _render_app_shell(
        request,
        "business-query-edit",
        business_query_form=_business_query_form_from_saved_query(saved_query),
        saved_business_queries=await _saved_business_queries(session, username),
        business_query_group_options=await _business_query_group_options(session, username),
        edit_saved_query_id=saved_query_id,
    )


@router.post("/app/business-query/queries/{saved_query_id}/edit", response_class=HTMLResponse)
async def edit_business_query(
    request: Request,
    session: Annotated[AsyncSession, Depends(get_session)],
    saved_query_id: int,
) -> HTMLResponse:
    username = _authenticated_username(request)
    if username is None:
        return RedirectResponse(url="/login", status_code=303)

    saved_query = await _saved_business_query_for_owner(session, username, saved_query_id)
    if saved_query is None:
        raise HTTPException(status_code=404, detail="Saved query not found")

    form = await request.form()
    form_values = _business_query_form_values(form)
    group_id_text = str(form.get("group_id", "")).strip()
    form_values["group_id"] = group_id_text
    errors, preview = _validate_business_query_form(form_values, require_isins=False)
    group_error, group_id = await _validate_business_query_group_assignment(
        session,
        username,
        group_id_text,
    )
    if group_error:
        errors["group_id"] = group_error

    status = ""
    if preview is not None and not errors:
        form_values = _normalized_business_query_form(preview)
        form_values["group_id"] = group_id_text
        saved_query.group_id = group_id
        saved_query.query_name = str(preview["query_name"])
        saved_query.legal_entity_type = str(preview["legal_entity_type"])
        saved_query.subcategory_key = str(preview["subcategory_key"])
        saved_query.tax_year_filter = str(preview["tax_year_filter"])
        saved_query.amount = Decimal(str(preview["amount"]))
        saved_query.note = str(preview["note"]) or None
        saved_query.default_isins = cast(list[str], preview["isins"]) or None
        try:
            await session.commit()
        except IntegrityError:
            await session.rollback()
            errors["query_name"] = "A saved query with this name already exists."
        else:
            status = "Saved query updated."

    return _render_app_shell(
        request,
        "business-query-edit",
        business_query_form=form_values,
        business_query_errors=errors,
        business_query_status=status,
        saved_business_queries=await _saved_business_queries(session, username),
        business_query_group_options=await _business_query_group_options(session, username),
        edit_saved_query_id=saved_query_id,
    )


@router.post("/app/business-query/save", response_class=HTMLResponse)
async def save_business_query(
    request: Request,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> HTMLResponse:
    username = _authenticated_username(request)
    if username is None:
        return RedirectResponse(url="/login", status_code=303)

    form = await request.form()
    form_values = _business_query_form_values(form)
    errors, preview = _validate_business_query_form(form_values, require_isins=False)
    status = ""
    if preview is not None:
        form_values = _normalized_business_query_form(preview)
        saved_query = BQSAVED(
            owner_username=username,
            query_name=str(preview["query_name"]),
            legal_entity_type=str(preview["legal_entity_type"]),
            subcategory_key=str(preview["subcategory_key"]),
            tax_year_filter=str(preview["tax_year_filter"]),
            amount=Decimal(str(preview["amount"])),
            note=str(preview["note"]) or None,
            default_isins=cast(list[str], preview["isins"]) or None,
        )
        session.add(saved_query)
        try:
            await session.commit()
        except IntegrityError:
            await session.rollback()
            errors["query_name"] = "A saved query with this name already exists."
        else:
            status = "Saved query created."

    return _render_app_shell(
        request,
        "business-query",
        business_query_form=form_values,
        business_query_errors=errors,
        business_query_status=status,
        saved_business_queries=await _saved_business_queries(session, username),
    )


@router.get("/app/business-query/saved/{saved_query_id}/load", response_class=HTMLResponse)
async def load_business_query(
    request: Request,
    session: Annotated[AsyncSession, Depends(get_session)],
    saved_query_id: int,
) -> HTMLResponse:
    username = _authenticated_username(request)
    if username is None:
        return RedirectResponse(url="/login", status_code=303)

    saved_query = await _saved_business_query_for_owner(session, username, saved_query_id)
    if saved_query is None:
        raise HTTPException(status_code=404, detail="Saved query not found")

    return _render_app_shell(
        request,
        "business-query",
        business_query_form=_business_query_form_from_saved_query(saved_query),
        business_query_status="Saved query loaded.",
        saved_business_queries=await _saved_business_queries(session, username),
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
            saved_business_queries=await _saved_business_queries(session, username),
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
async def app_update_data(
    request: Request,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> HTMLResponse:
    username = _authenticated_username(request)
    if username is None:
        return RedirectResponse(url="/login", status_code=303)

    recent_jobs = await _recent_update_data_jobs(session)
    return _render_app_shell(request, "update-data", update_data_recent_jobs=recent_jobs)


@router.post("/app/update-data", response_class=HTMLResponse)
async def submit_update_data(
    request: Request,
    background_tasks: BackgroundTasks,
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
        if any(result["status"] == "queued" for result in job_results):
            background_tasks.add_task(
                _run_queued_update_jobs_background,
                limit=BACKGROUND_UPDATE_DATA_JOB_LIMIT,
            )
    recent_jobs = await _recent_update_data_jobs(session)

    return _render_app_shell(
        request,
        "update-data",
        update_data_input=raw_isins if errors else "\n".join(normalized_isins),
        update_data_errors=errors,
        update_data_preview_isins=normalized_isins,
        update_data_job_results=job_results,
        update_data_recent_jobs=recent_jobs,
    )


async def _recent_update_data_jobs(session: AsyncSession) -> tuple[dict[str, str], ...]:
    jobs = (
        await session.scalars(
            select(INGJOB)
            .order_by(INGJOB.created_at.desc(), INGJOB.id.desc())
            .limit(RECENT_UPDATE_DATA_JOB_LIMIT)
        )
    ).all()

    return tuple(
        {
            "isin": job.isin,
            "status": job.status,
            "requested_user": job.requested_user or "-",
            "created_at": _format_update_data_timestamp(job.created_at),
            "started_at": _format_update_data_timestamp(job.started_at),
            "finished_at": _format_update_data_timestamp(job.finished_at),
            "message": job.message or "-",
            "error_detail": job.error_detail or "-",
        }
        for job in jobs
    )


def _format_update_data_timestamp(value: object) -> str:
    if value is None:
        return "-"
    return str(value)


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


async def _run_queued_update_jobs_background(limit: int) -> None:
    try:
        await run_queued_update_jobs(limit=limit)
    except Exception:
        logger.exception("Background update-data job execution failed.")


async def run_queued_update_jobs(limit: int = BACKGROUND_UPDATE_DATA_JOB_LIMIT) -> object:
    return await update_data.run_queued_update_jobs(limit=limit)


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
