import base64
import hashlib
import hmac
import json
import time
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from fondant.config import Settings, get_settings

TEMPLATE_DIR = Path(__file__).resolve().parents[1] / "templates"

templates = Jinja2Templates(directory=TEMPLATE_DIR)
router = APIRouter(include_in_schema=False)


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


@router.get("/app", response_class=HTMLResponse)
async def app_home(request: Request) -> HTMLResponse:
    username = _authenticated_username(request)
    if username is None:
        return RedirectResponse(url="/login", status_code=303)
    return templates.TemplateResponse(
        request=request,
        name="app.html",
        context={"page_title": "App - EasyETFsAT", "username": username},
    )
