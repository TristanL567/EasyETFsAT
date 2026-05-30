from __future__ import annotations

import argparse
import csv
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import sqlalchemy as sa
from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError

try:
    from fondant.config import get_settings
except ModuleNotFoundError:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from fondant.config import get_settings


DEFAULT_STORAGE_PATH = Path("Documentation/isin_storage.csv")
DEFAULT_FX_FRESHNESS_DAYS = 7
BASE_EXPECTED_CURRENCIES = frozenset({"USD", "GBP", "CHF"})
REQUIRED_TABLES = frozenset({"SOURCERPT", "TAXRPT", "REFEXC"})


@dataclass(frozen=True)
class ReadinessSummary:
    storage_path: Path
    storage_available: bool
    storage_isin_count: int | None
    missing_source_isins: list[str]
    source_without_tax_isins: list[str]
    observed_non_eur_currencies: list[str]
    expected_fx_currencies: list[str]
    latest_fx_by_currency: list[tuple[str, Any]]
    stale_fx_currencies: list[tuple[str, str, str]]
    threshold_days: int
    check_date: date


def sync_database_url(database_url: str) -> str:
    return database_url.replace("+asyncpg", "+psycopg").replace("+aiosqlite", "+pysqlite")


def create_readiness_engine(database_url: str | None = None) -> Engine:
    url = sync_database_url(database_url or get_settings().database_url)
    return sa.create_engine(url, future=True)


def build_readiness_report(
    engine: Engine,
    *,
    storage_path: Path = DEFAULT_STORAGE_PATH,
    threshold_days: int = DEFAULT_FX_FRESHNESS_DAYS,
    check_date: date | None = None,
) -> tuple[str, int]:
    with engine.connect() as connection:
        summary = inspect_data_readiness(
            connection,
            storage_path=storage_path,
            threshold_days=threshold_days,
            check_date=check_date or date.today(),
        )
    return render_readiness_summary(summary), 0


def inspect_data_readiness(
    connection: Connection,
    *,
    storage_path: Path = DEFAULT_STORAGE_PATH,
    threshold_days: int = DEFAULT_FX_FRESHNESS_DAYS,
    check_date: date,
) -> ReadinessSummary:
    inspector = sa.inspect(connection)
    table_set = set(inspector.get_table_names())
    missing_tables = sorted(REQUIRED_TABLES - table_set)
    if missing_tables:
        raise RuntimeError(f"Missing required readiness tables: {', '.join(missing_tables)}")

    storage_isins = load_storage_isins(storage_path)
    source_isins = _fetch_distinct_values(connection, "SOURCERPT", "SRCISN")
    tax_isins = _fetch_distinct_values(connection, "TAXRPT", "TAXISN")
    observed_non_eur_currencies = _fetch_observed_non_eur_currencies(connection)
    expected_fx_currencies = sorted(BASE_EXPECTED_CURRENCIES | set(observed_non_eur_currencies))
    latest_fx_by_currency = _fetch_latest_fx_by_currency(connection, expected_fx_currencies)
    stale_fx_currencies = _stale_fx_currencies(
        latest_fx_by_currency,
        threshold_days=threshold_days,
        check_date=check_date,
    )

    if storage_isins is None:
        missing_source_isins: list[str] = []
    else:
        missing_source_isins = sorted(set(storage_isins) - set(source_isins))

    source_without_tax_isins = sorted(set(source_isins) - set(tax_isins))

    return ReadinessSummary(
        storage_path=storage_path,
        storage_available=storage_isins is not None,
        storage_isin_count=len(storage_isins) if storage_isins is not None else None,
        missing_source_isins=missing_source_isins,
        source_without_tax_isins=source_without_tax_isins,
        observed_non_eur_currencies=observed_non_eur_currencies,
        expected_fx_currencies=expected_fx_currencies,
        latest_fx_by_currency=latest_fx_by_currency,
        stale_fx_currencies=stale_fx_currencies,
        threshold_days=threshold_days,
        check_date=check_date,
    )


def load_storage_isins(storage_path: Path) -> list[str] | None:
    if not storage_path.exists():
        return None

    isins: list[str] = []
    with storage_path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames or "ISIN" not in reader.fieldnames:
            return []
        for row in reader:
            isin = (row.get("ISIN") or "").strip().upper()
            if isin:
                isins.append(isin)
    return sorted(dict.fromkeys(isins))


def render_readiness_summary(summary: ReadinessSummary) -> str:
    storage_count = (
        str(summary.storage_isin_count)
        if summary.storage_available
        else f"unavailable ({summary.storage_path})"
    )
    lines = [
        "== Data Readiness Summary ==",
        f"check_date: {summary.check_date.isoformat()}",
        f"fx_freshness_threshold_days: {summary.threshold_days}",
        f"storage_isin_count: {storage_count}",
        "",
        "== Missing Source Reports ==",
        _format_single_column("isin", summary.missing_source_isins)
        if summary.storage_available
        else "storage ISIN file unavailable; missing source report comparison skipped",
        "",
        "== Source Reports Missing Curated TAXRPT ==",
        _format_single_column("isin", summary.source_without_tax_isins),
        "",
        "== Observed Non-EUR TAXRPT Currencies ==",
        _format_single_column("currency", summary.observed_non_eur_currencies),
        "",
        "== REFEXC Latest Date By Currency ==",
        _format_rows(("currency", "latest_rate_date"), summary.latest_fx_by_currency),
        "",
        "== Stale REFEXC Currencies ==",
        _format_rows(("currency", "latest_rate_date", "diagnostic"), summary.stale_fx_currencies),
        "",
        "Read-only: SELECT/introspection only; no OeKB or ECB fetches; no row mutations.",
    ]
    return "\n".join(lines) + "\n"


def _fetch_distinct_values(connection: Connection, table_name: str, column_name: str) -> list[str]:
    rows = connection.execute(_distinct_values_statement(connection, table_name, column_name)).mappings()
    return [_stringify(row["value"]).upper() for row in rows if _stringify(row["value"])]


def _distinct_values_statement(connection: Connection, table_name: str, column_name: str) -> sa.TextClause:
    return text(
        f"""
        SELECT DISTINCT {_q(connection, column_name)} AS value
        FROM {_q(connection, table_name)}
        WHERE {_q(connection, column_name)} IS NOT NULL
        ORDER BY {_q(connection, column_name)}
        """
    )


def _fetch_observed_non_eur_currencies(connection: Connection) -> list[str]:
    rows = connection.execute(
        text(
            f"""
            SELECT DISTINCT UPPER({_q(connection, "TAXCCY")}) AS currency
            FROM {_q(connection, "TAXRPT")}
            WHERE {_q(connection, "TAXCCY")} IS NOT NULL
              AND UPPER({_q(connection, "TAXCCY")}) <> 'EUR'
            ORDER BY UPPER({_q(connection, "TAXCCY")})
            """
        )
    ).mappings()
    return [_stringify(row["currency"]).upper() for row in rows if _stringify(row["currency"])]


def _fetch_latest_fx_by_currency(
    connection: Connection,
    currencies: list[str],
) -> list[tuple[str, Any]]:
    rows = connection.execute(
        text(
            f"""
            SELECT {_q(connection, "REFCCY")} AS currency,
                   MAX({_q(connection, "REFDAT")}) AS latest_rate_date
            FROM {_q(connection, "REFEXC")}
            GROUP BY {_q(connection, "REFCCY")}
            """
        )
    ).mappings()
    latest_by_currency = {
        _stringify(row["currency"]).upper(): row["latest_rate_date"]
        for row in rows
        if _stringify(row["currency"])
    }
    return [(currency, latest_by_currency.get(currency)) for currency in currencies]


def _stale_fx_currencies(
    latest_fx_by_currency: list[tuple[str, Any]],
    *,
    threshold_days: int,
    check_date: date,
) -> list[tuple[str, str, str]]:
    stale_rows: list[tuple[str, str, str]] = []
    for currency, latest_rate_date in latest_fx_by_currency:
        normalized_date = _as_date(latest_rate_date)
        if normalized_date is None:
            stale_rows.append((currency, "", "missing REFEXC rate"))
            continue

        age_days = (check_date - normalized_date).days
        if age_days > threshold_days:
            stale_rows.append(
                (
                    currency,
                    normalized_date.isoformat(),
                    f"{age_days} calendar days old; threshold is {threshold_days}",
                )
            )
    return stale_rows


def _as_date(value: Any) -> date | None:
    if isinstance(value, date):
        return value
    if isinstance(value, str) and value:
        return date.fromisoformat(value[:10])
    return None


def _q(connection: Connection, identifier: str) -> str:
    return connection.dialect.identifier_preparer.quote(identifier)


def _format_single_column(header: str, values: list[Any]) -> str:
    return _format_rows((header,), [(value,) for value in values])


def _format_rows(headers: tuple[str, ...], rows: list[Any]) -> str:
    normalized_rows = [tuple(_stringify(value) for value in row) for row in rows]
    if not normalized_rows:
        return "(none)"
    widths = [
        max(len(headers[index]), *(len(row[index]) for row in normalized_rows))
        for index in range(len(headers))
    ]
    header_line = " | ".join(header.ljust(widths[index]) for index, header in enumerate(headers))
    divider = "-+-".join("-" * width for width in widths)
    row_lines = [
        " | ".join(row[index].ljust(widths[index]) for index in range(len(headers)))
        for row in normalized_rows
    ]
    return "\n".join([header_line, divider, *row_lines])


def _stringify(value: Any) -> str:
    if value is None:
        return ""
    text_value = str(value).replace("\r", " ").replace("\n", " ")
    return text_value[:157] + "..." if len(text_value) > 160 else text_value


def _compact_error(exc: BaseException) -> str:
    return exc.__class__.__name__


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Report read-only data readiness diagnostics.")
    parser.add_argument(
        "--threshold-days",
        type=int,
        default=DEFAULT_FX_FRESHNESS_DAYS,
        help="Maximum allowed age in calendar days for latest REFEXC rates.",
    )
    parser.add_argument(
        "--storage-path",
        type=Path,
        default=DEFAULT_STORAGE_PATH,
        help="Path to the configured/storage ISIN CSV.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    engine = create_readiness_engine()
    try:
        report, exit_code = build_readiness_report(
            engine,
            storage_path=args.storage_path,
            threshold_days=args.threshold_days,
        )
        sys.stdout.write(report)
        return exit_code
    except (RuntimeError, SQLAlchemyError) as exc:
        sys.stderr.write(f"Data readiness check failed: {_compact_error(exc)}: {exc}\n")
        return 2
    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
