from __future__ import annotations

import sys
from dataclasses import dataclass
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


SECTION_HEADINGS = (
    "== Source To Curated Report Alignment ==",
    "== TAXDAT Parent Integrity ==",
    "== TAXADJ Parent Integrity ==",
    "== TAXDAT Tax Line Dictionary Integrity ==",
    "== TAXDAT Tax Category Dictionary Integrity ==",
    "== Schema Diagnostics ==",
)

REQUIRED_TABLES = (
    "SOURCERPT",
    "SOURCERAW",
    "SOURCEAGE",
    "TAXRPT",
    "TAXDAT",
    "TAXADJ",
    "TAXLIN",
    "TAXCAT",
)


@dataclass(frozen=True)
class CheckResult:
    title: str
    passed: bool
    headers: tuple[str, ...]
    rows: list[tuple[Any, ...]]
    detail: str = ""


def sync_database_url(database_url: str) -> str:
    return database_url.replace("+asyncpg", "+psycopg").replace("+aiosqlite", "+pysqlite")


def create_consistency_engine(database_url: str | None = None) -> Engine:
    url = sync_database_url(database_url or get_settings().database_url)
    return sa.create_engine(url, future=True)


def build_consistency_report(engine: Engine) -> tuple[str, bool]:
    with engine.connect() as connection:
        return render_consistency_report(connection)


def render_consistency_report(connection: Connection) -> tuple[str, bool]:
    results = run_consistency_checks(connection)
    passed = all(result.passed for result in results)
    return "\n\n".join(_render_result(result) for result in results) + "\n", passed


def run_consistency_checks(connection: Connection) -> list[CheckResult]:
    inspector = sa.inspect(connection)
    table_set = set(inspector.get_table_names())
    columns_by_table = {
        table_name: {column["name"] for column in inspector.get_columns(table_name)}
        for table_name in table_set
        if table_name in REQUIRED_TABLES
    }
    missing_tables = sorted(set(REQUIRED_TABLES) - table_set)

    if missing_tables:
        return [
            CheckResult(
                title="Schema Diagnostics",
                passed=False,
                headers=("object_type", "name", "diagnostic"),
                rows=[("table", table_name, "missing required table") for table_name in missing_tables],
                detail="Required source/curated tables are missing; no consistency queries were run.",
            )
        ]

    return [
        _source_to_curated_alignment(connection),
        _taxdat_parent_integrity(connection),
        _taxadj_parent_integrity(connection),
        _taxdat_taxlin_integrity(connection),
        _taxdat_taxcat_integrity(connection, columns_by_table["TAXCAT"]),
        CheckResult(
            title="Schema Diagnostics",
            passed=True,
            headers=("object_type", "name", "diagnostic"),
            rows=[],
            detail="Required tables are present. Queries are SELECT/introspection only.",
        ),
    ]


def _source_to_curated_alignment(connection: Connection) -> CheckResult:
    rows = connection.execute(
        text(
            f"""
            SELECT s.{_q(connection, "SRCISN")} AS isin,
                   s.{_q(connection, "SRCOKBIDN")} AS oekb_report_id,
                   s.{_q(connection, "SRCSTS")} AS source_status,
                   s.{_q(connection, "SRCVRN")} AS source_version
            FROM {_qt(connection, "SOURCERPT")} AS s
            JOIN {_qt(connection, "SOURCERAW")} AS raw
              ON raw.{_q(connection, "SRCISN")} = s.{_q(connection, "SRCISN")}
             AND raw.{_q(connection, "SRCOKBIDN")} = s.{_q(connection, "SRCOKBIDN")}
            JOIN {_qt(connection, "SOURCEAGE")} AS age
              ON age.{_q(connection, "SRCISN")} = s.{_q(connection, "SRCISN")}
             AND age.{_q(connection, "SRCOKBIDN")} = s.{_q(connection, "SRCOKBIDN")}
            LEFT JOIN {_qt(connection, "TAXRPT")} AS tax
              ON tax.{_q(connection, "TAXISN")} = s.{_q(connection, "SRCISN")}
             AND tax.{_q(connection, "TAXOKBIDN")} = s.{_q(connection, "SRCOKBIDN")}
            WHERE s.{_q(connection, "SRCSTS")} = 'FIN'
              AND tax.{_q(connection, "TAXIDN")} IS NULL
            ORDER BY s.{_q(connection, "SRCISN")}, s.{_q(connection, "SRCOKBIDN")}
            """
        )
    ).all()
    return CheckResult(
        title="Source To Curated Report Alignment",
        passed=not rows,
        headers=("isin", "oekb_report_id", "source_status", "source_version"),
        rows=rows,
        detail="FIN source reports with matching SOURCERAW and SOURCEAGE must have a TAXRPT.",
    )


def _taxdat_parent_integrity(connection: Connection) -> CheckResult:
    rows = connection.execute(
        text(
            f"""
            SELECT d.{_q(connection, "TAXIDN")} AS taxdat_id,
                   d.{_q(connection, "TAXRPTIDN")} AS expected_taxrpt_id,
                   d.{_q(connection, "TAXOKBIDN")} AS oekb_report_id,
                   COALESCE(by_id.{_q(connection, "TAXISN")}, by_okb.{_q(connection, "TAXISN")}) AS isin
            FROM {_qt(connection, "TAXDAT")} AS d
            LEFT JOIN {_qt(connection, "TAXRPT")} AS exact
              ON exact.{_q(connection, "TAXIDN")} = d.{_q(connection, "TAXRPTIDN")}
             AND exact.{_q(connection, "TAXOKBIDN")} = d.{_q(connection, "TAXOKBIDN")}
            LEFT JOIN {_qt(connection, "TAXRPT")} AS by_id
              ON by_id.{_q(connection, "TAXIDN")} = d.{_q(connection, "TAXRPTIDN")}
            LEFT JOIN {_qt(connection, "TAXRPT")} AS by_okb
              ON by_okb.{_q(connection, "TAXOKBIDN")} = d.{_q(connection, "TAXOKBIDN")}
            WHERE exact.{_q(connection, "TAXIDN")} IS NULL
            ORDER BY d.{_q(connection, "TAXOKBIDN")}, d.{_q(connection, "TAXIDN")}
            """
        )
    ).all()
    return CheckResult(
        title="TAXDAT Parent Integrity",
        passed=not rows,
        headers=("taxdat_id", "expected_taxrpt_id", "oekb_report_id", "isin"),
        rows=rows,
        detail="Every TAXDAT row must resolve to TAXRPT by TAXRPTIDN and TAXOKBIDN.",
    )


def _taxadj_parent_integrity(connection: Connection) -> CheckResult:
    rows = connection.execute(
        text(
            f"""
            SELECT a.{_q(connection, "TAXIDN")} AS taxadj_id,
                   a.{_q(connection, "TAXRPTIDN")} AS expected_taxrpt_id,
                   a.{_q(connection, "TAXOKBIDN")} AS oekb_report_id,
                   a.{_q(connection, "TAXCOD")} AS adjustment_code,
                   COALESCE(by_id.{_q(connection, "TAXISN")}, by_okb.{_q(connection, "TAXISN")}) AS isin
            FROM {_qt(connection, "TAXADJ")} AS a
            LEFT JOIN {_qt(connection, "TAXRPT")} AS exact
              ON exact.{_q(connection, "TAXIDN")} = a.{_q(connection, "TAXRPTIDN")}
             AND exact.{_q(connection, "TAXOKBIDN")} = a.{_q(connection, "TAXOKBIDN")}
            LEFT JOIN {_qt(connection, "TAXRPT")} AS by_id
              ON by_id.{_q(connection, "TAXIDN")} = a.{_q(connection, "TAXRPTIDN")}
            LEFT JOIN {_qt(connection, "TAXRPT")} AS by_okb
              ON by_okb.{_q(connection, "TAXOKBIDN")} = a.{_q(connection, "TAXOKBIDN")}
            WHERE exact.{_q(connection, "TAXIDN")} IS NULL
            ORDER BY a.{_q(connection, "TAXOKBIDN")}, a.{_q(connection, "TAXIDN")}
            """
        )
    ).all()
    return CheckResult(
        title="TAXADJ Parent Integrity",
        passed=not rows,
        headers=("taxadj_id", "expected_taxrpt_id", "oekb_report_id", "adjustment_code", "isin"),
        rows=rows,
        detail="Every TAXADJ row must resolve to TAXRPT by TAXRPTIDN and TAXOKBIDN.",
    )


def _taxdat_taxlin_integrity(connection: Connection) -> CheckResult:
    rows = connection.execute(
        text(
            f"""
            SELECT d.{_q(connection, "TAXIDN")} AS taxdat_id,
                   r.{_q(connection, "TAXISN")} AS isin,
                   d.{_q(connection, "TAXOKBIDN")} AS oekb_report_id,
                   d.{_q(connection, "TAXLINIDN")} AS taxlin_id,
                   lin.{_q(connection, "TAXCOD")} AS tax_line_code,
                   lin.{_q(connection, "TAXACT")} AS tax_line_active
            FROM {_qt(connection, "TAXDAT")} AS d
            LEFT JOIN {_qt(connection, "TAXRPT")} AS r
              ON r.{_q(connection, "TAXIDN")} = d.{_q(connection, "TAXRPTIDN")}
             AND r.{_q(connection, "TAXOKBIDN")} = d.{_q(connection, "TAXOKBIDN")}
            LEFT JOIN {_qt(connection, "TAXLIN")} AS lin
              ON lin.{_q(connection, "TAXIDN")} = d.{_q(connection, "TAXLINIDN")}
            WHERE lin.{_q(connection, "TAXIDN")} IS NULL
               OR lin.{_q(connection, "TAXACT")} IS NOT TRUE
            ORDER BY d.{_q(connection, "TAXOKBIDN")}, d.{_q(connection, "TAXIDN")}
            """
        )
    ).all()
    return CheckResult(
        title="TAXDAT Tax Line Dictionary Integrity",
        passed=not rows,
        headers=("taxdat_id", "isin", "oekb_report_id", "taxlin_id", "tax_line_code", "tax_line_active"),
        rows=rows,
        detail="Every TAXDAT.TAXLINIDN must resolve to an active TAXLIN row.",
    )


def _taxdat_taxcat_integrity(connection: Connection, taxcat_columns: set[str]) -> CheckResult:
    active_predicate = ""
    active_select = "1 AS tax_category_active"
    detail = "Every TAXDAT.TAXCATIDN must resolve to a TAXCAT row."
    headers = ("taxdat_id", "isin", "oekb_report_id", "taxcat_id", "tax_category_code")

    if "TAXACT" in taxcat_columns:
        active_predicate = f" OR cat.{_q(connection, 'TAXACT')} IS NOT TRUE"
        active_select = f"cat.{_q(connection, 'TAXACT')} AS tax_category_active"
        detail = "Every TAXDAT.TAXCATIDN must resolve to an active TAXCAT row."
        headers = (*headers, "tax_category_active")
    else:
        detail = (
            "Every TAXDAT.TAXCATIDN must resolve to a TAXCAT row. Current schema has no "
            "TAXCAT.TAXACT column, so existing TAXCAT rows are the active category dictionary."
        )

    rows = connection.execute(
        text(
            f"""
            SELECT d.{_q(connection, "TAXIDN")} AS taxdat_id,
                   r.{_q(connection, "TAXISN")} AS isin,
                   d.{_q(connection, "TAXOKBIDN")} AS oekb_report_id,
                   d.{_q(connection, "TAXCATIDN")} AS taxcat_id,
                   cat.{_q(connection, "TAXCOD")} AS tax_category_code,
                   {active_select}
            FROM {_qt(connection, "TAXDAT")} AS d
            LEFT JOIN {_qt(connection, "TAXRPT")} AS r
              ON r.{_q(connection, "TAXIDN")} = d.{_q(connection, "TAXRPTIDN")}
             AND r.{_q(connection, "TAXOKBIDN")} = d.{_q(connection, "TAXOKBIDN")}
            LEFT JOIN {_qt(connection, "TAXCAT")} AS cat
              ON cat.{_q(connection, "TAXIDN")} = d.{_q(connection, "TAXCATIDN")}
            WHERE cat.{_q(connection, "TAXIDN")} IS NULL
                  {active_predicate}
            ORDER BY d.{_q(connection, "TAXOKBIDN")}, d.{_q(connection, "TAXIDN")}
            """
        )
    ).all()
    return CheckResult(
        title="TAXDAT Tax Category Dictionary Integrity",
        passed=not rows,
        headers=headers,
        rows=rows,
        detail=detail,
    )


def _render_result(result: CheckResult) -> str:
    status = "PASS" if result.passed else "FAIL"
    body = _format_rows(result.headers, result.rows) if result.rows else "(none)"
    detail = f"\n{result.detail}" if result.detail else ""
    return f"== {result.title} ==\nstatus: {status}{detail}\n{body}"


def _qt(connection: Connection, table_name: str) -> str:
    return _q(connection, table_name)


def _q(connection: Connection, identifier: str) -> str:
    return connection.dialect.identifier_preparer.quote(identifier)


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


def _compact_error(exc: SQLAlchemyError) -> str:
    return exc.__class__.__name__


def main() -> int:
    engine = create_consistency_engine()
    try:
        report, passed = build_consistency_report(engine)
        sys.stdout.write(report)
        return 0 if passed else 1
    except SQLAlchemyError as exc:
        sys.stderr.write(f"Database consistency checks failed: {_compact_error(exc)}\n")
        return 2
    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
