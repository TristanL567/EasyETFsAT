from __future__ import annotations

import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

import sqlalchemy as sa
from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine, make_url
from sqlalchemy.exc import SQLAlchemyError

try:
    from fondant.config import get_settings
except ModuleNotFoundError:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from fondant.config import get_settings


SECTION_HEADINGS = (
    "== Snapshot Metadata ==",
    "== Schema Objects ==",
    "== Row Counts ==",
    "== Report Coverage By ISIN ==",
    "== FX Coverage By Currency ==",
    "== Recent Ingestion Runs ==",
    "== Recent Ingestion Errors ==",
    "== Alembic Revision ==",
)

REPORT_TABLE = "TAXRPT"
SOURCE_REPORT_TABLE = "SOURCERPT"
FX_TABLE = "REFEXC"
INGESTION_RUN_TABLE = "IMPLOG"
INGESTION_ERROR_TABLE = "IMPERR"
ALEMBIC_TABLE = "alembic_version"


def sync_database_url(database_url: str) -> str:
    return database_url.replace("+asyncpg", "+psycopg").replace("+aiosqlite", "+pysqlite")


def create_snapshot_engine(database_url: str | None = None) -> Engine:
    url = sync_database_url(database_url or get_settings().database_url)
    return sa.create_engine(url, future=True)


def build_snapshot(engine: Engine) -> str:
    with engine.connect() as connection:
        return render_snapshot(connection)


def render_snapshot(connection: Connection) -> str:
    inspector = sa.inspect(connection)
    tables = sorted(inspector.get_table_names(), key=str.casefold)
    views = sorted(inspector.get_view_names(), key=str.casefold)
    table_set = set(tables)

    sections = [
        _section("Snapshot Metadata", _snapshot_metadata(connection)),
        _section("Schema Objects", _schema_objects(tables, views)),
        _section("Row Counts", _row_counts(connection, tables, views)),
        _section("Report Coverage By ISIN", _report_coverage(connection, table_set)),
        _section("FX Coverage By Currency", _fx_coverage(connection, table_set)),
        _section("Recent Ingestion Runs", _recent_ingestion_runs(connection, table_set)),
        _section("Recent Ingestion Errors", _recent_ingestion_errors(connection, table_set)),
        _section("Alembic Revision", _alembic_revision(connection, table_set)),
    ]
    return "\n\n".join(sections) + "\n"


def _section(title: str, body: str) -> str:
    return f"== {title} ==\n{body}"


def _snapshot_metadata(connection: Connection) -> str:
    safe_url = make_url(str(connection.engine.url)).render_as_string(hide_password=True)
    return _format_rows(
        ("key", "value"),
        [
            ("database_url", safe_url),
            ("dialect", connection.dialect.name),
            ("generated_at", _database_now(connection)),
        ],
    )


def _database_now(connection: Connection) -> str:
    expression = "CURRENT_TIMESTAMP"
    row = connection.execute(text(f"SELECT {expression} AS generated_at")).mappings().one()
    return _stringify(row["generated_at"])


def _schema_objects(tables: list[str], views: list[str]) -> str:
    rows = [("table", table) for table in tables]
    rows.extend(("view", view) for view in views)
    return _format_rows(("type", "name"), rows)


def _row_counts(connection: Connection, tables: list[str], views: list[str]) -> str:
    rows: list[tuple[str, str, str]] = []
    for object_type, names in (("table", tables), ("view", views)):
        for name in names:
            try:
                count = connection.execute(
                    text(f"SELECT COUNT(*) AS row_count FROM {_quote_identifier(connection, name)}")
                ).scalar_one()
                rows.append((object_type, name, str(count)))
            except SQLAlchemyError as exc:
                rows.append((object_type, name, f"unavailable: {_compact_error(exc)}"))
    return _format_rows(("type", "name", "rows"), rows)


def _report_coverage(connection: Connection, table_set: set[str]) -> str:
    coverage: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "source_reports": 0,
            "tax_reports": 0,
            "min_report_year": "",
            "max_report_year": "",
            "latest_report_date": "",
        }
    )

    if SOURCE_REPORT_TABLE in table_set:
        for row in connection.execute(
            text(
                f"""
                SELECT SRCISN AS isin,
                       COUNT(*) AS source_reports,
                       MIN(SRCYEA) AS min_report_year,
                       MAX(SRCYEA) AS max_report_year,
                       MAX(SRCMDT) AS latest_report_date
                FROM {_quote_identifier(connection, SOURCE_REPORT_TABLE)}
                GROUP BY SRCISN
                ORDER BY SRCISN
                """
            )
        ).mappings():
            item = coverage[_stringify(row["isin"])]
            item["source_reports"] = row["source_reports"]
            item["min_report_year"] = _min_present(item["min_report_year"], row["min_report_year"])
            item["max_report_year"] = _max_present(item["max_report_year"], row["max_report_year"])
            item["latest_report_date"] = _max_present(
                item["latest_report_date"],
                row["latest_report_date"],
            )

    if REPORT_TABLE in table_set:
        for row in connection.execute(
            text(
                f"""
                SELECT TAXISN AS isin,
                       COUNT(*) AS tax_reports,
                       MIN(TAXYEA) AS min_report_year,
                       MAX(TAXYEA) AS max_report_year,
                       MAX(TAXMDT) AS latest_report_date
                FROM {_quote_identifier(connection, REPORT_TABLE)}
                GROUP BY TAXISN
                ORDER BY TAXISN
                """
            )
        ).mappings():
            item = coverage[_stringify(row["isin"])]
            item["tax_reports"] = row["tax_reports"]
            item["min_report_year"] = _min_present(item["min_report_year"], row["min_report_year"])
            item["max_report_year"] = _max_present(item["max_report_year"], row["max_report_year"])
            item["latest_report_date"] = _max_present(
                item["latest_report_date"],
                row["latest_report_date"],
            )

    if SOURCE_REPORT_TABLE not in table_set and REPORT_TABLE not in table_set:
        return "(report tables missing: SOURCERPT, TAXRPT)"

    rows = [
        (
            isin,
            item["source_reports"],
            item["tax_reports"],
            item["min_report_year"],
            item["max_report_year"],
            item["latest_report_date"],
        )
        for isin, item in sorted(coverage.items(), key=lambda pair: pair[0])
    ]
    return _format_rows(
        (
            "isin",
            "source_reports",
            "tax_reports",
            "min_report_year",
            "max_report_year",
            "latest_report_date",
        ),
        rows,
    )


def _fx_coverage(connection: Connection, table_set: set[str]) -> str:
    if FX_TABLE not in table_set:
        return f"(table missing: {FX_TABLE})"

    rows = connection.execute(
        text(
            f"""
            SELECT REFCCY AS currency,
                   COUNT(*) AS rates,
                   MIN(REFDAT) AS first_rate_date,
                   MAX(REFDAT) AS latest_rate_date
            FROM {_quote_identifier(connection, FX_TABLE)}
            GROUP BY REFCCY
            ORDER BY REFCCY
            """
        )
    ).all()
    return _format_rows(("currency", "rates", "first_rate_date", "latest_rate_date"), rows)


def _recent_ingestion_runs(connection: Connection, table_set: set[str]) -> str:
    if INGESTION_RUN_TABLE not in table_set:
        return f"(table missing: {INGESTION_RUN_TABLE})"

    rows = connection.execute(
        text(
            f"""
            SELECT IMPRUNIDN AS run_id,
                   IMPISN AS isin,
                   IMPSTS AS status,
                   IMPRSN AS records_seen,
                   IMPRSW AS records_written,
                   IMPSTADTS AS started_at,
                   IMPFINDTS AS finished_at,
                   IMPMSG AS message
            FROM {_quote_identifier(connection, INGESTION_RUN_TABLE)}
            ORDER BY IMPSTADTS DESC, IMPISN ASC
            LIMIT 20
            """
        )
    ).all()
    return _format_rows(
        (
            "run_id",
            "isin",
            "status",
            "records_seen",
            "records_written",
            "started_at",
            "finished_at",
            "message",
        ),
        rows,
    )


def _recent_ingestion_errors(connection: Connection, table_set: set[str]) -> str:
    if INGESTION_ERROR_TABLE not in table_set:
        return f"(table missing: {INGESTION_ERROR_TABLE})"

    rows = connection.execute(
        text(
            f"""
            SELECT IMPSTG AS stage,
                   COALESCE(IMPECD, '') AS error_code,
                   COUNT(*) AS errors,
                   MAX(IMPCRTDTS) AS latest_error_at,
                   MIN(IMPEMS) AS sample_message
            FROM {_quote_identifier(connection, INGESTION_ERROR_TABLE)}
            GROUP BY IMPSTG, IMPECD
            ORDER BY latest_error_at DESC, stage ASC, error_code ASC
            LIMIT 20
            """
        )
    ).all()
    return _format_rows(("stage", "error_code", "errors", "latest_error_at", "sample_message"), rows)


def _alembic_revision(connection: Connection, table_set: set[str]) -> str:
    if ALEMBIC_TABLE not in table_set:
        return f"(table missing: {ALEMBIC_TABLE})"

    rows = connection.execute(
        text(
            f"""
            SELECT version_num
            FROM {_quote_identifier(connection, ALEMBIC_TABLE)}
            ORDER BY version_num
            """
        )
    ).all()
    return _format_rows(("version_num",), rows)


def _quote_identifier(connection: Connection, name: str) -> str:
    return connection.dialect.identifier_preparer.quote(name)


def _format_rows(headers: tuple[str, ...], rows: list[Any]) -> str:
    normalized_rows = [tuple(_stringify(value) for value in row) for row in rows]
    if not normalized_rows:
        return _format_table(headers, [tuple("(none)" if index == 0 else "" for index, _ in enumerate(headers))])
    return _format_table(headers, normalized_rows)


def _format_table(headers: tuple[str, ...], rows: list[tuple[str, ...]]) -> str:
    widths = [
        max(len(headers[index]), *(len(row[index]) for row in rows))
        for index in range(len(headers))
    ]
    header_line = " | ".join(header.ljust(widths[index]) for index, header in enumerate(headers))
    divider = "-+-".join("-" * width for width in widths)
    row_lines = [
        " | ".join(row[index].ljust(widths[index]) for index in range(len(headers)))
        for row in rows
    ]
    return "\n".join([header_line, divider, *row_lines])


def _stringify(value: Any) -> str:
    if value is None:
        return ""
    text_value = str(value).replace("\r", " ").replace("\n", " ")
    return text_value[:157] + "..." if len(text_value) > 160 else text_value


def _min_present(left: Any, right: Any) -> str:
    values = [value for value in (_stringify(left), _stringify(right)) if value]
    return min(values) if values else ""


def _max_present(left: Any, right: Any) -> str:
    values = [value for value in (_stringify(left), _stringify(right)) if value]
    return max(values) if values else ""


def _compact_error(exc: SQLAlchemyError) -> str:
    return exc.__class__.__name__


def main() -> int:
    engine = create_snapshot_engine()
    try:
        sys.stdout.write(build_snapshot(engine))
    except SQLAlchemyError as exc:
        sys.stderr.write(f"Database snapshot failed: {_compact_error(exc)}\n")
        return 2
    finally:
        engine.dispose()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
