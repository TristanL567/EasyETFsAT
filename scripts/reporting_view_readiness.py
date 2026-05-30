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


REQUIRED_TABLES = frozenset({"TAXRPT", "REFEXC"})
REQUIRED_VIEWS = frozenset({"V1_TAXDATPRE", "V2_TAXDATEUR"})
KEY_COLUMNS = ("TAXISN", "TAXOKBIDN", "TAXYEA", "FNDCCY")
SELECTED_VALUE_COLUMNS = (
    "K61PVM",
    "K61PVO",
    "K61BVM",
    "K61BVO",
    "K61BVJ",
    "K61STI",
    "K62PVM",
    "K62PVO",
    "K62BVM",
    "K62BVO",
    "K62BVJ",
    "K62STI",
    "K40PVM",
    "K40PVO",
    "K40BVM",
    "K40BVO",
    "K40BVJ",
    "K40STI",
)


@dataclass(frozen=True)
class ReportingViewReadiness:
    row_counts: list[tuple[str, int]]
    reports_missing_v1: list[tuple[Any, ...]]
    v1_missing_v2: list[tuple[Any, ...]]
    missing_exact_fx: list[tuple[Any, ...]]
    invalid_exact_fx: list[tuple[Any, ...]]
    v1_null_counts: list[tuple[str, int]]
    v2_null_counts: list[tuple[str, int]]
    v2_null_reasons: list[tuple[str, int, int, int]]


def sync_database_url(database_url: str) -> str:
    return database_url.replace("+asyncpg", "+psycopg").replace("+aiosqlite", "+pysqlite")


def create_readiness_engine(database_url: str | None = None) -> Engine:
    url = sync_database_url(database_url or get_settings().database_url)
    return sa.create_engine(url, future=True)


def build_readiness_report(engine: Engine) -> tuple[str, int]:
    with engine.connect() as connection:
        readiness = inspect_reporting_view_readiness(connection)
    return render_readiness(readiness), 0


def inspect_reporting_view_readiness(connection: Connection) -> ReportingViewReadiness:
    _assert_required_schema(connection)
    return ReportingViewReadiness(
        row_counts=_row_counts(connection),
        reports_missing_v1=_reports_missing_v1(connection),
        v1_missing_v2=_v1_missing_v2(connection),
        missing_exact_fx=_missing_exact_fx(connection),
        invalid_exact_fx=_invalid_exact_fx(connection),
        v1_null_counts=_null_counts(connection, "V1_TAXDATPRE"),
        v2_null_counts=_null_counts(connection, "V2_TAXDATEUR"),
        v2_null_reasons=_v2_null_reasons(connection),
    )


def render_readiness(readiness: ReportingViewReadiness) -> str:
    lines = [
        "== Reporting View Readiness Summary ==",
        _format_rows(("object", "rows"), readiness.row_counts),
        "",
        "== TAXRPT Reports Missing From V1_TAXDATPRE ==",
        _format_rows(("TAXISN", "TAXOKBIDN", "TAXYEA", "FNDCCY"), readiness.reports_missing_v1),
        "",
        "== V1_TAXDATPRE Rows Missing From V2_TAXDATEUR ==",
        _format_rows(("TAXISN", "TAXOKBIDN", "TAXYEA", "FNDCCY"), readiness.v1_missing_v2),
        "",
        "== Non-EUR TAXRPT Date/Currency Missing Exact-Date REFEXC ==",
        _format_rows(("FNDCCY", "TAXMDT", "reports"), readiness.missing_exact_fx),
        "",
        "== Non-EUR Exact-Date REFEXC With Null Or Zero REFRAT ==",
        _format_rows(("FNDCCY", "TAXMDT", "REFRAT", "reports"), readiness.invalid_exact_fx),
        "",
        "== V1_TAXDATPRE Selected K40/K61/K62 Null Counts ==",
        _format_rows(("column", "null_values"), readiness.v1_null_counts),
        "",
        "== V2_TAXDATEUR Selected K40/K61/K62 Null Counts ==",
        _format_rows(("column", "null_values"), readiness.v2_null_counts),
        "",
        "== V2_TAXDATEUR Null Converted Value Reasons ==",
        _format_rows(
            ("column", "source_null_or_missing", "missing_exact_fx", "zero_exact_fx"),
            readiness.v2_null_reasons,
        ),
        "",
        "Read-only: SELECT/introspection only; no OeKB or ECB fetches; no row mutations.",
    ]
    return "\n".join(lines) + "\n"


def _assert_required_schema(connection: Connection) -> None:
    inspector = sa.inspect(connection)
    table_set = set(inspector.get_table_names())
    view_set = set(inspector.get_view_names())
    missing_tables = sorted(REQUIRED_TABLES - table_set)
    missing_views = sorted(REQUIRED_VIEWS - view_set)
    if missing_tables or missing_views:
        detail = []
        if missing_tables:
            detail.append(f"tables: {', '.join(missing_tables)}")
        if missing_views:
            detail.append(f"views: {', '.join(missing_views)}")
        raise RuntimeError(f"Missing required reporting readiness schema ({'; '.join(detail)})")


def _row_counts(connection: Connection) -> list[tuple[str, int]]:
    return [
        (object_name, _count_rows(connection, object_name))
        for object_name in ("TAXRPT", "V1_TAXDATPRE", "V2_TAXDATEUR")
    ]


def _count_rows(connection: Connection, object_name: str) -> int:
    row = connection.execute(
        text(f"SELECT COUNT(*) AS row_count FROM {_q(connection, object_name)}")
    ).mappings().one()
    return int(row["row_count"])


def _reports_missing_v1(connection: Connection) -> list[tuple[Any, ...]]:
    rows = connection.execute(
        text(
            f"""
            SELECT r.{_q(connection, "TAXISN")},
                   r.{_q(connection, "TAXOKBIDN")},
                   r.{_q(connection, "TAXYEA")},
                   r.{_q(connection, "TAXCCY")} AS {_q(connection, "FNDCCY")}
            FROM {_q(connection, "TAXRPT")} AS r
            LEFT JOIN {_q(connection, "V1_TAXDATPRE")} AS v
              ON v.{_q(connection, "TAXISN")} = r.{_q(connection, "TAXISN")}
             AND v.{_q(connection, "TAXOKBIDN")} = r.{_q(connection, "TAXOKBIDN")}
             AND v.{_q(connection, "TAXYEA")} = r.{_q(connection, "TAXYEA")}
             AND v.{_q(connection, "FNDCCY")} = r.{_q(connection, "TAXCCY")}
            WHERE v.{_q(connection, "TAXOKBIDN")} IS NULL
            ORDER BY r.{_q(connection, "TAXISN")}, r.{_q(connection, "TAXOKBIDN")}
            LIMIT 100
            """
        )
    ).all()
    return [tuple(row) for row in rows]


def _v1_missing_v2(connection: Connection) -> list[tuple[Any, ...]]:
    join_predicate = " AND ".join(
        f"e.{_q(connection, column)} = v.{_q(connection, column)}" for column in KEY_COLUMNS
    )
    rows = connection.execute(
        text(
            f"""
            SELECT v.{_q(connection, "TAXISN")},
                   v.{_q(connection, "TAXOKBIDN")},
                   v.{_q(connection, "TAXYEA")},
                   v.{_q(connection, "FNDCCY")}
            FROM {_q(connection, "V1_TAXDATPRE")} AS v
            LEFT JOIN {_q(connection, "V2_TAXDATEUR")} AS e
              ON {join_predicate}
            WHERE e.{_q(connection, "TAXOKBIDN")} IS NULL
            ORDER BY v.{_q(connection, "TAXISN")}, v.{_q(connection, "TAXOKBIDN")}
            LIMIT 100
            """
        )
    ).all()
    return [tuple(row) for row in rows]


def _missing_exact_fx(connection: Connection) -> list[tuple[Any, ...]]:
    rows = connection.execute(
        text(
            f"""
            SELECT UPPER(r.{_q(connection, "TAXCCY")}) AS {_q(connection, "FNDCCY")},
                   r.{_q(connection, "TAXMDT")},
                   COUNT(*) AS reports
            FROM {_q(connection, "TAXRPT")} AS r
            LEFT JOIN {_q(connection, "REFEXC")} AS fx
              ON fx.{_q(connection, "REFCCY")} = r.{_q(connection, "TAXCCY")}
             AND fx.{_q(connection, "REFDAT")} = r.{_q(connection, "TAXMDT")}
            WHERE r.{_q(connection, "TAXCCY")} IS NOT NULL
              AND UPPER(r.{_q(connection, "TAXCCY")}) <> 'EUR'
              AND r.{_q(connection, "TAXMDT")} IS NOT NULL
              AND fx.{_q(connection, "REFCCY")} IS NULL
            GROUP BY UPPER(r.{_q(connection, "TAXCCY")}), r.{_q(connection, "TAXMDT")}
            ORDER BY UPPER(r.{_q(connection, "TAXCCY")}), r.{_q(connection, "TAXMDT")}
            LIMIT 100
            """
        )
    ).all()
    return [tuple(row) for row in rows]


def _invalid_exact_fx(connection: Connection) -> list[tuple[Any, ...]]:
    rows = connection.execute(
        text(
            f"""
            SELECT UPPER(r.{_q(connection, "TAXCCY")}) AS {_q(connection, "FNDCCY")},
                   r.{_q(connection, "TAXMDT")},
                   fx.{_q(connection, "REFRAT")},
                   COUNT(*) AS reports
            FROM {_q(connection, "TAXRPT")} AS r
            JOIN {_q(connection, "REFEXC")} AS fx
              ON fx.{_q(connection, "REFCCY")} = r.{_q(connection, "TAXCCY")}
             AND fx.{_q(connection, "REFDAT")} = r.{_q(connection, "TAXMDT")}
            WHERE r.{_q(connection, "TAXCCY")} IS NOT NULL
              AND UPPER(r.{_q(connection, "TAXCCY")}) <> 'EUR'
              AND (fx.{_q(connection, "REFRAT")} IS NULL OR fx.{_q(connection, "REFRAT")} = 0)
            GROUP BY UPPER(r.{_q(connection, "TAXCCY")}), r.{_q(connection, "TAXMDT")}, fx.{_q(connection, "REFRAT")}
            ORDER BY UPPER(r.{_q(connection, "TAXCCY")}), r.{_q(connection, "TAXMDT")}
            LIMIT 100
            """
        )
    ).all()
    return [tuple(row) for row in rows]


def _null_counts(connection: Connection, object_name: str) -> list[tuple[str, int]]:
    row = connection.execute(_null_counts_statement(connection, object_name)).mappings().one()
    return [(column, int(row[column] or 0)) for column in SELECTED_VALUE_COLUMNS]


def _null_counts_statement(connection: Connection, object_name: str) -> sa.TextClause:
    select_parts = [
        f"SUM(CASE WHEN {_q(connection, column)} IS NULL THEN 1 ELSE 0 END) AS {_q(connection, column)}"
        for column in SELECTED_VALUE_COLUMNS
    ]
    return text(f"SELECT {', '.join(select_parts)} FROM {_q(connection, object_name)}")


def _v2_null_reasons(connection: Connection) -> list[tuple[str, int, int, int]]:
    rows: list[tuple[str, int, int, int]] = []
    join_predicate = " AND ".join(
        f"e.{_q(connection, column)} = v.{_q(connection, column)}" for column in KEY_COLUMNS
    )
    for column in SELECTED_VALUE_COLUMNS:
        row = connection.execute(
            text(
                f"""
                SELECT
                    SUM(CASE
                        WHEN e.{_q(connection, column)} IS NULL
                         AND v.{_q(connection, column)} IS NULL
                        THEN 1 ELSE 0
                    END) AS source_null_or_missing,
                    SUM(CASE
                        WHEN e.{_q(connection, column)} IS NULL
                         AND v.{_q(connection, column)} IS NOT NULL
                         AND e.{_q(connection, "FXRAT")} IS NULL
                        THEN 1 ELSE 0
                    END) AS missing_exact_fx,
                    SUM(CASE
                        WHEN e.{_q(connection, column)} IS NULL
                         AND v.{_q(connection, column)} IS NOT NULL
                         AND e.{_q(connection, "FXRAT")} = 0
                        THEN 1 ELSE 0
                    END) AS zero_exact_fx
                FROM {_q(connection, "V2_TAXDATEUR")} AS e
                LEFT JOIN {_q(connection, "V1_TAXDATPRE")} AS v
                  ON {join_predicate}
                """
            )
        ).mappings().one()
        rows.append(
            (
                column,
                int(row["source_null_or_missing"] or 0),
                int(row["missing_exact_fx"] or 0),
                int(row["zero_exact_fx"] or 0),
            )
        )
    return rows


def _q(connection: Connection, identifier: str) -> str:
    return connection.dialect.identifier_preparer.quote(identifier)


def _format_rows(headers: tuple[str, ...], rows: list[Any]) -> str:
    normalized_rows = [tuple(_stringify(value) for value in row) for row in rows]
    if not normalized_rows:
        normalized_rows = [tuple("(none)" if index == 0 else "" for index, _ in enumerate(headers))]
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


def main() -> int:
    engine = create_readiness_engine()
    try:
        report, exit_code = build_readiness_report(engine)
        sys.stdout.write(report)
        return exit_code
    except (RuntimeError, SQLAlchemyError) as exc:
        sys.stderr.write(f"Reporting view readiness check failed: {_compact_error(exc)}: {exc}\n")
        return 2
    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
