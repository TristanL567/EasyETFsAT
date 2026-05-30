from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import sqlalchemy as sa
from sqlalchemy import bindparam, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql.elements import TextClause

try:
    from fondant.config import get_settings
except ModuleNotFoundError:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from fondant.config import get_settings


CLASSIFICATIONS = (
    "healthy_noop_refresh",
    "successful_update",
    "isolated_failure",
    "systemic_failure",
    "no_recent_runs",
    "schema_unavailable",
)

INGESTION_RUN_TABLE = "IMPLOG"
INGESTION_ERROR_TABLE = "IMPERR"
DEFAULT_RUN_LIMIT = 20
SYSTEMIC_FAILURE_RATIO = 0.8
CONNECTION_ERROR_MARKERS = (
    "connection",
    "connect",
    "timeout",
    "timed out",
    "client",
    "network",
    "http",
    "ssl",
    "dns",
    "refused",
    "unavailable",
)


@dataclass(frozen=True)
class RunSummary:
    run_id: str
    isin: str
    status: str
    records_seen: int
    records_written: int
    started_at: Any
    finished_at: Any
    message: str


@dataclass(frozen=True)
class ErrorGroup:
    stage: str
    error_code: str
    errors: int
    affected_runs: int
    latest_error_at: Any
    sample_message: str


@dataclass(frozen=True)
class HealthSummary:
    classification: str
    inspected_runs: int
    success_count: int
    failure_count: int
    records_seen_total: int
    records_written_total: int
    error_groups: list[ErrorGroup]
    detail: str


def sync_database_url(database_url: str) -> str:
    return database_url.replace("+asyncpg", "+psycopg").replace("+aiosqlite", "+pysqlite")


def create_health_engine(database_url: str | None = None) -> Engine:
    url = sync_database_url(database_url or get_settings().database_url)
    return sa.create_engine(url, future=True)


def build_health_report(engine: Engine, *, run_limit: int = DEFAULT_RUN_LIMIT) -> tuple[str, int]:
    with engine.connect() as connection:
        summary = inspect_ingestion_health(connection, run_limit=run_limit)
    exit_code = 2 if summary.classification == "schema_unavailable" else 0
    return render_health_summary(summary), exit_code


def inspect_ingestion_health(
    connection: Connection,
    *,
    run_limit: int = DEFAULT_RUN_LIMIT,
) -> HealthSummary:
    inspector = sa.inspect(connection)
    table_set = set(inspector.get_table_names())
    missing_tables = sorted({INGESTION_RUN_TABLE, INGESTION_ERROR_TABLE} - table_set)
    if missing_tables:
        return HealthSummary(
            classification="schema_unavailable",
            inspected_runs=0,
            success_count=0,
            failure_count=0,
            records_seen_total=0,
            records_written_total=0,
            error_groups=[],
            detail=f"Missing required ingestion log tables: {', '.join(missing_tables)}.",
        )

    runs = _fetch_recent_runs(connection, run_limit=run_limit)
    if not runs:
        return HealthSummary(
            classification="no_recent_runs",
            inspected_runs=0,
            success_count=0,
            failure_count=0,
            records_seen_total=0,
            records_written_total=0,
            error_groups=[],
            detail="IMPLOG is present but contains no recent rows.",
        )

    error_groups = _fetch_recent_error_groups(connection, [run.run_id for run in runs])
    success_count = sum(1 for run in runs if _is_success(run.status))
    failure_count = len(runs) - success_count
    records_seen_total = sum(run.records_seen for run in runs)
    records_written_total = sum(run.records_written for run in runs)

    classification, detail = _classify(
        runs=runs,
        error_groups=error_groups,
        success_count=success_count,
        failure_count=failure_count,
        records_seen_total=records_seen_total,
        records_written_total=records_written_total,
    )

    return HealthSummary(
        classification=classification,
        inspected_runs=len(runs),
        success_count=success_count,
        failure_count=failure_count,
        records_seen_total=records_seen_total,
        records_written_total=records_written_total,
        error_groups=error_groups,
        detail=detail,
    )


def render_health_summary(summary: HealthSummary) -> str:
    lines = [
        "== Ingestion Health Summary ==",
        f"classification: {summary.classification}",
        f"detail: {summary.detail}",
        f"recent_runs_inspected: {summary.inspected_runs}",
        f"success_count: {summary.success_count}",
        f"failure_count: {summary.failure_count}",
        f"records_seen_total: {summary.records_seen_total}",
        f"records_written_total: {summary.records_written_total}",
        "",
        "== Recent Error Evidence ==",
    ]
    if not summary.error_groups:
        lines.append("(none)")
    else:
        lines.append(
            _format_rows(
                ("stage", "error_code", "errors", "affected_runs", "latest_error_at", "sample_message"),
                [
                    (
                        group.stage,
                        group.error_code,
                        group.errors,
                        group.affected_runs,
                        group.latest_error_at,
                        group.sample_message,
                    )
                    for group in summary.error_groups
                ],
            )
        )
    return "\n".join(lines) + "\n"


def _fetch_recent_runs(connection: Connection, *, run_limit: int) -> list[RunSummary]:
    rows = connection.execute(
        _recent_runs_statement(connection),
        {"run_limit": run_limit},
    ).mappings()
    return [
        RunSummary(
            run_id=_stringify(row["run_id"]),
            isin=_stringify(row["isin"]),
            status=_stringify(row["status"]),
            records_seen=int(row["records_seen"] or 0),
            records_written=int(row["records_written"] or 0),
            started_at=row["started_at"],
            finished_at=row["finished_at"],
            message=_stringify(row["message"]),
        )
        for row in rows
    ]


def _fetch_recent_error_groups(connection: Connection, run_ids: list[str]) -> list[ErrorGroup]:
    if not run_ids:
        return []

    rows = connection.execute(_recent_error_groups_statement(connection), {"run_ids": run_ids}).mappings()
    return [
        ErrorGroup(
            stage=_stringify(row["stage"]),
            error_code=_stringify(row["error_code"]),
            errors=int(row["errors"] or 0),
            affected_runs=int(row["affected_runs"] or 0),
            latest_error_at=row["latest_error_at"],
            sample_message=_stringify(row["sample_message"]),
        )
        for row in rows
    ]


def _recent_runs_statement(connection: Connection) -> TextClause:
    return text(
        f"""
        SELECT {_q(connection, "IMPRUNIDN")} AS run_id,
               {_q(connection, "IMPISN")} AS isin,
               {_q(connection, "IMPSTS")} AS status,
               COALESCE({_q(connection, "IMPRSN")}, 0) AS records_seen,
               COALESCE({_q(connection, "IMPRSW")}, 0) AS records_written,
               {_q(connection, "IMPSTADTS")} AS started_at,
               {_q(connection, "IMPFINDTS")} AS finished_at,
               COALESCE({_q(connection, "IMPMSG")}, '') AS message
        FROM {_q(connection, INGESTION_RUN_TABLE)}
        ORDER BY {_q(connection, "IMPSTADTS")} DESC,
                 {_q(connection, "IMPCRTDTS")} DESC,
                 {_q(connection, "IMPISN")} ASC
        LIMIT :run_limit
        """
    )


def _recent_error_groups_statement(connection: Connection) -> TextClause:
    return text(
        f"""
        SELECT {_q(connection, "IMPSTG")} AS stage,
               COALESCE({_q(connection, "IMPECD")}, '') AS error_code,
               COUNT(*) AS errors,
               COUNT(DISTINCT {_q(connection, "IMPRUNIDN")}) AS affected_runs,
               MAX({_q(connection, "IMPCRTDTS")}) AS latest_error_at,
               MIN({_q(connection, "IMPEMS")}) AS sample_message
        FROM {_q(connection, INGESTION_ERROR_TABLE)}
        WHERE {_q(connection, "IMPRUNIDN")} IN :run_ids
        GROUP BY {_q(connection, "IMPSTG")}, {_q(connection, "IMPECD")}
        ORDER BY errors DESC, latest_error_at DESC, stage ASC, error_code ASC
        LIMIT 10
        """
    ).bindparams(bindparam("run_ids", expanding=True))


def _classify(
    *,
    runs: list[RunSummary],
    error_groups: list[ErrorGroup],
    success_count: int,
    failure_count: int,
    records_seen_total: int,
    records_written_total: int,
) -> tuple[str, str]:
    failure_ratio = failure_count / len(runs)
    if failure_count == len(runs) or failure_ratio >= SYSTEMIC_FAILURE_RATIO:
        return (
            "systemic_failure",
            "All or most recent ingestion runs failed.",
        )
    if _has_repeated_connection_errors(error_groups):
        return (
            "systemic_failure",
            "Recent IMPERR groups show repeated connection/client failure evidence.",
        )
    if failure_count and success_count:
        return (
            "isolated_failure",
            "Recent failures are mixed with successful runs.",
        )
    if success_count and records_written_total > 0:
        return (
            "successful_update",
            "Recent successful runs wrote source or curated records.",
        )
    if success_count and records_seen_total > 0 and records_written_total == 0:
        return (
            "healthy_noop_refresh",
            "Recent successful runs saw records and had no writes.",
        )
    if success_count:
        return (
            "healthy_noop_refresh",
            "Recent successful runs completed without writes.",
        )
    return (
        "systemic_failure",
        "Recent ingestion runs did not include a successful run.",
    )


def _has_repeated_connection_errors(error_groups: list[ErrorGroup]) -> bool:
    for group in error_groups:
        evidence = f"{group.stage} {group.error_code} {group.sample_message}".casefold()
        if group.affected_runs >= 2 and any(marker in evidence for marker in CONNECTION_ERROR_MARKERS):
            return True
    return False


def _is_success(status: str) -> bool:
    return status.casefold() == "success"


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
    engine = create_health_engine()
    try:
        report, exit_code = build_health_report(engine)
        stream = sys.stderr if exit_code else sys.stdout
        stream.write(report)
        return exit_code
    except SQLAlchemyError as exc:
        sys.stderr.write(f"Ingestion health classification failed: {_compact_error(exc)}\n")
        return 2
    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
