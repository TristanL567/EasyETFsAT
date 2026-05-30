from __future__ import annotations

from dataclasses import dataclass

from fondant.ingestion.pipeline import ingest_isin


@dataclass(frozen=True)
class UpdateDataResult:
    isin: str
    status: str
    records_seen: int
    records_written: int
    message: str
    error: str | None = None


async def update_single_isin(isin: str) -> UpdateDataResult:
    """Run update-data ingestion for one already normalized and validated ISIN."""
    try:
        ingestion_result = await ingest_isin(isin)
    except Exception as exc:
        error = str(exc) or type(exc).__name__
        return UpdateDataResult(
            isin=isin,
            status="failed",
            records_seen=0,
            records_written=0,
            message=f"Ingestion failed for {isin}: {error}",
            error=error,
        )

    status = ingestion_result.status.lower()
    if status == "success":
        return UpdateDataResult(
            isin=ingestion_result.isin,
            status="success",
            records_seen=ingestion_result.records_seen,
            records_written=ingestion_result.records_written,
            message=ingestion_result.message or "Ingestion completed.",
        )

    message = ingestion_result.message or f"Ingestion returned status {ingestion_result.status}."
    return UpdateDataResult(
        isin=ingestion_result.isin,
        status="failed",
        records_seen=ingestion_result.records_seen,
        records_written=ingestion_result.records_written,
        message=message,
        error=message,
    )
