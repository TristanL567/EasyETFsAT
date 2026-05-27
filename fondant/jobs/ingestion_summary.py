from __future__ import annotations

from fondant.ingestion.pipeline import IngestionResult


def summarize_ingestion_batch(
    *,
    action_label: str,
    results: list[IngestionResult],
    unchanged_count: int | None = None,
) -> None:
    success = [result for result in results if result.status == "SUCCESS"]
    failed = [result for result in results if result.status != "SUCCESS"]
    total_seen = sum(result.records_seen for result in results)
    total_written = sum(result.records_written for result in results)
    outcome, failure_category = _classify_batch(total=len(results), failed=len(failed))

    print(f"{action_label} ISINs: {len(results)}")
    print(f"Success: {len(success)} | Failed: {len(failed)}")
    print(f"Batch outcome: {outcome}")
    print(f"Failure category: {failure_category}")
    if unchanged_count is not None:
        print(f"Unchanged ISINs: {unchanged_count}")
    print(f"Total FIN reports seen: {total_seen}")
    print(f"Total reports written/updated: {total_written}")
    if failed:
        print("Failed ISINs:")
        for result in failed:
            print(f"- {result.isin}: {result.message or 'unknown error'}")


def _classify_batch(*, total: int, failed: int) -> tuple[str, str]:
    if failed == 0:
        return "all_success", "none"
    if failed == total:
        return "all_failure", "systemic_batch_failure"
    return "mixed_failure", "isolated_isin_failures"
