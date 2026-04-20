from __future__ import annotations

import argparse
import asyncio

from sqlalchemy import distinct, select

from fondant.db.models import SOURCERPT
from fondant.db.session import AsyncSessionFactory
from fondant.ingestion.pipeline import IngestionResult, ingest_many
from fondant.jobs.isin_storage import is_valid_isin, normalize_isin


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Refresh all ISINs that already exist in SOURCERPT (checks for OeKB changes)."
    )
    parser.add_argument(
        "--isin",
        action="append",
        default=[],
        help="Restrict refresh to one or more specific ISINs (repeatable).",
    )
    parser.add_argument("--limit", type=int, default=None, help="Optional max number of ISINs to refresh.")
    parser.add_argument("--dry-run", action="store_true", help="Show candidates without fetching.")
    parser.add_argument("--show-isins", action="store_true", help="Print candidate ISIN list.")
    return parser


def _sanitize_requested_isins(raw_isins: list[str]) -> tuple[set[str], list[str]]:
    cleaned: set[str] = set()
    invalid: list[str] = []
    for raw in raw_isins:
        normalized = normalize_isin(raw)
        if not normalized:
            continue
        if not is_valid_isin(normalized):
            invalid.append(raw)
            continue
        cleaned.add(normalized)
    return cleaned, invalid


async def _load_existing_source_isins() -> list[str]:
    async with AsyncSessionFactory() as session:
        rows = await session.execute(select(distinct(SOURCERPT.isin)))
    return sorted(row[0] for row in rows if row[0])


def _summarize_results(results: list[IngestionResult]) -> None:
    success = [result for result in results if result.status == "SUCCESS"]
    failed = [result for result in results if result.status != "SUCCESS"]
    total_seen = sum(result.records_seen for result in results)
    total_written = sum(result.records_written for result in results)
    unchanged = sum(1 for result in results if result.records_written == 0 and result.status == "SUCCESS")

    print(f"Refreshed ISINs: {len(results)}")
    print(f"Success: {len(success)} | Failed: {len(failed)}")
    print(f"Unchanged ISINs: {unchanged}")
    print(f"Total FIN reports seen: {total_seen}")
    print(f"Total reports written/updated: {total_written}")
    if failed:
        print("Failed ISINs:")
        for result in failed:
            print(f"- {result.isin}: {result.message or 'unknown error'}")


async def run_job(args: argparse.Namespace) -> int:
    requested, invalid_inputs = _sanitize_requested_isins(args.isin)
    if invalid_inputs:
        print("Invalid --isin values:")
        for value in invalid_inputs:
            print(f"- {value}")
        return 1

    existing = await _load_existing_source_isins()
    if not existing:
        print("SOURCERPT has no ISINs yet. Nothing to refresh.")
        return 0

    existing_set = set(existing)
    if requested:
        candidates = [isin for isin in sorted(requested) if isin in existing_set]
        missing_requested = [isin for isin in sorted(requested) if isin not in existing_set]
        if missing_requested:
            print("Requested ISINs not found in SOURCERPT:")
            for isin in missing_requested:
                print(f"- {isin}")
    else:
        candidates = existing

    if args.limit is not None:
        candidates = candidates[: max(args.limit, 0)]

    print(f"Existing ISINs in SOURCERPT: {len(existing)}")
    print(f"Candidate refresh count: {len(candidates)}")
    if args.show_isins:
        for isin in candidates:
            print(isin)

    if args.dry_run or not candidates:
        return 0

    results = await ingest_many(candidates)
    _summarize_results(results)
    return 0 if all(result.status == "SUCCESS" for result in results) else 2


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    raise SystemExit(asyncio.run(run_job(args)))


if __name__ == "__main__":
    main()

