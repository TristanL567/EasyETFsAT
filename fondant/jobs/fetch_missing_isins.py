from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from sqlalchemy import distinct, select

from fondant.db.models import SOURCERPT
from fondant.db.session import AsyncSessionFactory
from fondant.ingestion.pipeline import IngestionResult, ingest_many
from fondant.jobs.isin_storage import (
    DEFAULT_STORAGE_PATH,
    add_storage_isins,
    is_valid_isin,
    load_storage_isins,
    normalize_isin,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch OeKB data for ISINs in storage that are not yet available in SOURCERPT."
    )
    parser.add_argument(
        "--storage",
        type=Path,
        default=DEFAULT_STORAGE_PATH,
        help="Path to ISIN storage CSV (default: Documentation/isin_storage.csv).",
    )
    parser.add_argument(
        "--isin",
        action="append",
        default=[],
        help="Additional ISIN to include in this run (repeatable).",
    )
    parser.add_argument(
        "--persist-input",
        action="store_true",
        help="Persist --isin inputs into storage before fetching.",
    )
    parser.add_argument("--limit", type=int, default=None, help="Optional max number of ISINs to fetch.")
    parser.add_argument("--force", action="store_true", help="Fetch even if ISIN already exists in SOURCERPT.")
    parser.add_argument("--dry-run", action="store_true", help="Show candidates without fetching.")
    parser.add_argument("--show-isins", action="store_true", help="Print candidate ISIN list.")
    return parser


def _sanitize_input_isins(raw_isins: list[str]) -> tuple[list[str], list[str]]:
    deduped: set[str] = set()
    invalid: list[str] = []

    for raw in raw_isins:
        normalized = normalize_isin(raw)
        if not normalized:
            continue
        if not is_valid_isin(normalized):
            invalid.append(raw)
            continue
        deduped.add(normalized)

    return sorted(deduped), invalid


async def _load_existing_source_isins() -> set[str]:
    async with AsyncSessionFactory() as session:
        rows = await session.execute(select(distinct(SOURCERPT.isin)))
    return {row[0] for row in rows if row[0]}


def _summarize_results(results: list[IngestionResult]) -> None:
    success = [result for result in results if result.status == "SUCCESS"]
    failed = [result for result in results if result.status != "SUCCESS"]
    total_seen = sum(result.records_seen for result in results)
    total_written = sum(result.records_written for result in results)

    print(f"Fetched ISINs: {len(results)}")
    print(f"Success: {len(success)} | Failed: {len(failed)}")
    print(f"Total FIN reports seen: {total_seen}")
    print(f"Total reports written/updated: {total_written}")
    if failed:
        print("Failed ISINs:")
        for result in failed:
            print(f"- {result.isin}: {result.message or 'unknown error'}")


async def run_job(args: argparse.Namespace) -> int:
    input_isins, invalid_inputs = _sanitize_input_isins(args.isin)
    if invalid_inputs:
        print("Invalid --isin values:")
        for value in invalid_inputs:
            print(f"- {value}")
        return 1

    if args.persist_input and input_isins:
        added, invalid_persist = add_storage_isins(input_isins, path=args.storage)
        if invalid_persist:
            print("Invalid ISIN values while persisting:")
            for value in invalid_persist:
                print(f"- {value}")
            return 1
        print(f"Persisted {len(added)} new ISIN(s) to {args.storage}.")

    storage_isins = load_storage_isins(path=args.storage)
    universe = sorted(set(storage_isins) | set(input_isins))

    if not universe:
        print("No ISINs found in storage/input.")
        return 0

    existing = await _load_existing_source_isins()
    if args.force:
        candidates = universe
    else:
        candidates = [isin for isin in universe if isin not in existing]

    if args.limit is not None:
        candidates = candidates[: max(args.limit, 0)]

    print(f"Universe size: {len(universe)}")
    print(f"Already in SOURCERPT: {len([isin for isin in universe if isin in existing])}")
    print(f"Candidate fetch count: {len(candidates)}")
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

