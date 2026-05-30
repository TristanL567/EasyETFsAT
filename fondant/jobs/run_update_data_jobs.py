from __future__ import annotations

import argparse
import asyncio

from fondant.db.session import AsyncSessionFactory
from fondant.update_data import run_update_jobs

DEFAULT_LIMIT = 1


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run queued update-data INGJOB rows.")
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=f"Maximum queued jobs to process in this run. Defaults to {DEFAULT_LIMIT}.",
    )
    return parser


async def run_job(args: argparse.Namespace) -> int:
    limit = max(args.limit, 0)

    async with AsyncSessionFactory() as session:
        summary = await run_update_jobs(session, limit=limit)

    if summary.processed == 0:
        print(f"No queued update-data jobs found. skipped/no queued: {summary.skipped}.")
    else:
        print(
            "Update-data jobs processed: "
            f"{summary.processed}; successes: {summary.successes}; "
            f"failures: {summary.failures}; skipped/no queued: {summary.skipped}."
        )
    return 0


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    raise SystemExit(asyncio.run(run_job(args)))


if __name__ == "__main__":
    main()
