"""Top up ECB reference rates in REFEXC up to today.

Loads USD, GBP and CHF rates from the earliest per-currency latest stored date
(inclusive) up to today, so gaps left by missed runs are filled. Update-data
runs do the same top-up automatically before they process queued jobs.

Dry run by default: fetches from ECB and reports what it would write. Writes
only with --apply.
"""

from __future__ import annotations

import argparse
import asyncio

from fondant.ingestion.fx_pipeline import top_up_ecb_rates


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Top up ECB reference rates in REFEXC up to today. Dry run unless --apply is given."
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write the fetched rates. Without it, only report what would be written.",
    )
    return parser


async def run_job(*, apply: bool) -> int:
    result = await top_up_ecb_rates(apply=apply)
    latest = result.latest_rate_date.isoformat() if result.latest_rate_date else None
    print(
        f"mode={'apply' if apply else 'dry-run'} currencies={','.join(result.currencies)} "
        f"start={result.start_date.isoformat()} end={result.end_date.isoformat()} "
        f"rates_seen={result.rates_seen} latest_rate_date={latest} "
        f"rates_written={result.rates_written} applied={str(apply).lower()}"
    )
    return 0


def main() -> None:
    args = _build_parser().parse_args()
    raise SystemExit(asyncio.run(run_job(apply=args.apply)))


if __name__ == "__main__":
    main()
