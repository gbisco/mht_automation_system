"""
Daily IQ Job Runner

Purpose:
    - Provide a runnable script for the Daily IQ automation workflow
    - Default execution to today's date for scheduled runs
    - Allow optional manual overrides through command-line arguments
    - Return process exit codes for automation environments such as GitHub Actions

Usage examples:
    python scripts/run_daily_iq_job.py
    python scripts/run_daily_iq_job.py --target-date 2026-03-20
    python scripts/run_daily_iq_job.py --target-date 2026-03-20 --notify
"""
from __future__ import annotations

import argparse
import sys
from pprint import pprint
from datetime import datetime

from app.automation.daily_iq_job import DailyIQJob
from app.logger.logger import AppLogger


logger = AppLogger("entrypoints.run_daily_iq_job")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the Daily IQ automation job."
    )

    parser.add_argument(
        "--target-date",
        dest="target_date",
        default=datetime.today().strftime("%Y-%m-%d"),
        help="Target date in YYYY-MM-DD format. Defaults to today.",
    )

    parser.add_argument(
        "--storage-method",
        dest="storage_method",
        default="sharepoint",
        help="Storage backend to use. Default: sharepoint.",
    )

    parser.add_argument(
        "--notify",
        action="store_true",
        help="Send notification after successful run.",
    )

    parser.add_argument(
        "--notification-method",
        dest="notification_method",
        default="email",
        help="Notification backend to use when --notify is enabled. Default: email.",
    )

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    logger.write(
        f"Starting Daily IQ entrypoint | "
        f"target_date={args.target_date} | "
        f"storage_method={args.storage_method} | "
        f"notify={args.notify} | "
        f"notification_method={args.notification_method}"
    )

    try:
        job = DailyIQJob()

        result = job.execute(
            target_date=args.target_date,
            storage_method=args.storage_method,
            notify=args.notify,
            notification_method=args.notification_method,
        )

        pprint(result)

        status = result.get("status")

        if status == "success":
            logger.write("Daily IQ entrypoint completed successfully.")
            return 0

        if status == "skipped":
            logger.write(
                "Daily IQ entrypoint finished with skipped status.",
                level="info",
            )
            return 0

        logger.write(
            f"Daily IQ entrypoint finished with failure status: {status}",
            level="warning",
        )
        return 1

    except Exception as exc:
        logger.exception(f"Daily IQ entrypoint failed unexpectedly | error={exc}")
        return 1

if __name__ == "__main__":
    sys.exit(main())