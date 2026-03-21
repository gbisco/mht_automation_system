from pprint import pprint

from app.automation.daily_iq_job import DailyIQJob
from app.logger.logger import AppLogger


logger = AppLogger("scripts.test_daily_iq_job_real")


def run_live_test_with_notification(target_date: str) -> tuple[dict, dict]:
    """
    Run full live test:
    - real calendar
    - real B3 pipeline
    - real SharePoint upload
    - real email notification
    """
    logger.write(
        f"Starting live E2E test with notification | target_date={target_date}"
    )

    job = DailyIQJob()

    calendar_service = job._build_calendar_service(target_date)

    if not job._is_target_date_trading_day(calendar_service):
        raise ValueError(
            f"Target date {target_date} is not a trading day. "
            "Choose an open market day."
        )

    processing_date = job._resolve_processing_date(calendar_service)

    # Hit B3 only once
    pipeline_result = job._execute_pipeline(processing_date)

    storage_result = job._store_output(
        storage_method="sharepoint",
        pipeline_result=pipeline_result,
    )

    recipients = job._resolve_recipients(None)

    notification_result = job._send_notification(
        notification_method="email",
        recipients=recipients,
        pipeline_result=pipeline_result,
        target_date=target_date,
        processing_date=processing_date,
        storage_result=storage_result,
    )

    result = {
        "status": "success",
        "message": "Live E2E test with notification completed successfully.",
        "target_date": target_date,
        "processing_date": processing_date,
        "file_name": job._extract_file_name(pipeline_result),
        "storage_method": "sharepoint",
        "storage_result": storage_result,
        "notification_sent": True,
        "notification_method": "email",
        "notification_result": notification_result,
        "error": None,
    }

    logger.write(
        f"Finished live E2E test with notification | target_date={target_date} | "
        f"processing_date={processing_date}"
    )

    return result, pipeline_result


def run_cached_test_without_notification(
    target_date: str,
    pipeline_result: dict,
) -> dict:
    """
    Run second E2E-style test:
    - reuse already generated pipeline result
    - real SharePoint upload
    - no email notification
    - avoids second B3 request
    """
    logger.write(
        f"Starting cached E2E test without notification | target_date={target_date}"
    )

    job = DailyIQJob()

    calendar_service = job._build_calendar_service(target_date)

    if not job._is_target_date_trading_day(calendar_service):
        raise ValueError(
            f"Target date {target_date} is not a trading day. "
            "Choose an open market day."
        )

    processing_date = job._resolve_processing_date(calendar_service)

    storage_result = job._store_output(
        storage_method="sharepoint",
        pipeline_result=pipeline_result,
    )

    result = {
        "status": "success",
        "message": "Cached E2E test without notification completed successfully.",
        "target_date": target_date,
        "processing_date": processing_date,
        "file_name": job._extract_file_name(pipeline_result),
        "storage_method": "sharepoint",
        "storage_result": storage_result,
        "notification_sent": False,
        "notification_method": None,
        "notification_result": None,
        "error": None,
    }

    logger.write(
        f"Finished cached E2E test without notification | target_date={target_date} | "
        f"processing_date={processing_date}"
    )

    return result


if __name__ == "__main__":
    # Pick a target date that is open in your calendar fixture
    target_date = "2026-03-20"

    print("\n=== TEST 1: LIVE E2E WITH NOTIFICATION ===\n")
    live_result, cached_pipeline_result = run_live_test_with_notification(target_date)
    pprint(live_result)

    print("\n=== TEST 2: CACHED E2E WITHOUT NOTIFICATION ===\n")
    cached_result = run_cached_test_without_notification(
        target_date=target_date,
        pipeline_result=cached_pipeline_result,
    )
    pprint(cached_result)