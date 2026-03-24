import pytest
from unittest.mock import MagicMock, patch

from app.automation.daily_iq_job import DailyIQJob


@pytest.fixture
def job():
    """
    Build a DailyIQJob instance with a mocked pipeline.
    """
    job = DailyIQJob()
    job.pipeline = MagicMock()
    return job


def _mock_calendar_service():
    """
    Build a mocked calendar service for a valid trading day.
    """
    mock_calendar_service = MagicMock()
    mock_calendar_service.is_trading_day.return_value = True
    mock_calendar_service.get_target_date.return_value = "2026-03-20"

    mock_previous_day = MagicMock()
    mock_previous_day.strftime.return_value = "2026-03-19"
    mock_calendar_service.get_previous_trading_day.return_value = mock_previous_day

    return mock_calendar_service


def _mock_pipeline_result(csv_content=b"col1,col2\n1,2\n"):
    """
    Build a mocked pipeline result including IQ output and raw B3 file.
    """
    return {
        "csv_content": csv_content,
        "file_name": "iq_2026_03_19.csv",
        "request_date": "2026-03-19",
        "raw_b3_content": b"raw-b3-file-bytes",
        "raw_b3_file_name": "DerivativesOpenPositionFile_20260319.csv",
    }


def _mock_storage_side_effect():
    """
    Build two upload results:
    1. IQ output upload
    2. raw B3 file upload
    """
    return [
        {
            "status": "uploaded",
            "file_path": "test/iq_coeff/iq_2026_03_19.csv",
            "name": "iq_2026_03_19.csv",
            "web_url": "https://sharepoint/iq-file",
        },
        {
            "status": "uploaded",
            "file_path": "test/b3_raw/DerivativesOpenPositionFile_20260319.csv",
            "name": "DerivativesOpenPositionFile_20260319.csv",
            "web_url": "https://sharepoint/raw-file",
        },
    ]


def test_execute_skips_when_target_date_is_not_trading_day(job):
    """
    Job should stop cleanly when target date is not a trading day.
    """
    mock_calendar_service = MagicMock()
    mock_calendar_service.is_trading_day.return_value = False

    with patch.object(job, "_build_calendar_service", return_value=mock_calendar_service):
        result = job.execute(target_date="2026-03-20")

    assert result["status"] == "skipped"
    assert result["target_date"] == "2026-03-20"
    assert result["processing_date"] is None
    assert result["notification_sent"] is False

    job.pipeline.run.assert_not_called()


def test_execute_success_without_notification(job):
    """
    Job should complete successfully without sending notification.
    """
    mock_calendar_service = _mock_calendar_service()
    job.pipeline.run.return_value = _mock_pipeline_result()

    with patch.object(job, "_build_calendar_service", return_value=mock_calendar_service), \
         patch("app.automation.daily_iq_job.SharePointStorage") as mock_storage_cls:

        mock_storage = mock_storage_cls.return_value
        mock_storage.upload_file_bytes.side_effect = _mock_storage_side_effect()

        result = job.execute(
            target_date="2026-03-20",
            storage_method="sharepoint",
            notify=False,
        )

    assert result["status"] == "success"
    assert result["target_date"] == "2026-03-20"
    assert result["processing_date"] == "2026-03-19"
    assert result["file_name"] == "iq_2026_03_19.csv"
    assert result["notification_sent"] is False
    assert result["notification_method"] is None

    job.pipeline.run.assert_called_once_with("2026-03-19")
    assert mock_storage.upload_file_bytes.call_count == 2

    calls = mock_storage.upload_file_bytes.call_args_list

    assert calls[0].kwargs == {
        "file_path": "test/iq_coeff/iq_2026_03_19.csv",
        "file_bytes": b"col1,col2\n1,2\n",
        "content_type": "text/csv",
    }

    assert calls[1].kwargs == {
        "file_path": "test/b3_raw/DerivativesOpenPositionFile_20260319.csv",
        "file_bytes": b"raw-b3-file-bytes",
        "content_type": "text/csv",
    }


def test_execute_success_without_notification_converts_csv_string_to_bytes(job):
    """
    Job should convert CSV string content to bytes before SharePoint upload.
    """
    mock_calendar_service = _mock_calendar_service()
    job.pipeline.run.return_value = _mock_pipeline_result(
        csv_content="col1,col2\n1,2\n"
    )

    with patch.object(job, "_build_calendar_service", return_value=mock_calendar_service), \
         patch("app.automation.daily_iq_job.SharePointStorage") as mock_storage_cls:

        mock_storage = mock_storage_cls.return_value
        mock_storage.upload_file_bytes.side_effect = _mock_storage_side_effect()

        result = job.execute(
            target_date="2026-03-20",
            storage_method="sharepoint",
            notify=False,
        )

    assert result["status"] == "success"
    assert mock_storage.upload_file_bytes.call_count == 2

    calls = mock_storage.upload_file_bytes.call_args_list

    assert calls[0].kwargs == {
        "file_path": "test/iq_coeff/iq_2026_03_19.csv",
        "file_bytes": b"col1,col2\n1,2\n",
        "content_type": "text/csv",
    }

    assert calls[1].kwargs == {
        "file_path": "test/b3_raw/DerivativesOpenPositionFile_20260319.csv",
        "file_bytes": b"raw-b3-file-bytes",
        "content_type": "text/csv",
    }


def test_execute_success_with_notification(job):
    """
    Job should complete successfully and send notification when requested.
    """
    mock_calendar_service = _mock_calendar_service()
    job.pipeline.run.return_value = _mock_pipeline_result()

    with patch.object(job, "_build_calendar_service", return_value=mock_calendar_service), \
         patch("app.automation.daily_iq_job.SharePointStorage") as mock_storage_cls, \
         patch("app.automation.daily_iq_job.EmailSender") as mock_email_cls:

        mock_storage = mock_storage_cls.return_value
        mock_storage.upload_file_bytes.side_effect = _mock_storage_side_effect()

        mock_email = mock_email_cls.return_value
        mock_email.send.return_value = {
            "status": "sent",
            "message": "Email sent successfully",
        }

        result = job.execute(
            target_date="2026-03-20",
            storage_method="sharepoint",
            notify=True,
            notification_method="email",
            recipients=["test@example.com"],
        )

    assert result["status"] == "success"
    assert result["notification_sent"] is True
    assert result["notification_method"] == "email"

    assert mock_storage.upload_file_bytes.call_count == 2
    mock_email.create_email.assert_called_once()
    mock_email.add_attachment.assert_called_once_with(
        file_name="iq_2026_03_19.csv",
        file_bytes=b"col1,col2\n1,2\n",
        content_type="text/csv",
    )
    mock_email.send.assert_called_once()


def test_execute_returns_failed_when_pipeline_raises(job):
    """
    Job should return failed status when pipeline raises an exception.
    """
    mock_calendar_service = _mock_calendar_service()
    job.pipeline.run.side_effect = RuntimeError("Pipeline exploded")

    with patch.object(job, "_build_calendar_service", return_value=mock_calendar_service), \
         patch.object(job, "_upload_error_log", return_value={"status": "uploaded"}):

        result = job.execute(target_date="2026-03-20")

    assert result["status"] == "failed"
    assert "Pipeline exploded" in result["error"]
    assert result["error_log_storage_result"] == {"status": "uploaded"}


def test_execute_returns_failed_when_storage_raises(job):
    """
    Job should return failed status when storage upload raises an exception.
    """
    mock_calendar_service = _mock_calendar_service()
    job.pipeline.run.return_value = _mock_pipeline_result()

    with patch.object(job, "_build_calendar_service", return_value=mock_calendar_service), \
         patch("app.automation.daily_iq_job.SharePointStorage") as mock_storage_cls, \
         patch.object(job, "_upload_error_log", return_value={"status": "uploaded"}):

        mock_storage = mock_storage_cls.return_value
        mock_storage.upload_file_bytes.side_effect = RuntimeError("Storage failed")

        result = job.execute(target_date="2026-03-20")

    assert result["status"] == "failed"
    assert "Storage failed" in result["error"]
    assert result["error_log_storage_result"] == {"status": "uploaded"}


def test_execute_returns_failed_when_notification_raises(job):
    """
    Job should return failed status when notification sending raises an exception.
    """
    mock_calendar_service = _mock_calendar_service()
    job.pipeline.run.return_value = _mock_pipeline_result()

    with patch.object(job, "_build_calendar_service", return_value=mock_calendar_service), \
         patch("app.automation.daily_iq_job.SharePointStorage") as mock_storage_cls, \
         patch("app.automation.daily_iq_job.EmailSender") as mock_email_cls, \
         patch.object(job, "_upload_error_log", return_value={"status": "uploaded"}):

        mock_storage = mock_storage_cls.return_value
        mock_storage.upload_file_bytes.side_effect = _mock_storage_side_effect()

        mock_email = mock_email_cls.return_value
        mock_email.send.side_effect = RuntimeError("Email failed")

        result = job.execute(
            target_date="2026-03-20",
            notify=True,
            notification_method="email",
            recipients=["test@example.com"],
        )

    assert result["status"] == "failed"
    assert "Email failed" in result["error"]
    assert result["error_log_storage_result"] == {"status": "uploaded"}


def test_resolve_recipients_uses_config_default(job):
    """
    _resolve_recipients should return config defaults when no override is passed.
    """
    with patch(
        "app.automation.daily_iq_job.config.DEFAULT_REPORT_RECIPIENTS",
        ["default@example.com"],
    ):
        recipients = job._resolve_recipients(None)

    assert recipients == ["default@example.com"]


def test_resolve_recipients_uses_passed_recipients(job):
    """
    _resolve_recipients should use passed recipients when provided.
    """
    recipients = job._resolve_recipients(["custom@example.com"])

    assert recipients == ["custom@example.com"]