import pytest
from unittest.mock import MagicMock, patch, call

from app import config
from app.automation.daily_iq_job import DailyIQJob


@pytest.fixture
def job():
    """
    Build a DailyIQJob instance with mocked pipelines.
    """
    job = DailyIQJob()
    job.pipeline = MagicMock()
    job.delta_pipeline = MagicMock()
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
        "file_name": "iq_coef_20260319.csv",
        "request_date": "2026-03-19",
        "raw_b3_content": b"raw-b3-file-bytes",
        "raw_b3_file_name": "DerivativesOpenPositionFile_20260319.csv",
    }


def _mock_delta_result(csv_content="Asset,Delta_IQ\nABCB,1.0\n"):
    """
    Build a mocked delta pipeline result.
    """
    return {
        "csv_content": csv_content,
        "file_name": "iq_delta_latest.csv",
    }


def _mock_storage_side_effect():
    """
    Build three upload results:
    1. IQ output upload
    2. raw B3 file upload
    3. delta IQ file upload
    """
    return [
        {
            "status": "uploaded",
            "file_path": "test/iq_coeff/iq_coef_20260319.csv",
            "name": "iq_coef_20260319.csv",
            "web_url": "https://sharepoint/iq-file",
        },
        {
            "status": "uploaded",
            "file_path": "test/b3_raw/DerivativesOpenPositionFile_20260319.csv",
            "name": "DerivativesOpenPositionFile_20260319.csv",
            "web_url": "https://sharepoint/raw-file",
        },
        {
            "status": "uploaded",
            "file_path": "test/iq_coeff/iq_delta_latest.csv",
            "name": "iq_delta_latest.csv",
            "web_url": "https://sharepoint/delta-file",
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


def test_execute_success_without_notification(job):
    """
    Job should complete successfully without sending notification.
    """
    mock_calendar_service = _mock_calendar_service()
    pipeline_result = _mock_pipeline_result()
    delta_result = _mock_delta_result()

    with patch.object(job, "_build_calendar_service", return_value=mock_calendar_service), \
         patch.object(job, "_execute_pipeline", return_value=pipeline_result) as mock_execute_pipeline, \
         patch.object(job, "_store_output", side_effect=_mock_storage_side_effect()) as mock_store_output, \
         patch.object(job, "_resolve_previous_iq_file_path", return_value="test/iq_coeff/iq_coef_20260318.csv") as mock_resolve_previous_path, \
         patch.object(job, "_load_previous_iq_output", return_value=b"prev-iq-bytes") as mock_load_previous, \
         patch.object(job, "_execute_delta_pipeline", return_value=delta_result) as mock_execute_delta, \
         patch("app.automation.daily_iq_job.SharePointStorage"):

        result = job.execute(
            target_date="2026-03-20",
            storage_method="sharepoint",
            notify=False,
        )

    assert result["status"] == "success"
    assert result["target_date"] == "2026-03-20"
    assert result["processing_date"] == "2026-03-19"
    assert result["file_name"] == "iq_coef_20260319.csv"
    assert result["notification_sent"] is False
    assert result["notification_method"] is None

    mock_execute_pipeline.assert_called_once_with("2026-03-19")
    mock_resolve_previous_path.assert_called_once()
    mock_load_previous.assert_called_once_with("test/iq_coeff/iq_coef_20260318.csv")
    mock_execute_delta.assert_called_once_with(
        current_iq_content=b"col1,col2\n1,2\n",
        previous_iq_content=b"prev-iq-bytes",
    )

    assert mock_store_output.call_count == 3

    assert mock_store_output.call_args_list[0].kwargs == {
        "storage_method": "sharepoint",
        "file_name": "iq_coef_20260319.csv",
        "file_content": b"col1,col2\n1,2\n",
        "folder": config.SHAREPOINT_IQ_OUTPUT_FOLDER,
    }

    assert mock_store_output.call_args_list[1].kwargs == {
        "storage_method": "sharepoint",
        "file_name": "DerivativesOpenPositionFile_20260319.csv",
        "file_content": b"raw-b3-file-bytes",
        "folder": config.SHAREPOINT_B3_RAW_FOLDER,
    }

    assert mock_store_output.call_args_list[2].kwargs == {
        "storage_method": "sharepoint",
        "file_name": "iq_delta_latest.csv",
        "file_content": "Asset,Delta_IQ\nABCB,1.0\n",
        "folder": config.SHAREPOINT_IQ_OUTPUT_FOLDER,
    }

    assert result["storage_result"]["iq_file"]["name"] == "iq_coef_20260319.csv"
    assert result["storage_result"]["raw_b3_file"]["name"] == "DerivativesOpenPositionFile_20260319.csv"
    assert result["storage_result"]["delta_iq_file"]["name"] == "iq_delta_latest.csv"


def test_execute_success_without_notification_converts_csv_string_to_bytes_for_delta(job):
    """
    Job should convert current IQ CSV string content to bytes before calling
    the delta pipeline wrapper.
    """
    mock_calendar_service = _mock_calendar_service()
    pipeline_result = _mock_pipeline_result(csv_content="col1,col2\n1,2\n")
    delta_result = _mock_delta_result()

    with patch.object(job, "_build_calendar_service", return_value=mock_calendar_service), \
         patch.object(job, "_execute_pipeline", return_value=pipeline_result), \
         patch.object(job, "_store_output", side_effect=_mock_storage_side_effect()), \
         patch.object(job, "_resolve_previous_iq_file_path", return_value="test/iq_coeff/iq_coef_20260318.csv"), \
         patch.object(job, "_load_previous_iq_output", return_value=b"prev-iq-bytes"), \
         patch.object(job, "_execute_delta_pipeline", return_value=delta_result) as mock_execute_delta, \
         patch("app.automation.daily_iq_job.SharePointStorage"):

        result = job.execute(
            target_date="2026-03-20",
            storage_method="sharepoint",
            notify=False,
        )

    assert result["status"] == "success"
    mock_execute_delta.assert_called_once_with(
        current_iq_content=b"col1,col2\n1,2\n",
        previous_iq_content=b"prev-iq-bytes",
    )


def test_execute_success_with_notification(job):
    """
    Job should complete successfully and send notification when requested.
    """
    mock_calendar_service = _mock_calendar_service()
    pipeline_result = _mock_pipeline_result()
    delta_result = _mock_delta_result()
    storage_results = _mock_storage_side_effect()

    with patch.object(job, "_build_calendar_service", return_value=mock_calendar_service), \
         patch.object(job, "_execute_pipeline", return_value=pipeline_result), \
         patch.object(job, "_store_output", side_effect=storage_results), \
         patch.object(job, "_resolve_previous_iq_file_path", return_value="test/iq_coeff/iq_coef_20260318.csv"), \
         patch.object(job, "_load_previous_iq_output", return_value=b"prev-iq-bytes"), \
         patch.object(job, "_execute_delta_pipeline", return_value=delta_result), \
         patch.object(job, "_resolve_recipients", return_value=["test@example.com"]) as mock_resolve_recipients, \
         patch.object(job, "_send_notification", return_value={"status": "sent"}) as mock_send_notification, \
         patch("app.automation.daily_iq_job.SharePointStorage"):

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
    assert result["notification_result"] == {"status": "sent"}

    mock_resolve_recipients.assert_called_once_with(["test@example.com"])
    mock_send_notification.assert_called_once_with(
        notification_method="email",
        recipients=["test@example.com"],
        pipeline_result=pipeline_result,
        delta_result=delta_result,
        target_date="2026-03-20",
        processing_date="2026-03-19",
        previous_iq_date="2026-03-18",
        storage_result={
            "iq_file": storage_results[0],
            "raw_b3_file": storage_results[1],
            "delta_iq_file": storage_results[2],
        },
    )


def test_execute_returns_failed_when_pipeline_raises(job):
    """
    Job should return failed status when IQ pipeline execution raises.
    """
    mock_calendar_service = _mock_calendar_service()

    with patch.object(job, "_build_calendar_service", return_value=mock_calendar_service), \
         patch.object(job, "_execute_pipeline", side_effect=RuntimeError("Pipeline exploded")), \
         patch.object(job, "_upload_error_log", return_value={"status": "uploaded"}):

        result = job.execute(target_date="2026-03-20")

    assert result["status"] == "failed"
    assert "Pipeline exploded" in result["error"]
    assert result["error_log_storage_result"] == {"status": "uploaded"}


def test_execute_returns_failed_when_delta_pipeline_raises(job):
    """
    Job should return failed status when delta pipeline execution raises.
    """
    mock_calendar_service = _mock_calendar_service()
    pipeline_result = _mock_pipeline_result()

    with patch.object(job, "_build_calendar_service", return_value=mock_calendar_service), \
         patch.object(job, "_execute_pipeline", return_value=pipeline_result), \
         patch.object(job, "_store_output", side_effect=_mock_storage_side_effect()[:2]), \
         patch.object(job, "_resolve_previous_iq_file_path", return_value="test/iq_coeff/iq_coef_20260318.csv"), \
         patch.object(job, "_load_previous_iq_output", return_value=b"prev-iq-bytes"), \
         patch.object(job, "_execute_delta_pipeline", side_effect=RuntimeError("Delta failed")), \
         patch.object(job, "_upload_error_log", return_value={"status": "uploaded"}), \
         patch("app.automation.daily_iq_job.SharePointStorage"):

        result = job.execute(target_date="2026-03-20")

    assert result["status"] == "failed"
    assert "Delta failed" in result["error"]
    assert result["error_log_storage_result"] == {"status": "uploaded"}


def test_execute_returns_failed_when_storage_raises(job):
    """
    Job should return failed status when storage upload raises an exception.
    """
    mock_calendar_service = _mock_calendar_service()
    pipeline_result = _mock_pipeline_result()

    with patch.object(job, "_build_calendar_service", return_value=mock_calendar_service), \
         patch.object(job, "_execute_pipeline", return_value=pipeline_result), \
         patch.object(job, "_store_output", side_effect=RuntimeError("Storage failed")), \
         patch.object(job, "_upload_error_log", return_value={"status": "uploaded"}):

        result = job.execute(target_date="2026-03-20")

    assert result["status"] == "failed"
    assert "Storage failed" in result["error"]
    assert result["error_log_storage_result"] == {"status": "uploaded"}


def test_execute_returns_failed_when_notification_raises(job):
    """
    Job should return failed status when notification sending raises an exception.
    """
    mock_calendar_service = _mock_calendar_service()
    pipeline_result = _mock_pipeline_result()
    delta_result = _mock_delta_result()

    with patch.object(job, "_build_calendar_service", return_value=mock_calendar_service), \
         patch.object(job, "_execute_pipeline", return_value=pipeline_result), \
         patch.object(job, "_store_output", side_effect=_mock_storage_side_effect()), \
         patch.object(job, "_resolve_previous_iq_file_path", return_value="test/iq_coeff/iq_coef_20260318.csv"), \
         patch.object(job, "_load_previous_iq_output", return_value=b"prev-iq-bytes"), \
         patch.object(job, "_execute_delta_pipeline", return_value=delta_result), \
         patch.object(job, "_resolve_recipients", return_value=["test@example.com"]), \
         patch.object(job, "_send_notification", side_effect=RuntimeError("Email failed")), \
         patch.object(job, "_upload_error_log", return_value={"status": "uploaded"}), \
         patch("app.automation.daily_iq_job.SharePointStorage"):

        result = job.execute(
            target_date="2026-03-20",
            notify=True,
            notification_method="email",
            recipients=["test@example.com"],
        )

    assert result["status"] == "failed"
    assert "Email failed" in result["error"]
    assert result["error_log_storage_result"] == {"status": "uploaded"}


def test_execute_delta_pipeline_returns_expected_result(job):
    """
    _execute_delta_pipeline should decode both byte inputs, run the delta
    pipeline, and return csv content plus output file name.
    """
    job.delta_pipeline.run.return_value = "Asset,Delta_IQ\nABCB,1.0\n"
    job.delta_pipeline.get_output_filename.return_value = "iq_delta_latest.csv"

    result = job._execute_delta_pipeline(
        current_iq_content=b"Asset,IQ_coef\nABCB,3.2\n",
        previous_iq_content=b"Asset,IQ_coef\nABCB,2.1\n",
    )

    assert result == {
        "csv_content": "Asset,Delta_IQ\nABCB,1.0\n",
        "file_name": "iq_delta_latest.csv",
    }

    job.delta_pipeline.run.assert_called_once_with(
        current_csv="Asset,IQ_coef\nABCB,3.2\n",
        previous_csv="Asset,IQ_coef\nABCB,2.1\n",
    )
    job.delta_pipeline.get_output_filename.assert_called_once()


def test_execute_delta_pipeline_raises_for_non_bytes_current(job):
    """
    _execute_delta_pipeline should reject non-bytes current input.
    """
    with pytest.raises(ValueError, match="current_iq_content must be bytes"):
        job._execute_delta_pipeline(
            current_iq_content="not-bytes",
            previous_iq_content=b"Asset,IQ_coef\nABCB,2.1\n",
        )


def test_load_previous_iq_output_downloads_bytes(job):
    """
    _load_previous_iq_output should check existence and download file bytes.
    """
    with patch("app.automation.daily_iq_job.SharePointStorage") as mock_storage_cls:
        mock_storage = mock_storage_cls.return_value
        mock_storage.file_exists.return_value = True
        mock_storage.download_file_bytes.return_value = b"previous-file-bytes"

        result = job._load_previous_iq_output(
            "test/iq_coeff/iq_coef_20260318.csv"
        )

    assert result == b"previous-file-bytes"
    mock_storage.file_exists.assert_called_once_with(
        "test/iq_coeff/iq_coef_20260318.csv"
    )
    mock_storage.download_file_bytes.assert_called_once_with(
        "test/iq_coeff/iq_coef_20260318.csv"
    )


def test_extract_iq_date_returns_expected_date(job):
    """
    _extract_iq_date should parse valid IQ file names.
    """
    result = job._extract_iq_date("iq_coef_20260319.csv")

    assert result == "2026-03-19"


def test_extract_iq_date_returns_none_for_invalid_name(job):
    """
    _extract_iq_date should return None for invalid file names.
    """
    assert job._extract_iq_date("random_file.csv") is None
    assert job._extract_iq_date("iq_coef_202603.csv") is None
    assert job._extract_iq_date("iq_coef_2026AA19.csv") is None


def test_resolve_previous_iq_file_path_selects_latest_prior_file(job):
    """
    _resolve_previous_iq_file_path should select the latest valid prior IQ file
    based on filename date, not just folder order.
    """
    mock_storage = MagicMock()
    mock_storage.list_files.return_value = [
        {
            "name": "iq_coef_20260323.csv",
            "file_path": "test/iq_coeff/iq_coef_20260323.csv",
        },
        {
            "name": "iq_coef_20260320.csv",
            "file_path": "test/iq_coeff/iq_coef_20260320.csv",
        },
        {
            "name": "iq_coef_20260319.csv",
            "file_path": "test/iq_coeff/iq_coef_20260319.csv",
        },
        {
            "name": "ignore_me.txt",
            "file_path": "test/iq_coeff/ignore_me.txt",
        },
    ]

    result = job._resolve_previous_iq_file_path(
        storage=mock_storage,
        folder_path="test/iq_coeff",
        processing_date="2026-03-23",
    )

    assert result == "test/iq_coeff/iq_coef_20260320.csv"
    mock_storage.list_files.assert_called_once_with("test/iq_coeff")


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