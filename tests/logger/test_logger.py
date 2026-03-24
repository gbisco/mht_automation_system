from pathlib import Path
import logging
import uuid

import pytest

from app.logger.logger import AppLogger


@pytest.fixture
def logger(tmp_path: Path) -> AppLogger:
    """
    Create logger with isolated temp directory.
    """
    logger_name = f"test.logger.{uuid.uuid4()}"

    logger = AppLogger(logger_name)

    # Remove handlers created during initialization
    for handler in logger.logger.handlers[:]:
        logger.logger.removeHandler(handler)
        handler.close()

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    app_log_file = tmp_path / "app.log"
    error_log_file = tmp_path / "error.log"

    file_handler = logging.FileHandler(app_log_file, encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    error_file_handler = logging.FileHandler(error_log_file, encoding="utf-8")
    error_file_handler.setLevel(logging.ERROR)
    error_file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    logger.logger.addHandler(file_handler)
    logger.logger.addHandler(error_file_handler)
    logger.logger.addHandler(console_handler)

    logger.log_file = app_log_file
    logger.error_log_file = error_log_file

    return logger


# =========================
# Initialization
# =========================

def test_logger_initializes_paths(logger: AppLogger):
    assert logger.log_file.name == "app.log"
    assert logger.error_log_file.name == "error.log"


# =========================
# Write Logs
# =========================

def test_write_info_logs_to_app_log(logger: AppLogger):
    logger.write("info message", "info")

    content = logger.get_log_text()
    assert "info message" in content


def test_write_error_logs_to_both_logs(logger: AppLogger):
    logger.write("error message", "error")

    app_content = logger.get_log_text()
    error_content = logger.get_error_log_text()

    assert "error message" in app_content
    assert "error message" in error_content


def test_write_warning_does_not_go_to_error_log(logger: AppLogger):
    logger.write("warning message", "warning")

    error_content = logger.get_error_log_text()

    assert "warning message" not in error_content


# =========================
# Exception Logging
# =========================

def test_exception_logs_traceback(logger: AppLogger):
    try:
        raise ValueError("test exception")
    except Exception:
        logger.exception("exception occurred")

    error_content = logger.get_error_log_text()

    assert "exception occurred" in error_content
    assert "ValueError" in error_content


# =========================
# Wipe
# =========================

def test_wipe_clears_both_logs(logger: AppLogger):
    logger.write("test message", "info")
    logger.write("error message", "error")

    logger.wipe()

    assert logger.get_log_text() == ""
    assert logger.get_error_log_text() == ""


# =========================
# Retrieval Methods
# =========================

def test_get_log_text_returns_empty_if_missing(tmp_path: Path):
    logger = AppLogger(f"test.logger.{uuid.uuid4()}")
    logger.log_file = tmp_path / "missing.log"

    assert logger.get_log_text() == ""


def test_get_error_log_text_returns_empty_if_missing(tmp_path: Path):
    logger = AppLogger(f"test.logger.{uuid.uuid4()}")
    logger.error_log_file = tmp_path / "missing_error.log"

    assert logger.get_error_log_text() == ""


def test_get_paths(logger: AppLogger):
    assert isinstance(logger.get_log_path(), Path)
    assert isinstance(logger.get_error_log_path(), Path)