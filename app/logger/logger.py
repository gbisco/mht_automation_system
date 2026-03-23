"""
Simple Logger Wrapper

Purpose:
    - Centralized logging setup
    - Simple interface: write(), exception(), wipe()
    - Persist all logs to app.log
    - Persist error and exception logs to error.log
"""

import logging
from pathlib import Path


class AppLogger:
    def __init__(self, name: str):
        """
        Initialize a logger.

        Args:
            name: logger name (e.g. "automation.b3_fetcher")
        """
        base_dir = Path(__file__).resolve().parent
        log_dir = base_dir / "logs"
        log_file = log_dir / "app.log"
        error_log_file = log_dir / "error.log"

        log_dir.mkdir(parents=True, exist_ok=True)

        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)

        # Prevent duplicate handlers
        if not self.logger.handlers:
            formatter = logging.Formatter(
                "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
            )

            # Main application log: stores all log levels
            file_handler = logging.FileHandler(log_file, encoding="utf-8")
            file_handler.setLevel(logging.INFO)
            file_handler.setFormatter(formatter)

            # Error log: stores only errors and critical/exception logs
            error_file_handler = logging.FileHandler(error_log_file, encoding="utf-8")
            error_file_handler.setLevel(logging.ERROR)
            error_file_handler.setFormatter(formatter)

            # Console output: stores all log levels
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            console_handler.setFormatter(formatter)

            self.logger.addHandler(file_handler)
            self.logger.addHandler(error_file_handler)
            self.logger.addHandler(console_handler)

        self.log_file = log_file
        self.error_log_file = error_log_file

    def write(self, message: str, level: str = "info") -> None:
        """
        Write a log message.

        Levels:
            info, warning, error, critical
        """
        level = level.lower()

        if level == "info":
            self.logger.info(message)
        elif level == "warning":
            self.logger.warning(message)
        elif level == "error":
            self.logger.error(message)
        elif level == "critical":
            self.logger.critical(message)
        else:
            self.logger.info(message)

    def exception(self, message: str) -> None:
        """
        Log exception with traceback.
        """
        self.logger.exception(message)

    def wipe(self) -> None:
        """
        Clear the main and error log files.
        """
        if self.log_file.exists():
            self.log_file.write_text("", encoding="utf-8")

        if self.error_log_file.exists():
            self.error_log_file.write_text("", encoding="utf-8")

    def get_log_text(self) -> str:
        """
        Return the full contents of the main application log file.
        """
        if not self.log_file.exists():
            return ""
        return self.log_file.read_text(encoding="utf-8")

    def get_error_log_text(self) -> str:
        """
        Return the full contents of the error log file.
        """
        if not self.error_log_file.exists():
            return ""
        return self.error_log_file.read_text(encoding="utf-8")

    def get_log_path(self) -> Path:
        """
        Return the path to the main application log file.
        """
        return self.log_file

    def get_error_log_path(self) -> Path:
        """
        Return the path to the error log file.
        """
        return self.error_log_file