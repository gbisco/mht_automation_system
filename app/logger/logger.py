"""
Simple Logger Wrapper

Purpose:
    - Centralized logging setup
    - Simple interface: write(), exception(), wipe()
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

        log_dir.mkdir(parents=True, exist_ok=True)

        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)

        # Prevent duplicate handlers
        if not self.logger.handlers:
            formatter = logging.Formatter(
                "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
            )

            file_handler = logging.FileHandler(log_file, encoding="utf-8")
            file_handler.setFormatter(formatter)

            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)

            self.logger.addHandler(file_handler)
            self.logger.addHandler(console_handler)

        self.log_file = log_file

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
        Clear the log file.
        """
        if self.log_file.exists():
            self.log_file.write_text("")