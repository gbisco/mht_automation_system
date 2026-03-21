from __future__ import annotations

import io
from typing import Any

import pandas as pd

from app.logger.logger import AppLogger
from app.iq_processing.b3_fetcher import B3Fetcher
from app.iq_processing.iq_calculation import IQCalculation


class DailyIQPipeline:
    """
    Service responsible for:
    - Fetching B3 derivatives data for a given date
    - Converting raw CSV bytes into DataFrame
    - Computing IQ coefficient table
    - Returning file-ready CSV content

    Internal:
    - logger (AppLogger): logger instance for pipeline operations
    - fetcher (B3Fetcher): service to fetch B3 data
    - calculator (IQCalculation): service to compute IQ tables
    """

    DEFAULT_FILE_NAME = "DerivativesOpenPositionFile"

    def __init__(self):
        """
        Initialize the daily IQ pipeline.
        """
        self.logger = AppLogger("automation.daily_iq_pipeline")
        self.fetcher = B3Fetcher()
        self.calculator = IQCalculation()

    # =========================
    # Internal helper methods
    # =========================

    def _convert_csv_bytes_to_dataframe(self, content: bytes) -> pd.DataFrame:
        """
        Convert raw CSV bytes into a pandas DataFrame.

        Args:
            content (bytes): Raw CSV file content

        Returns:
            pd.DataFrame
        """
        return pd.read_csv(
            io.BytesIO(content),
            sep=";",
            dtype=str,
            keep_default_na=False,
            encoding="utf-8-sig",
        )

    def _build_output_filename(self, target_date: str) -> str:
        """
        Build output file name for IQ result.

        Args:
            target_date (str): Date in YYYY-MM-DD format

        Returns:
            str
        """
        return f"iq_coef_{target_date.replace('-', '')}.csv"

    # =========================
    # Public methods
    # =========================

    def run(self, target_date: str) -> dict[str, Any]:
        """
        Execute daily IQ pipeline for a given date.

        Args:
            target_date (str): Target date in YYYY-MM-DD format

        Returns:
            dict[str, Any]: CSV content and metadata
        """
        self.logger.write(f"Starting daily IQ pipeline | date={target_date}")

        # Step 1: Fetch B3 file
        fetch_result = self.fetcher.fetch(
            file_name=self.DEFAULT_FILE_NAME,
            date_str=target_date,
        )

        # Step 2: Convert to DataFrame
        df = self._convert_csv_bytes_to_dataframe(fetch_result["content"])

        # Step 3: Calculate IQ
        iq_coef_df = self.calculator.calculate_from_dataframe(df)

        # Step 4: Convert to CSV
        csv_content = iq_coef_df.to_csv(index=False)

        # Step 5: Build IQ output file name
        file_name = self._build_output_filename(target_date)

        # Step 6: Get raw B3 metadata
        raw_b3_content = fetch_result["content"]
        raw_b3_file_name = fetch_result.get("download_name", f"{self.DEFAULT_FILE_NAME}_{target_date}.csv")

        self.logger.write(
            f"Daily IQ pipeline completed | date={target_date} | rows={len(iq_coef_df)}"
        )

        return {
            "csv_content": csv_content,
            "file_name": file_name,
            "request_date": target_date,
            "raw_b3_content": raw_b3_content,
            "raw_b3_file_name": raw_b3_file_name,
        }