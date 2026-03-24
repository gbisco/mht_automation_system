from __future__ import annotations

from io import StringIO
import pandas as pd

from app.logger.logger import AppLogger
from app.iq_processing.iq_delta_processor import IQDeltaProcessor


class DailyIQDeltaPipeline:
    """
    Pipeline responsible for:
    - Receiving current and previous IQ coefficient CSV content
    - Parsing both CSV inputs into pandas DataFrames
    - Executing IQ delta calculation through IQDeltaProcessor
    - Converting the resulting delta DataFrame back into CSV
    - Returning the delta CSV content for downstream storage

    This pipeline does not handle:
    - Calendar logic
    - File retrieval
    - File upload
    - Date resolution
    """

    OUTPUT_FILENAME = "iq_delta_latest.csv"

    def __init__(self):
        self.logger = AppLogger("automation.daily_iq_delta_pipeline")
        self.processor = IQDeltaProcessor()

    def _read_csv_to_dataframe(self, csv_content: str, csv_name: str) -> pd.DataFrame:
        """
        Convert CSV content into a pandas DataFrame.

        Args:
            csv_content (str): Raw CSV content.
            csv_name (str): Friendly CSV name used in logs and errors.

        Returns:
            pd.DataFrame: Parsed dataframe.
        """
        self.logger.write(f"[{csv_name}] Reading CSV content into dataframe")
        return pd.read_csv(StringIO(csv_content))

    def _convert_dataframe_to_csv(self, df: pd.DataFrame) -> str:
        """
        Convert a pandas DataFrame into CSV string content.

        Args:
            df (pd.DataFrame): Dataframe to serialize.

        Returns:
            str: CSV string content.
        """
        self.logger.write("Converting delta dataframe to CSV content")
        return df.to_csv(index=False)

    def get_output_filename(self) -> str:
        """
        Return the generic overwriteable delta output filename.

        Returns:
            str: Output CSV filename.
        """
        return self.OUTPUT_FILENAME

    def run(
        self,
        current_csv: str,
        previous_csv: str,
    ) -> str:
        """
        Execute the daily IQ delta pipeline.

        Args:
            current_csv (str): Current IQ coefficient CSV content.
            previous_csv (str): Previous IQ coefficient CSV content.

        Returns:
            str: Delta CSV content.
        """
        self.logger.write("Starting daily IQ delta pipeline")

        current_df = self._read_csv_to_dataframe(current_csv, "current_csv")
        previous_df = self._read_csv_to_dataframe(previous_csv, "previous_csv")

        delta_df = self.processor.calculate(current_df, previous_df)
        delta_csv = self._convert_dataframe_to_csv(delta_df)

        self.logger.write("Daily IQ delta pipeline completed successfully")
        return delta_csv