from __future__ import annotations

from dataclasses import dataclass
import pandas as pd

from app.logger.logger import AppLogger

class IQDeltaProcessor:
    INPUT_REQUIRED_COLUMNS = [
        "Asset",
        "Date",
        "IQ_call",
        "IQ_put",
        "IQ_coef",
    ]

    @dataclass(frozen=True)
    class DeltaColumns:
        asset: str = "Asset"
        current_date: str = "IQ_Current_Date"
        previous_date: str = "IQ_Prev_Date"
        iq_call_current: str = "IQ_call_current"
        iq_call_prev: str = "IQ_call_prev"
        iq_put_current: str = "IQ_put_current"
        iq_put_prev: str = "IQ_put_prev"
        iq_coef_current: str = "IQ_coef_current"
        iq_coef_prev: str = "IQ_coef_prev"
        delta_iq: str = "Delta_IQ"
        pct_change: str = "Pct_change"

    def __init__(self):
        self.logger = AppLogger("automation.iq_delta_processor")

    def _validate_dataframe(
        self,
        df: pd.DataFrame,
        df_name: str,
    ) -> None:
        """
        Validate that the provided object is a non-empty pandas DataFrame.

        This method ensures the input exists and is structurally valid
        before any schema-specific validation is performed.

        Args:
            df (pd.DataFrame): Input dataframe to validate.
            df_name (str): Friendly dataframe name used in logs and errors.

        Raises:
            ValueError: If dataframe is None or empty.
            TypeError: If input is not a pandas DataFrame.

        Returns:
            None
        """
        self.logger.write(f"[{df_name}] Validating dataframe object")

        if df is None:
            self.logger.write(f"[{df_name}] Dataframe is None", level="error")
            raise ValueError(f"{df_name} dataframe cannot be None")

        if not isinstance(df, pd.DataFrame):
            self.logger.write(
                f"[{df_name}] Invalid type: {type(df).__name__}",
                level="error",
            )
            raise TypeError(
                f"{df_name} must be a pandas DataFrame, got {type(df).__name__}"
            )

        if df.empty:
            self.logger.write(f"[{df_name}] Dataframe is empty", level="error")
            raise ValueError(f"{df_name} dataframe cannot be empty")

        self.logger.write(
            f"[{df_name}] Dataframe validated successfully | rows={len(df)}"
        )


    def _normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize dataframe column names by stripping leading and trailing
        whitespace from each column header.

        Args:
            df (pd.DataFrame): Input dataframe whose columns should be normalized.

        Returns:
            pd.DataFrame: Copy of the dataframe with normalized column names.
        """
        self.logger.write("Normalizing dataframe columns")

        normalized_df = df.copy()
        normalized_df.columns = normalized_df.columns.str.strip()

        self.logger.write(
            f"Column normalization complete | columns={list(normalized_df.columns)}"
        )
        return normalized_df


    def _validate_columns(self, df: pd.DataFrame, df_name: str) -> None:
        """
        Validate that the dataframe contains all required IQ coefficient columns.

        Expected columns:
        - Asset
        - Date
        - IQ_call
        - IQ_put
        - IQ_coef

        Args:
            df (pd.DataFrame): Dataframe to validate.
            df_name (str): Friendly dataframe name used in logs and error messages.

        Raises:
            ValueError: If one or more required columns are missing.

        Returns:
            None
        """
        self.logger.write(f"[{df_name}] Validating required columns")

        missing_columns = [
            column for column in self.REQUIRED_COLUMNS
            if column not in df.columns
        ]

        if missing_columns:
            self.logger.write(
                f"[{df_name}] Missing required columns: {missing_columns}",
                level="error",
            )
            raise ValueError(
                f"{df_name} is missing required columns: {missing_columns}"
            )

        self.logger.write(f"[{df_name}] Required columns validated successfully")


    def _prepare_dataframe(
        self,
        df: pd.DataFrame,
        df_name: str,
    ) -> pd.DataFrame:
        """
        Validate, normalize, and prepare an IQ dataframe for delta comparison.

        Preparation steps:
        - Validate dataframe object
        - Normalize column names
        - Validate required columns
        - Strip whitespace from asset values
        - Normalize decimal separators in numeric columns
        - Coerce numeric IQ columns
        - Validate unique asset rows

        Args:
            df (pd.DataFrame): Input dataframe.
            df_name (str): Friendly dataframe name used in logs and errors.

        Raises:
            ValueError: If validation or preparation fails.
            TypeError: If input is not a pandas DataFrame.

        Returns:
            pd.DataFrame: Prepared dataframe ready for delta comparison.
        """
        self.logger.write(f"[{df_name}] Preparing dataframe for delta calculation")

        self._validate_dataframe(df, df_name)
        prepared_df = self._normalize_columns(df)
        self._validate_columns(prepared_df, df_name)

        prepared_df = prepared_df.copy()
        prepared_df["Asset"] = prepared_df["Asset"].astype(str).str.strip()

        numeric_columns = ["IQ_call", "IQ_put", "IQ_coef"]

        for column in numeric_columns:
            prepared_df[column] = (
                prepared_df[column]
                .astype(str)
                .str.strip()
                .str.replace(",", ".", regex=False)
            )

            prepared_df[column] = pd.to_numeric(prepared_df[column], errors="coerce")

            if prepared_df[column].isna().any():
                self.logger.write(
                    f"[{df_name}] Invalid numeric values found in column: {column}",
                    level="error",
                )
                raise ValueError(
                    f"{df_name} contains invalid numeric values in column: {column}"
                )

        duplicate_assets = prepared_df[
            prepared_df["Asset"].duplicated()
        ]["Asset"].tolist()

        if duplicate_assets:
            self.logger.write(
                f"[{df_name}] Duplicate assets found: {duplicate_assets}",
                level="error",
            )
            raise ValueError(
                f"{df_name} contains duplicate asset rows: {duplicate_assets}"
            )

        self.logger.write(f"[{df_name}] Dataframe preparation completed successfully")
        return prepared_df

    def calculate(
        self,
        current_df: pd.DataFrame,
        previous_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Calculate IQ delta dataframe from current and previous IQ coefficient dataframes.

        The resulting dataframe contains current and previous IQ values for each
        asset, along with Delta_IQ and Pct_change.

        Merge behavior:
        - Left join from current_df to previous_df on Asset
        - Assets present in current_df but missing in previous_df are retained
        - Delta_IQ and Pct_change will be NaN where previous values are missing

        Args:
            current_df (pd.DataFrame): Current IQ coefficient dataframe.
            previous_df (pd.DataFrame): Previous IQ coefficient dataframe.

        Raises:
            ValueError: If dataframe validation or preparation fails.

        Returns:
            pd.DataFrame: Delta dataframe including previous/current IQ values,
            delta, and percent change.
        """
        self.logger.write("Starting IQ delta calculation")

        current_prepared = self._prepare_dataframe(current_df, "current_df")
        previous_prepared = self._prepare_dataframe(previous_df, "previous_df")

        merged_df = current_prepared.merge(
            previous_prepared,
            on="Asset",
            how="left",
            suffixes=("_current", "_prev"),
        )

        output_columns = self.DeltaColumns()

        result_df = pd.DataFrame()

        result_df[output_columns.asset] = merged_df["Asset"]
        result_df[output_columns.current_date] = merged_df["Date_current"]
        result_df[output_columns.previous_date] = merged_df["Date_prev"]

        result_df[output_columns.iq_call_current] = merged_df["IQ_call_current"]
        result_df[output_columns.iq_call_prev] = merged_df["IQ_call_prev"]

        result_df[output_columns.iq_put_current] = merged_df["IQ_put_current"]
        result_df[output_columns.iq_put_prev] = merged_df["IQ_put_prev"]

        result_df[output_columns.iq_coef_current] = merged_df["IQ_coef_current"]
        result_df[output_columns.iq_coef_prev] = merged_df["IQ_coef_prev"]

        result_df[output_columns.delta_iq] = (
            result_df[output_columns.iq_coef_current]
            - result_df[output_columns.iq_coef_prev]
        )

        result_df[output_columns.pct_change] = (
            result_df[output_columns.delta_iq]
            / result_df[output_columns.iq_coef_prev]
        )

        result_df.loc[
            result_df[output_columns.iq_coef_prev].isna(),
            output_columns.pct_change,
        ] = pd.NA

        result_df.loc[
            result_df[output_columns.iq_coef_prev] == 0,
            output_columns.pct_change,
        ] = pd.NA

        self.logger.write(
            f"IQ delta calculation completed successfully | rows={len(result_df)}"
        )
        return result_df