from __future__ import annotations
import io
from dataclasses import dataclass
from typing import Any
import pandas as pd
from app.logger.logger import AppLogger


class IQCalculation:
    """
    Service responsible for:
    - Receiving raw derivatives CSV bytes
    - Validating IQ calculation input
    - Computing Manhattan IQ tables
    - Returning IQ result tables for downstream processing

    Internal:
    - logger (AppLogger): logger instance for IQ calculation operations
    """
    # Required columns for validation
    REQUIRED_COLUMNS = [
        "RptDt",
        "Asst",
        "SgmtNm",
        "BrrwrQty",
        "LndrQty",
    ]

    @dataclass(frozen=True)
    class IQColumns:
        date: str = "RptDt"
        asset: str = "Asst"
        segment: str = "SgmtNm"
        borrower: str = "BrrwrQty"
        lender: str = "LndrQty"

    def __init__(self):
        """
        Initialize the IQ calculation service.
        """
        # Initialize logger
        self.logger = AppLogger("automation.iq_calculation")

    # =========================
    # Internal helper methods
    # =========================

    def _validate_csv_bytes(self, csv_bytes: bytes) -> None:
        """
        Validate raw CSV byte payload before calculation.

        Args:
            csv_bytes (bytes): Raw CSV content

        Returns:
            None
        """
        # Validate type
        if not isinstance(csv_bytes, bytes):
            raise ValueError("csv_bytes must be raw bytes.")

        # Validate not empty
        if len(csv_bytes) == 0:
            raise ValueError(
                "Empty CSV payload. Expected derivatives open position CSV bytes."
        )

    def _normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize column names by stripping whitespace.

        Args:
            df (pd.DataFrame): Raw input DataFrame

        Returns:
            pd.DataFrame: DataFrame with normalized column names
        """
        df = df.copy()
        df.columns = [str(col).strip() for col in df.columns]
        return df

    def _validate_columns(self, df: pd.DataFrame) -> None:
        """
        Validate that all required columns exist in the DataFrame.

        Args:
            df (pd.DataFrame): DataFrame to validate

        Returns:
            None
        """
        missing = [c for c in self.REQUIRED_COLUMNS if c not in df.columns]

        if missing:
            raise ValueError(
                "CSV schema not recognized for 'Posições em aberto em Derivativos (Listado)'. "
                f"Missing columns: {missing}. "
                f"Received columns: {list(df.columns)}"
            )

    def _to_num(self, series: pd.Series) -> pd.Series:
        """
        Convert Brazilian-formatted numeric strings to floats.

        Args:
            series (pd.Series): Series with numeric string values

        Returns:
            pd.Series: Numeric series with NaNs replaced by 0.0
        """
        s = series.astype(str).str.strip()
        s = s.str.replace(".", "", regex=False)
        s = s.str.replace(",", ".", regex=False)
        return pd.to_numeric(s, errors="coerce").fillna(0.0)

    def _compute_iq_tables(self, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Compute IQ tables from a prepared DataFrame.

        Args:
            df (pd.DataFrame): Normalized and validated input DataFrame

        Returns:
            tuple[pd.DataFrame, pd.DataFrame]:
                - iq_table
                - iq_coef_table
        """
        self.logger.write("Starting IQ table computation")

        cols = self.IQColumns()

        # Keep only equity options
        opt = df[df[cols.segment].isin(["EQUITY CALL", "EQUITY PUT"])].copy()

        if opt.empty:
            raise ValueError(
                "No rows found where SgmtNm is 'EQUITY CALL' or 'EQUITY PUT'. "
                "Confirm the file is the listed derivatives open position dataset."
            )

        # Normalize and prepare numeric fields
        norm = pd.DataFrame({
            "Asset": opt[cols.asset].astype(str).str.strip(),
            "Date": opt[cols.date].astype(str).str.strip(),
            "Type": opt[cols.segment].map({
                "EQUITY CALL": "CALL",
                "EQUITY PUT": "PUT",
            }),
            "BorrowerQuantity": self._to_num(opt[cols.borrower]),
            "LenderQuantity": self._to_num(opt[cols.lender]),
        })

        # Aggregate positions per asset/date/type
        grouped = (
            norm.groupby(
                ["Asset", "Date", "Type"],
                as_index=False
            )[["BorrowerQuantity", "LenderQuantity"]]
            .sum()
        )

        # Compute IQ with safe division
        grouped["IQ"] = grouped.apply(
            lambda row: (
                row["BorrowerQuantity"] / row["LenderQuantity"]
                if row["LenderQuantity"] else pd.NA
            ),
            axis=1,
        )

        iq_table = grouped[["Asset", "Date", "Type", "IQ"]].copy()

        # Build IQ coefficient table
        call_df = (
            iq_table[iq_table["Type"] == "CALL"]
            [["Asset", "Date", "IQ"]]
            .rename(columns={"IQ": "IQ_call"})
        )

        put_df = (
            iq_table[iq_table["Type"] == "PUT"]
            [["Asset", "Date", "IQ"]]
            .rename(columns={"IQ": "IQ_put"})
        )

        iq_coef_table = call_df.merge(
            put_df,
            on=["Asset", "Date"],
            how="inner",
        )

        iq_coef_table["IQ_coef"] = iq_coef_table.apply(
            lambda row: (
                row["IQ_call"] / row["IQ_put"]
                if pd.notna(row["IQ_put"]) and row["IQ_put"] != 0
                else pd.NA
            ),
            axis=1,
        )

        # Final ordering for deterministic output
        iq_table = iq_table.sort_values(
            ["Date", "Asset", "Type"]
        ).reset_index(drop=True)

        iq_coef_table = iq_coef_table.sort_values(
            ["Date", "Asset"]
        ).reset_index(drop=True)

        self.logger.write(
            f"IQ table computation completed | rows_iq={len(iq_table)} | rows_coef={len(iq_coef_table)}"
        )

        return iq_table, iq_coef_table

    # =========================
    # Public methods
    # =========================

    def calculate_from_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate Manhattan IQ coefficient table from a DataFrame.

        Args:
            df (pd.DataFrame): Raw derivatives input DataFrame

        Returns:
            pd.DataFrame: IQ coefficient table
        """
        self.logger.write("Starting IQ calculation pipeline")

        # Validate input DataFrame
        self._validate_dataframe(df)

        # Normalize and validate schema
        df = self._normalize_columns(df)
        self._validate_columns(df)

        # Compute IQ tables
        _, iq_coef_table = self._compute_iq_tables(df)

        self.logger.write(
            f"IQ calculation pipeline completed | rows_coef={len(iq_coef_table)}"
        )

        return iq_coef_table