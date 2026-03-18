from pathlib import Path
from typing import Optional
from datetime import datetime, date, timedelta

import pandas as pd


class CalendarService:
    """
    Service responsible for:
    - Loading market calendar data
    - Validating target date
    - Providing trading-day utilities

    Internal:
    - calendar_path (Path): Path to the calendar CSV file
    - target_date (date): target date to run methods
    - calendar_df (df): loaded calendar
    """

    def __init__(self, calendar_path: Path, target_date: str):
        """
        Initialize the calendar service.

        Args:
            calendar_path (Path): Path to the calendar CSV file
            target_date (str): Target date in YYYY-MM-DD format
        """
        # Store calendar file path
        self.calendar_path = calendar_path

        # Initialize calendar DataFrame
        self.calendar_df: Optional[pd.DataFrame] = None

        # Validate target date format
        self._validate_target_date(target_date)

        # Convert target date to date
        self.target_date: date = datetime.strptime(target_date, "%Y-%m-%d").date()

        # Load calendar data
        self._load_calendar()

    # =========================
    # Internal setup methods
    # =========================

    def _load_calendar(self) -> None:
        """
        Load calendar CSV into DataFrame and store closed dates.
        """

        # Try loading CSV
        try:
            df = pd.read_csv(self.calendar_path)
        except Exception as e:
            raise ValueError(f"Failed to load calendar CSV: {e}")

        # Validate 'date' column exists
        if "date" not in df.columns:
            raise ValueError("Calendar CSV must contain a 'date' column")

        # Check for empty values in 'date'
        if df["date"].isnull().any():
            raise ValueError("Calendar CSV contains empty values in 'date' column")

        # Convert to datetime.date
        df["date"] = pd.to_datetime(df["date"]).dt.date

        # Store
        self.calendar_df = df

    def _validate_target_date(self, target_date: str) -> None:
        """
        Validate input date format.

        Args:
            target_date (str): Date in YYYY-MM-DD format
        """
        # Validate type
        if not isinstance(target_date, str):
            raise ValueError("target_date must be a string in 'YYYY-MM-DD' format")

        # Validate format
        try:
            datetime.strptime(target_date, "%Y-%m-%d")
        except ValueError:
            raise ValueError("target_date must be in 'YYYY-MM-DD' format")

    # =========================
    # Public methods
    # =========================

    def is_trading_day(self) -> bool:
        """
        Check if the stored target date is a trading day.

        Returns:
            bool: True if open, False otherwise
        """
        # Return False if weekend
        if self.target_date.weekday() >= 5:
            return False

        # Return False if in calendar
        if self.target_date in self.calendar_df["date"].values:
            return False

        # Default to True
        return True


    def get_previous_trading_day(self) -> Optional[date]:
        """
        Get the closest previous trading day before target_date.

        Returns:
            Optional[date]: Previous trading day or None if not found
        """
        current_date = self.target_date - timedelta(days=1)

        # Loop backwards until we find a trading day
        while True:
            # Weekend → skip
            if current_date.weekday() >= 5:
                current_date -= timedelta(days=1)
                continue

            # Holiday → skip
            if current_date in self.calendar_df["date"].values:
                current_date -= timedelta(days=1)
                continue

            # Found valid trading day
            return current_date

    def get_target_date(self) -> date:
        """
        Return the stored target date.

        Returns:
            date
        """
        return self.target_date