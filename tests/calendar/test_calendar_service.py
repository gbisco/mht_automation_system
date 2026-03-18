from pathlib import Path
from datetime import date

import pytest
from app.calendar.calendar_service import CalendarService


@pytest.fixture
def sample_calendar_path() -> Path:
    return Path(__file__).resolve().parent.parent / "fixtures" / "sample_calendar.csv"


def test_calendar_service_initializes_with_valid_inputs(sample_calendar_path: Path):
    service = CalendarService(
        calendar_path=sample_calendar_path,
        target_date="2026-01-02",
    )

    assert service.calendar_path == sample_calendar_path
    assert isinstance(service.target_date, date)
    assert service.target_date == date(2026, 1, 2)


def test_calendar_service_rejects_invalid_date_format(sample_calendar_path: Path):
    with pytest.raises(ValueError):
        CalendarService(
            calendar_path=sample_calendar_path,
            target_date="01-02-2026",
        )


def test_is_trading_day_returns_false_for_closed_holiday(sample_calendar_path: Path):
    service = CalendarService(
        calendar_path=sample_calendar_path,
        target_date="2026-01-01",
    )

    assert service.is_trading_day() is False


def test_is_trading_day_returns_false_for_saturday(sample_calendar_path: Path):
    service = CalendarService(
        calendar_path=sample_calendar_path,
        target_date="2026-01-03",  # Saturday
    )

    assert service.is_trading_day() is False


def test_is_trading_day_returns_false_for_sunday(sample_calendar_path: Path):
    service = CalendarService(
        calendar_path=sample_calendar_path,
        target_date="2026-01-04",  # Sunday
    )

    assert service.is_trading_day() is False


def test_is_trading_day_returns_true_for_regular_weekday(sample_calendar_path: Path):
    service = CalendarService(
        calendar_path=sample_calendar_path,
        target_date="2026-01-02",  # Friday, not holiday
    )

    assert service.is_trading_day() is True


def test_get_previous_trading_day_skips_weekend_and_holiday(sample_calendar_path: Path):
    service = CalendarService(
        calendar_path=sample_calendar_path,
        target_date="2026-01-20",  # Tuesday after MLK Day
    )

    previous_day = service.get_previous_trading_day()

    assert isinstance(previous_day, date)
    assert previous_day == date(2026, 1, 16)  # Friday


def test_get_previous_trading_day_skips_weekend(sample_calendar_path: Path):
    service = CalendarService(
        calendar_path=sample_calendar_path,
        target_date="2026-01-05",  # Monday
    )

    previous_day = service.get_previous_trading_day()

    assert isinstance(previous_day, date)
    assert previous_day == date(2026, 1, 2)  # Friday


def test_get_target_date_returns_date(sample_calendar_path: Path):
    service = CalendarService(
        calendar_path=sample_calendar_path,
        target_date="2026-02-17",
    )

    result = service.get_target_date()

    assert isinstance(result, date)
    assert result == date(2026, 2, 17)