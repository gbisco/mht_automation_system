import pandas as pd
import pytest

from app.iq_processing.iq_delta_processor import IQDeltaProcessor


@pytest.fixture
def processor():
    return IQDeltaProcessor()


# =========================
# _validate_dataframe
# =========================

def test_validate_dataframe_raises_for_none(processor):
    with pytest.raises(ValueError, match="current_df dataframe cannot be None"):
        processor._validate_dataframe(None, "current_df")


def test_validate_dataframe_raises_for_non_dataframe(processor):
    with pytest.raises(TypeError, match="current_df must be a pandas DataFrame"):
        processor._validate_dataframe(["not", "a", "df"], "current_df")


def test_validate_dataframe_raises_for_empty_dataframe(processor):
    empty_df = pd.DataFrame()

    with pytest.raises(ValueError, match="current_df dataframe cannot be empty"):
        processor._validate_dataframe(empty_df, "current_df")


def test_validate_dataframe_passes_for_valid_dataframe(processor):
    df = pd.DataFrame({"Asset": ["ABCB"], "IQ_coef": [1.23]})
    processor._validate_dataframe(df, "current_df")


# =========================
# _normalize_columns
# =========================

def test_normalize_columns_strips_whitespace(processor):
    df = pd.DataFrame(columns=[" Asset ", " Date ", " IQ_call "])

    result = processor._normalize_columns(df)

    assert list(result.columns) == ["Asset", "Date", "IQ_call"]


# =========================
# _validate_columns
# =========================

def test_validate_columns_raises_for_missing_columns(processor):
    df = pd.DataFrame(columns=["Asset", "Date", "IQ_call"])

    with pytest.raises(ValueError, match="current_df is missing required columns"):
        processor._validate_columns(df, "current_df")


def test_validate_columns_passes_when_required_columns_exist(processor):
    df = pd.DataFrame(
        columns=["Asset", "Date", "IQ_call", "IQ_put", "IQ_coef"]
    )

    processor._validate_columns(df, "current_df")


# =========================
# _prepare_dataframe
# =========================

def test_prepare_dataframe_strips_asset_whitespace_and_converts_numeric(processor):
    df = pd.DataFrame(
        {
            " Asset ": [" ABCB "],
            " Date ": ["2026-03-20"],
            " IQ_call ": ["0,716216"],
            " IQ_put ": ["0,220779"],
            " IQ_coef ": ["3,244038"],
        }
    )

    result = processor._prepare_dataframe(df, "current_df")

    assert list(result.columns) == ["Asset", "Date", "IQ_call", "IQ_put", "IQ_coef"]
    assert result.loc[0, "Asset"] == "ABCB"
    assert result.loc[0, "IQ_call"] == pytest.approx(0.716216)
    assert result.loc[0, "IQ_put"] == pytest.approx(0.220779)
    assert result.loc[0, "IQ_coef"] == pytest.approx(3.244038)


def test_prepare_dataframe_raises_for_invalid_numeric_value(processor):
    df = pd.DataFrame(
        {
            "Asset": ["ABCB"],
            "Date": ["2026-03-20"],
            "IQ_call": ["abc"],
            "IQ_put": ["0.2"],
            "IQ_coef": ["1.5"],
        }
    )

    with pytest.raises(ValueError, match="invalid numeric values in column: IQ_call"):
        processor._prepare_dataframe(df, "current_df")


def test_prepare_dataframe_raises_for_duplicate_assets(processor):
    df = pd.DataFrame(
        {
            "Asset": ["ABCB", "ABCB"],
            "Date": ["2026-03-20", "2026-03-20"],
            "IQ_call": ["1.0", "1.1"],
            "IQ_put": ["0.5", "0.6"],
            "IQ_coef": ["2.0", "2.1"],
        }
    )

    with pytest.raises(ValueError, match="duplicate asset rows"):
        processor._prepare_dataframe(df, "current_df")


# =========================
# calculate
# =========================

def test_calculate_returns_expected_output(processor):
    current_df = pd.DataFrame(
        {
            "Asset": ["ABCB", "ABEV"],
            "Date": ["2026-03-20", "2026-03-20"],
            "IQ_call": [0.716216, 0.634328],
            "IQ_put": [0.220779, 0.911972],
            "IQ_coef": [3.244038, 0.695557],
        }
    )

    previous_df = pd.DataFrame(
        {
            "Asset": ["ABCB", "ABEV"],
            "Date": ["2026-03-19", "2026-03-19"],
            "IQ_call": [0.5, 0.7],
            "IQ_put": [0.2, 0.8],
            "IQ_coef": [2.103929, 0.912849],
        }
    )

    result = processor.calculate(current_df, previous_df)

    assert list(result.columns) == [
        "Asset",
        "IQ_Current_Date",
        "IQ_Prev_Date",
        "IQ_call_current",
        "IQ_call_prev",
        "IQ_put_current",
        "IQ_put_prev",
        "IQ_coef_current",
        "IQ_coef_prev",
        "Delta_IQ",
        "Pct_change",
    ]

    abcb_row = result[result["Asset"] == "ABCB"].iloc[0]
    assert abcb_row["IQ_Current_Date"] == "2026-03-20"
    assert abcb_row["IQ_Prev_Date"] == "2026-03-19"
    assert abcb_row["IQ_call_current"] == pytest.approx(0.716216)
    assert abcb_row["IQ_call_prev"] == pytest.approx(0.5)
    assert abcb_row["IQ_put_current"] == pytest.approx(0.220779)
    assert abcb_row["IQ_put_prev"] == pytest.approx(0.2)
    assert abcb_row["IQ_coef_current"] == pytest.approx(3.244038)
    assert abcb_row["IQ_coef_prev"] == pytest.approx(2.103929)
    assert abcb_row["Delta_IQ"] == pytest.approx(1.140109)
    assert abcb_row["Pct_change"] == pytest.approx(1.140109 / 2.103929)


def test_calculate_keeps_current_asset_when_previous_missing(processor):
    current_df = pd.DataFrame(
        {
            "Asset": ["ABCB"],
            "Date": ["2026-03-20"],
            "IQ_call": [0.716216],
            "IQ_put": [0.220779],
            "IQ_coef": [3.244038],
        }
    )

    previous_df = pd.DataFrame(
        {
            "Asset": ["OTHER"],
            "Date": ["2026-03-19"],
            "IQ_call": [0.1],
            "IQ_put": [0.2],
            "IQ_coef": [0.3],
        }
    )

    result = processor.calculate(current_df, previous_df)

    row = result.iloc[0]
    assert row["Asset"] == "ABCB"
    assert row["IQ_Current_Date"] == "2026-03-20"
    assert pd.isna(row["IQ_Prev_Date"])
    assert row["IQ_call_current"] == pytest.approx(0.716216)
    assert pd.isna(row["IQ_call_prev"])
    assert row["IQ_put_current"] == pytest.approx(0.220779)
    assert pd.isna(row["IQ_put_prev"])
    assert row["IQ_coef_current"] == pytest.approx(3.244038)
    assert pd.isna(row["IQ_coef_prev"])
    assert pd.isna(row["Delta_IQ"])
    assert pd.isna(row["Pct_change"])


def test_calculate_sets_pct_change_to_na_when_previous_coef_is_zero(processor):
    current_df = pd.DataFrame(
        {
            "Asset": ["ABCB"],
            "Date": ["2026-03-20"],
            "IQ_call": [0.716216],
            "IQ_put": [0.220779],
            "IQ_coef": [3.244038],
        }
    )

    previous_df = pd.DataFrame(
        {
            "Asset": ["ABCB"],
            "Date": ["2026-03-19"],
            "IQ_call": [0.1],
            "IQ_put": [0.2],
            "IQ_coef": [0.0],
        }
    )

    result = processor.calculate(current_df, previous_df)

    row = result.iloc[0]
    assert row["Delta_IQ"] == pytest.approx(3.244038)
    assert pd.isna(row["Pct_change"])