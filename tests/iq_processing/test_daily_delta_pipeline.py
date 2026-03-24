from io import StringIO
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pandas.testing as pdt
import pytest

from app.iq_processing.daily_delta_pipeline import DailyIQDeltaPipeline


@pytest.fixture
def pipeline():
    return DailyIQDeltaPipeline()


@pytest.fixture
def fixtures_dir():
    return Path("tests/fixtures")


@pytest.fixture
def current_csv(fixtures_dir):
    return (fixtures_dir / "iq_coef_20260320.csv").read_text(encoding="utf-8")


@pytest.fixture
def previous_csv(fixtures_dir):
    return (fixtures_dir / "iq_coef_20260319.csv").read_text(encoding="utf-8")


@pytest.fixture
def expected_df(fixtures_dir):
    return pd.read_csv(fixtures_dir / "iq_delta_expected.csv")


# =========================
# _read_csv_to_dataframe
# =========================

def test_read_csv_to_dataframe_returns_dataframe(pipeline, current_csv):
    result = pipeline._read_csv_to_dataframe(current_csv, "current_csv")

    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == ["Asset", "Date", "IQ_call", "IQ_put", "IQ_coef"]
    assert len(result) > 0


# =========================
# _convert_dataframe_to_csv
# =========================

def test_convert_dataframe_to_csv_returns_csv_string(pipeline):
    df = pd.DataFrame(
        {
            "Asset": ["ABCB"],
            "IQ_Current_Date": ["2026-03-20"],
            "IQ_Prev_Date": ["2026-03-19"],
            "IQ_call_current": [0.716216],
            "IQ_call_prev": [0.401961],
            "IQ_put_current": [0.220779],
            "IQ_put_prev": [0.191053],
            "IQ_coef_current": [3.244038],
            "IQ_coef_prev": [2.103929],
            "Delta_IQ": [1.140109],
            "Pct_change": [0.541896],
        }
    )

    result = pipeline._convert_dataframe_to_csv(df)

    assert isinstance(result, str)
    assert "Asset,IQ_Current_Date,IQ_Prev_Date" in result
    assert "ABCB" in result


# =========================
# get_output_filename
# =========================

def test_get_output_filename_returns_generic_overwriteable_name(pipeline):
    result = pipeline.get_output_filename()

    assert result == "iq_delta_latest.csv"


# =========================
# run
# =========================

def test_run_calls_internal_pipeline_steps(pipeline, current_csv, previous_csv):
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
            "IQ_call": [0.401961],
            "IQ_put": [0.191053],
            "IQ_coef": [2.103929],
        }
    )

    delta_df = pd.DataFrame(
        {
            "Asset": ["ABCB"],
            "IQ_Current_Date": ["2026-03-20"],
            "IQ_Prev_Date": ["2026-03-19"],
            "IQ_call_current": [0.716216],
            "IQ_call_prev": [0.401961],
            "IQ_put_current": [0.220779],
            "IQ_put_prev": [0.191053],
            "IQ_coef_current": [3.244038],
            "IQ_coef_prev": [2.103929],
            "Delta_IQ": [1.140109],
            "Pct_change": [0.541896],
        }
    )

    with patch.object(
        pipeline,
        "_read_csv_to_dataframe",
        side_effect=[current_df, previous_df],
    ) as mock_read, patch.object(
        pipeline.processor,
        "calculate",
        return_value=delta_df,
    ) as mock_calculate, patch.object(
        pipeline,
        "_convert_dataframe_to_csv",
        return_value="mock_csv_output",
    ) as mock_convert:
        result = pipeline.run(current_csv, previous_csv)

    assert result == "mock_csv_output"
    assert mock_read.call_count == 2
    mock_calculate.assert_called_once_with(current_df, previous_df)
    mock_convert.assert_called_once_with(delta_df)


def test_run_returns_expected_csv_output_from_fixtures(
    pipeline,
    current_csv,
    previous_csv,
    expected_df,
):
    result_csv = pipeline.run(current_csv, previous_csv)
    result_df = pd.read_csv(StringIO(result_csv))

    result_df = result_df.sort_values("Asset").reset_index(drop=True)
    expected_df = expected_df.sort_values("Asset").reset_index(drop=True)

    pdt.assert_frame_equal(result_df, expected_df, check_dtype=False)