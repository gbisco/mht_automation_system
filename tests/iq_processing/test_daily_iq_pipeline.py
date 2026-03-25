import pandas as pd
import pandas.testing as pdt
import pytest
from unittest.mock import MagicMock, patch

from app.iq_processing.daily_iq_pipeline import DailyIQPipeline


@pytest.fixture
def pipeline():
    return DailyIQPipeline()


# =========================
# __init__
# =========================

def test_init_builds_dependencies():
    pipeline = DailyIQPipeline()

    assert pipeline.DEFAULT_FILE_NAME == "DerivativesOpenPositionFile"
    assert pipeline.logger is not None
    assert pipeline.fetcher is not None
    assert pipeline.calculator is not None


# =========================
# _convert_csv_bytes_to_dataframe
# =========================

def test_convert_csv_bytes_to_dataframe_returns_dataframe(pipeline):
    content = (
        b"RptDt;TckrSymb;SgmtNm;OpnIntrst\n"
        b"2026-03-19;ABCB;EQUITY CALL;100\n"
        b"2026-03-19;ABEV;EQUITY PUT;200\n"
    )

    result = pipeline._convert_csv_bytes_to_dataframe(content)

    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == ["RptDt", "TckrSymb", "SgmtNm", "OpnIntrst"]
    assert len(result) == 2
    assert result.loc[0, "TckrSymb"] == "ABCB"
    assert result.loc[1, "TckrSymb"] == "ABEV"


def test_convert_csv_bytes_to_dataframe_keeps_values_as_strings(pipeline):
    content = (
        b"RptDt;TckrSymb;OpnIntrst\n"
        b"2026-03-19;ABCB;00123\n"
    )

    result = pipeline._convert_csv_bytes_to_dataframe(content)

    assert result.loc[0, "OpnIntrst"] == "00123"
    assert isinstance(result.loc[0, "OpnIntrst"], str)


def test_convert_csv_bytes_to_dataframe_keeps_empty_values_as_empty_string(pipeline):
    content = (
        b"RptDt;TckrSymb;OpnIntrst\n"
        b"2026-03-19;ABCB;\n"
    )

    result = pipeline._convert_csv_bytes_to_dataframe(content)

    assert result.loc[0, "OpnIntrst"] == ""


# =========================
# _build_output_filename
# =========================

def test_build_output_filename_returns_expected_name(pipeline):
    result = pipeline._build_output_filename("2026-03-19")

    assert result == "iq_coef_20260319.csv"


# =========================
# run
# =========================

def test_run_returns_expected_result_with_download_name(pipeline):
    target_date = "2026-03-19"
    raw_bytes = (
        b"RptDt;TckrSymb;SgmtNm;OpnIntrst\n"
        b"2026-03-19;ABCB;EQUITY CALL;100\n"
    )

    converted_df = pd.DataFrame(
        {
            "RptDt": ["2026-03-19"],
            "TckrSymb": ["ABCB"],
            "SgmtNm": ["EQUITY CALL"],
            "OpnIntrst": ["100"],
        }
    )

    iq_coef_df = pd.DataFrame(
        {
            "Asset": ["ABCB"],
            "Date": ["2026-03-19"],
            "IQ_call": [0.7],
            "IQ_put": [0.2],
            "IQ_coef": [3.5],
        }
    )

    fetch_result = {
        "content": raw_bytes,
        "download_name": "DerivativesOpenPositionFile_20260319.csv",
    }

    with patch.object(pipeline.fetcher, "fetch", return_value=fetch_result) as mock_fetch, \
         patch.object(
             pipeline,
             "_convert_csv_bytes_to_dataframe",
             return_value=converted_df,
         ) as mock_convert, \
         patch.object(
             pipeline.calculator,
             "calculate_from_dataframe",
             return_value=iq_coef_df,
         ) as mock_calculate:

        result = pipeline.run(target_date)

    mock_fetch.assert_called_once_with(
        file_name="DerivativesOpenPositionFile",
        date_str="2026-03-19",
    )
    mock_convert.assert_called_once_with(raw_bytes)
    mock_calculate.assert_called_once_with(converted_df)

    assert result["csv_content"] == iq_coef_df.to_csv(index=False)
    assert result["file_name"] == "iq_coef_20260319.csv"
    assert result["request_date"] == "2026-03-19"
    assert result["raw_b3_content"] == raw_bytes
    assert result["raw_b3_file_name"] == "DerivativesOpenPositionFile_20260319.csv"


def test_run_uses_default_raw_b3_file_name_when_download_name_missing(pipeline):
    target_date = "2026-03-19"
    raw_bytes = (
        b"RptDt;TckrSymb;SgmtNm;OpnIntrst\n"
        b"2026-03-19;ABCB;EQUITY CALL;100\n"
    )

    converted_df = pd.DataFrame(
        {
            "RptDt": ["2026-03-19"],
            "TckrSymb": ["ABCB"],
            "SgmtNm": ["EQUITY CALL"],
            "OpnIntrst": ["100"],
        }
    )

    iq_coef_df = pd.DataFrame(
        {
            "Asset": ["ABCB"],
            "Date": ["2026-03-19"],
            "IQ_call": [0.7],
            "IQ_put": [0.2],
            "IQ_coef": [3.5],
        }
    )

    fetch_result = {
        "content": raw_bytes,
    }

    with patch.object(pipeline.fetcher, "fetch", return_value=fetch_result), \
         patch.object(
             pipeline,
             "_convert_csv_bytes_to_dataframe",
             return_value=converted_df,
         ), \
         patch.object(
             pipeline.calculator,
             "calculate_from_dataframe",
             return_value=iq_coef_df,
         ):

        result = pipeline.run(target_date)

    assert result["raw_b3_file_name"] == "DerivativesOpenPositionFile_2026-03-19.csv"


def test_run_passes_dataframe_from_converter_to_calculator(pipeline):
    target_date = "2026-03-19"
    raw_bytes = b"dummy-bytes"

    converted_df = pd.DataFrame(
        {
            "RptDt": ["2026-03-19"],
            "TckrSymb": ["ABCB"],
        }
    )

    iq_coef_df = pd.DataFrame(
        {
            "Asset": ["ABCB"],
            "Date": ["2026-03-19"],
            "IQ_call": [0.7],
            "IQ_put": [0.2],
            "IQ_coef": [3.5],
        }
    )

    with patch.object(
        pipeline.fetcher,
        "fetch",
        return_value={"content": raw_bytes, "download_name": "file.csv"},
    ), patch.object(
        pipeline,
        "_convert_csv_bytes_to_dataframe",
        return_value=converted_df,
    ), patch.object(
        pipeline.calculator,
        "calculate_from_dataframe",
        return_value=iq_coef_df,
    ) as mock_calculate:

        pipeline.run(target_date)

    mock_calculate.assert_called_once_with(converted_df)


def test_run_returns_csv_from_calculated_dataframe(pipeline):
    target_date = "2026-03-19"
    raw_bytes = b"dummy-bytes"

    iq_coef_df = pd.DataFrame(
        {
            "Asset": ["ABCB", "ABEV"],
            "Date": ["2026-03-19", "2026-03-19"],
            "IQ_call": [0.7, 0.6],
            "IQ_put": [0.2, 0.9],
            "IQ_coef": [3.5, 0.666667],
        }
    )

    with patch.object(
        pipeline.fetcher,
        "fetch",
        return_value={"content": raw_bytes, "download_name": "file.csv"},
    ), patch.object(
        pipeline,
        "_convert_csv_bytes_to_dataframe",
        return_value=pd.DataFrame({"dummy": ["value"]}),
    ), patch.object(
        pipeline.calculator,
        "calculate_from_dataframe",
        return_value=iq_coef_df,
    ):

        result = pipeline.run(target_date)

    expected_csv = iq_coef_df.to_csv(index=False)
    assert result["csv_content"] == expected_csv


def test_run_propagates_fetch_error(pipeline):
    with patch.object(
        pipeline.fetcher,
        "fetch",
        side_effect=RuntimeError("fetch failed"),
    ):
        with pytest.raises(RuntimeError, match="fetch failed"):
            pipeline.run("2026-03-19")


def test_run_propagates_calculation_error(pipeline):
    with patch.object(
        pipeline.fetcher,
        "fetch",
        return_value={"content": b"dummy-bytes", "download_name": "file.csv"},
    ), patch.object(
        pipeline,
        "_convert_csv_bytes_to_dataframe",
        return_value=pd.DataFrame({"dummy": ["value"]}),
    ), patch.object(
        pipeline.calculator,
        "calculate_from_dataframe",
        side_effect=RuntimeError("calculation failed"),
    ):
        with pytest.raises(RuntimeError, match="calculation failed"):
            pipeline.run("2026-03-19")