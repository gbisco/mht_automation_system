from pathlib import Path
import pandas as pd
import pandas.testing as pdt
import pytest
from app.automation.iq_processing.iq_calculation import IQCalculation


@pytest.fixture
def calculator() -> IQCalculation:
    """
    Create IQCalculation service instance.
    """
    return IQCalculation()


@pytest.fixture
def raw_b3_dataframe() -> pd.DataFrame:
    """
    Load real B3 derivatives fixture as raw input DataFrame.
    """
    fixture_path = Path("tests/fixtures/b3_derivatives_2026_03_18.csv")

    return pd.read_csv(
        fixture_path,
        sep=";",
        dtype=str,
        keep_default_na=False,
        encoding="utf-8-sig",
    )


@pytest.fixture
def expected_iq_coef_dataframe() -> pd.DataFrame:
    """
    Load expected IQ coefficient output fixture.
    """
    fixture_path = Path("tests/fixtures/iq_coef_expected_2026_03_18.csv")

    df = pd.read_csv(
        fixture_path,
        dtype=str,
        keep_default_na=False,
        encoding="utf-8-sig",
    )

    # Convert numeric columns so comparisons are numeric, not string-based
    for col in ["IQ_call", "IQ_put", "IQ_coef"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def test_validate_dataframe_raises_for_non_dataframe(calculator: IQCalculation) -> None:
    """
    Validate _validate_dataframe raises for invalid input type.
    """
    with pytest.raises(ValueError, match="Input must be a pandas DataFrame."):
        calculator._validate_dataframe("not_a_dataframe")


def test_validate_dataframe_raises_for_empty_dataframe(calculator: IQCalculation) -> None:
    """
    Validate _validate_dataframe raises for empty DataFrame.
    """
    empty_df = pd.DataFrame()

    with pytest.raises(ValueError, match="Input DataFrame is empty."):
        calculator._validate_dataframe(empty_df)


def test_normalize_columns_strips_whitespace(calculator: IQCalculation) -> None:
    """
    Validate _normalize_columns strips whitespace from column names.
    """
    df = pd.DataFrame(columns=[" RptDt ", " Asst ", " SgmtNm "])

    normalized = calculator._normalize_columns(df)

    assert normalized.columns.tolist() == ["RptDt", "Asst", "SgmtNm"]


def test_validate_columns_raises_for_missing_columns(calculator: IQCalculation) -> None:
    """
    Validate _validate_columns raises when required columns are missing.
    """
    df = pd.DataFrame(
        columns=["RptDt", "Asst", "SgmtNm"]  # Missing BrrwrQty and LndrQty
    )

    with pytest.raises(ValueError, match="Missing columns"):
        calculator._validate_columns(df)


def test_to_num_converts_brazilian_number_format(calculator: IQCalculation) -> None:
    """
    Validate _to_num converts Brazilian-formatted numeric strings correctly.
    """
    series = pd.Series(["1.234,56", "10,00", "0", "", None])

    result = calculator._to_num(series)

    expected = pd.Series([1234.56, 10.0, 0.0, 0.0, 0.0])

    pdt.assert_series_equal(result, expected, check_names=False)


def test_calculate_from_dataframe_matches_expected_output(
    calculator: IQCalculation,
    raw_b3_dataframe: pd.DataFrame,
    expected_iq_coef_dataframe: pd.DataFrame,
) -> None:
    """
    Validate calculation output matches expected IQ coefficient fixture.
    """
    result_df = calculator.calculate_from_dataframe(raw_b3_dataframe)

    # Normalize result numeric columns
    for col in ["IQ_call", "IQ_put", "IQ_coef"]:
        result_df[col] = pd.to_numeric(result_df[col], errors="coerce")

    # Ensure deterministic ordering before comparison
    result_df = result_df.sort_values(["Date", "Asset"]).reset_index(drop=True)
    expected_df = expected_iq_coef_dataframe.sort_values(["Date", "Asset"]).reset_index(drop=True)

    pdt.assert_frame_equal(
        result_df,
        expected_df,
        check_dtype=False,
        check_exact=False,
        rtol=1e-9,
        atol=1e-12,
    )