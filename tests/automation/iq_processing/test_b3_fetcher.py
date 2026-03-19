from unittest.mock import Mock
import pytest
from app.automation.iq_processing.b3_fetcher import B3Fetcher


@pytest.fixture
def fetcher() -> B3Fetcher:
    """
    Create a B3Fetcher instance for tests.
    """
    return B3Fetcher()


def test_b3_fetcher_initializes_with_default_urls(fetcher: B3Fetcher) -> None:
    """
    Validate default B3 URLs are built correctly.
    """
    assert fetcher.base_url == "https://arquivos.b3.com.br"
    assert fetcher.request_token_url == "https://arquivos.b3.com.br/api/download/requestname"
    assert fetcher.download_url == "https://arquivos.b3.com.br/api/download/"


def test_is_html_returns_true_for_html_payload(fetcher: B3Fetcher) -> None:
    """
    Validate HTML payload detection returns True.
    """
    content = b"<!DOCTYPE html><html><body>blocked</body></html>"

    assert fetcher._is_html(content) is True


def test_is_html_returns_false_for_csv_payload(fetcher: B3Fetcher) -> None:
    """
    Validate non-HTML payload detection returns False.
    """
    content = b"col1,col2\nvalue1,value2\n"

    assert fetcher._is_html(content) is False


def test_resolve_download_name_prefers_content_disposition(fetcher: B3Fetcher) -> None:
    """
    Validate file name is resolved from content-disposition header first.
    """
    headers = {
        "content-disposition": 'attachment; filename="b3_file.csv"'
    }
    token_payload = {
        "file": {
            "name": "ignored_name",
            "extension": ".csv",
        }
    }

    result = fetcher._resolve_download_name(
        headers=headers,
        token_payload=token_payload,
        fallback_name="fallback.csv",
    )

    assert result == "b3_file.csv"


def test_resolve_download_name_uses_token_payload_when_header_missing(fetcher: B3Fetcher) -> None:
    """
    Validate file name is resolved from token payload if header is missing.
    """
    headers = {}
    token_payload = {
        "file": {
            "name": "DerivativesOpenPositionFile_20260318",
            "extension": ".csv",
        }
    }

    result = fetcher._resolve_download_name(
        headers=headers,
        token_payload=token_payload,
        fallback_name="fallback.csv",
    )

    assert result == "DerivativesOpenPositionFile_20260318.csv"


def test_resolve_download_name_uses_fallback_when_no_name_available(fetcher: B3Fetcher) -> None:
    """
    Validate fallback file name is used when no upstream name is available.
    """
    headers = {}
    token_payload = {}

    result = fetcher._resolve_download_name(
        headers=headers,
        token_payload=token_payload,
        fallback_name="fallback.csv",
    )

    assert result == "fallback.csv"


def test_request_token_returns_json_payload(fetcher: B3Fetcher, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Validate _request_token returns the parsed JSON payload.
    """
    mock_response = Mock()
    mock_response.json.return_value = {"token": "abc123"}
    mock_response.raise_for_status.return_value = None

    mock_get = Mock(return_value=mock_response)
    monkeypatch.setattr(fetcher.session, "get", mock_get)

    result = fetcher._request_token(
        file_name="DerivativesOpenPositionFile",
        date_str="2026-03-18",
    )

    assert result == {"token": "abc123"}
    mock_get.assert_called_once_with(
        fetcher.request_token_url,
        params={
            "fileName": "DerivativesOpenPositionFile",
            "date": "2026-03-18",
        },
        headers=fetcher.default_headers,
        timeout=30,
    )


def test_download_file_returns_content_and_headers(fetcher: B3Fetcher, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Validate _download_file returns response content and headers.
    """
    mock_response = Mock()
    mock_response.content = b"col1,col2\nvalue1,value2\n"
    mock_response.headers = {"content-disposition": 'attachment; filename="test.csv"'}
    mock_response.raise_for_status.return_value = None

    mock_get = Mock(return_value=mock_response)
    monkeypatch.setattr(fetcher.session, "get", mock_get)

    content, headers = fetcher._download_file(token="abc123")

    assert content == b"col1,col2\nvalue1,value2\n"
    assert headers["content-disposition"] == 'attachment; filename="test.csv"'
    mock_get.assert_called_once_with(
        fetcher.download_url,
        params={"token": "abc123"},
        timeout=120,
    )


def test_fetch_returns_expected_metadata(fetcher: B3Fetcher, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Validate fetch returns raw content and expected metadata.
    """
    mock_content = b"col1,col2\nvalue1,value2\n"

    monkeypatch.setattr(
        fetcher,
        "_request_token",
        Mock(return_value={"token": "abc123"}),
    )
    monkeypatch.setattr(
        fetcher,
        "_download_file",
        Mock(return_value=(mock_content, {})),
    )
    monkeypatch.setattr(
        fetcher,
        "_is_html",
        Mock(return_value=False),
    )
    monkeypatch.setattr(
        fetcher,
        "_resolve_download_name",
        Mock(return_value="download.csv"),
    )

    result = fetcher.fetch(
        file_name="DerivativesOpenPositionFile",
        date_str="2026-03-18",
    )

    assert result["content"] == mock_content
    assert result["download_name"] == "download.csv"
    assert result["request_date"] == "2026-03-18"
    assert result["source_file_name"] == "DerivativesOpenPositionFile"
    assert result["size_bytes"] == len(mock_content)


def test_fetch_raises_runtime_error_when_html_is_returned(fetcher: B3Fetcher, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Validate fetch raises RuntimeError when HTML payload is detected.
    """
    monkeypatch.setattr(
        fetcher,
        "_request_token",
        Mock(return_value={"token": "abc123"}),
    )
    monkeypatch.setattr(
        fetcher,
        "_download_file",
        Mock(return_value=(b"<!DOCTYPE html><html></html>", {})),
    )
    monkeypatch.setattr(
        fetcher,
        "_is_html",
        Mock(return_value=True),
    )

    with pytest.raises(RuntimeError, match="B3 returned HTML instead of file content."):
        fetcher.fetch(
            file_name="DerivativesOpenPositionFile",
            date_str="2026-03-18",
        )