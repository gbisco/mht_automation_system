from unittest.mock import Mock
import pytest

from app.storage.sharepoint_storage import SharePointStorage
import requests


# =========================
# Fixtures
# =========================

@pytest.fixture
def storage() -> SharePointStorage:
    return SharePointStorage(
        tenant_id="test-tenant-id",
        client_id="test-client-id",
        client_secret="test-client-secret",
        site_id="test-site-id",
        drive_id="test-drive-id",
        graph_base_url="https://graph.microsoft.com/v1.0",
    )


# =========================
# Init + Config Tests
# =========================

def test_init_sets_values(storage: SharePointStorage) -> None:
    assert storage.tenant_id == "test-tenant-id"
    assert storage.client_id == "test-client-id"
    assert storage.client_secret == "test-client-secret"
    assert storage.site_id == "test-site-id"
    assert storage.drive_id == "test-drive-id"


def test_validate_config_missing_fields() -> None:
    with pytest.raises(ValueError, match="Missing: SHAREPOINT_TENANT_ID"):
        SharePointStorage(
            tenant_id="",
            client_id="x",
            client_secret="x",
            site_id="x",
            drive_id="x",
        )


# =========================
# URL Builder
# =========================

def test_build_file_url(storage: SharePointStorage) -> None:
    result = storage._build_file_url("folder/file.csv")

    assert result == (
        "https://graph.microsoft.com/v1.0/sites/test-site-id/"
        "drives/test-drive-id/root:/folder/file.csv"
    )


def test_build_file_url_strips_leading_slash(storage: SharePointStorage) -> None:
    result = storage._build_file_url("/folder/file.csv")

    assert result.endswith("root:/folder/file.csv")


# =========================
# Token
# =========================

def test_get_access_token_success(storage: SharePointStorage, monkeypatch):
    mock_response = Mock()
    mock_response.json.return_value = {"access_token": "fake-token"}
    mock_response.raise_for_status.return_value = None

    monkeypatch.setattr(
        "app.storage.sharepoint_storage.requests.post",
        Mock(return_value=mock_response),
    )

    token = storage._get_access_token()

    assert token == "fake-token"


def test_get_access_token_failure(storage: SharePointStorage, monkeypatch):
    mock_post = Mock(side_effect=requests.RequestException("fail"))

    monkeypatch.setattr(
        "app.storage.sharepoint_storage.requests.post",
        mock_post,
    )

    with pytest.raises(RuntimeError):
        storage._get_access_token()

# =========================
# Upload
# =========================

def test_upload_file_bytes_success(storage: SharePointStorage, monkeypatch):
    monkeypatch.setattr(storage, "_get_access_token", Mock(return_value="fake-token"))

    mock_response = Mock()
    mock_response.json.return_value = {
        "name": "file.csv",
        "id": "123",
        "webUrl": "https://example.com/file.csv",
    }
    mock_response.raise_for_status.return_value = None

    monkeypatch.setattr(
        "app.storage.sharepoint_storage.requests.put",
        Mock(return_value=mock_response),
    )

    result = storage.upload_file_bytes(
        file_path="folder/file.csv",
        file_bytes=b"data",
        content_type="text/csv",
    )

    assert result["status"] == "uploaded"
    assert result["file_path"] == "folder/file.csv"
    assert result["name"] == "file.csv"
    assert result["id"] == "123"
    assert result["web_url"] == "https://example.com/file.csv"


def test_upload_file_bytes_invalid_path(storage: SharePointStorage):
    with pytest.raises(ValueError):
        storage.upload_file_bytes("", b"data")


def test_upload_file_bytes_invalid_bytes(storage: SharePointStorage):
    with pytest.raises(TypeError):
        storage.upload_file_bytes("file.csv", "not-bytes")  # type: ignore[arg-type]


# =========================
# Download
# =========================

def test_download_file_bytes_success(storage: SharePointStorage, monkeypatch):
    monkeypatch.setattr(storage, "_get_access_token", Mock(return_value="fake-token"))

    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.content = b"hello"
    mock_response.raise_for_status.return_value = None

    monkeypatch.setattr(
        "app.storage.sharepoint_storage.requests.get",
        Mock(return_value=mock_response),
    )

    result = storage.download_file_bytes("folder/file.txt")

    assert result == b"hello"


def test_download_file_bytes_not_found(storage: SharePointStorage, monkeypatch):
    monkeypatch.setattr(storage, "_get_access_token", Mock(return_value="fake-token"))

    mock_response = Mock()
    mock_response.status_code = 404

    monkeypatch.setattr(
        "app.storage.sharepoint_storage.requests.get",
        Mock(return_value=mock_response),
    )

    with pytest.raises(FileNotFoundError):
        storage.download_file_bytes("missing.txt")


def test_download_file_bytes_invalid_path(storage: SharePointStorage):
    with pytest.raises(ValueError):
        storage.download_file_bytes("")


# =========================
# Exists
# =========================

def test_file_exists_true(storage: SharePointStorage, monkeypatch):
    monkeypatch.setattr(storage, "_get_access_token", Mock(return_value="fake-token"))

    mock_response = Mock()
    mock_response.status_code = 200

    monkeypatch.setattr(
        "app.storage.sharepoint_storage.requests.get",
        Mock(return_value=mock_response),
    )

    assert storage.file_exists("file.csv") is True


def test_file_exists_false(storage: SharePointStorage, monkeypatch):
    monkeypatch.setattr(storage, "_get_access_token", Mock(return_value="fake-token"))

    mock_response = Mock()
    mock_response.status_code = 404

    monkeypatch.setattr(
        "app.storage.sharepoint_storage.requests.get",
        Mock(return_value=mock_response),
    )

    assert storage.file_exists("file.csv") is False


def test_file_exists_invalid_path(storage: SharePointStorage):
    with pytest.raises(ValueError):
        storage.file_exists("")


# =========================
# List Files
# =========================

def test_list_files_returns_latest_first_with_top_n(
    storage: SharePointStorage,
    monkeypatch,
):
    monkeypatch.setattr(storage, "_get_access_token", Mock(return_value="fake-token"))

    mock_response = Mock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {
        "value": [
            {
                "name": "iq_old.csv",
                "file": {},
                "webUrl": "https://example.com/iq_old.csv",
                "lastModifiedDateTime": "2026-03-20T10:00:00Z",
            },
            {
                "name": "iq_new.csv",
                "file": {},
                "webUrl": "https://example.com/iq_new.csv",
                "lastModifiedDateTime": "2026-03-21T10:00:00Z",
            },
            {
                "name": "iq_mid.csv",
                "file": {},
                "webUrl": "https://example.com/iq_mid.csv",
                "lastModifiedDateTime": "2026-03-20T20:00:00Z",
            },
        ]
    }

    monkeypatch.setattr(
        "app.storage.sharepoint_storage.requests.get",
        Mock(return_value=mock_response),
    )

    result = storage.list_files("test/iq_coeff", top_n=2)

    assert len(result) == 2
    assert result[0]["name"] == "iq_new.csv"
    assert result[0]["file_path"] == "test/iq_coeff/iq_new.csv"
    assert result[1]["name"] == "iq_mid.csv"
    assert result[1]["file_path"] == "test/iq_coeff/iq_mid.csv"


def test_list_files_skips_folders(storage: SharePointStorage, monkeypatch):
    monkeypatch.setattr(storage, "_get_access_token", Mock(return_value="fake-token"))

    mock_response = Mock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {
        "value": [
            {
                "name": "subfolder",
                "folder": {},
                "lastModifiedDateTime": "2026-03-21T09:00:00Z",
            },
            {
                "name": "iq_file.csv",
                "file": {},
                "webUrl": "https://example.com/iq_file.csv",
                "lastModifiedDateTime": "2026-03-21T10:00:00Z",
            },
        ]
    }

    monkeypatch.setattr(
        "app.storage.sharepoint_storage.requests.get",
        Mock(return_value=mock_response),
    )

    result = storage.list_files("test/iq_coeff")

    assert len(result) == 1
    assert result[0]["name"] == "iq_file.csv"
    assert result[0]["file_path"] == "test/iq_coeff/iq_file.csv"