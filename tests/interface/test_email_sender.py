from pathlib import Path
from unittest.mock import Mock

import pytest

from app.interface.email_sender import EmailSender


@pytest.fixture
def sender() -> EmailSender:
    """
    Create EmailSender instance for tests.
    """
    return EmailSender(
        tenant_id="test-tenant-id",
        client_id="test-client-id",
        client_secret="test-client-secret",
        sender_email="automation@novaflowdigi.com",
        default_recipients=["default@example.com"],
        graph_base_url="https://graph.microsoft.com/v1.0",
    )


@pytest.fixture
def csv_fixture_bytes() -> bytes:
    """
    Load sample CSV fixture as raw bytes for attachment tests.
    """
    fixture_path = Path("tests/fixtures/b3_derivatives_2026_03_18.csv")
    return fixture_path.read_bytes()


def test_email_sender_initializes_with_expected_values(sender: EmailSender) -> None:
    """
    Validate EmailSender initializes with expected configuration and empty draft state.
    """
    assert sender.tenant_id == "test-tenant-id"
    assert sender.client_id == "test-client-id"
    assert sender.client_secret == "test-client-secret"
    assert sender.sender_email == "automation@novaflowdigi.com"
    assert sender.default_recipients == ["default@example.com"]
    assert sender.graph_base_url == "https://graph.microsoft.com/v1.0"

    assert sender.subject == ""
    assert sender.body == ""
    assert sender.draft_recipients is None
    assert sender.draft_cc is None
    assert sender.draft_bcc is None
    assert sender.attachments == []


def test_validate_config_raises_for_missing_required_values() -> None:
    """
    Validate constructor raises when required config is missing.
    """
    with pytest.raises(ValueError, match="Missing: EMAIL_TENANT_ID"):
        EmailSender(
            tenant_id="",
            client_id="test-client-id",
            client_secret="test-client-secret",
            sender_email="automation@novaflowdigi.com",
        )


def test_resolve_recipients_returns_explicit_recipients(sender: EmailSender) -> None:
    """
    Validate explicit recipients are preferred over defaults.
    """
    result = sender._resolve_recipients(["custom@example.com"])

    assert result == ["custom@example.com"]


def test_resolve_recipients_falls_back_to_default_recipients(sender: EmailSender) -> None:
    """
    Validate default recipients are used when explicit recipients are not provided.
    """
    result = sender._resolve_recipients(None)

    assert result == ["default@example.com"]


def test_resolve_recipients_raises_when_no_recipients_available(sender: EmailSender) -> None:
    """
    Validate recipient resolution raises when neither explicit nor default recipients exist.
    """
    sender.default_recipients = []

    with pytest.raises(ValueError, match="No recipients provided"):
        sender._resolve_recipients(None)


def test_build_recipient_list_returns_graph_format(sender: EmailSender) -> None:
    """
    Validate recipient list is converted to Microsoft Graph format.
    """
    result = sender._build_recipient_list(["a@example.com", "b@example.com"])

    assert result == [
        {"emailAddress": {"address": "a@example.com"}},
        {"emailAddress": {"address": "b@example.com"}},
    ]


def test_create_email_resets_draft_state(sender: EmailSender) -> None:
    """
    Validate create_email stores draft values and resets attachments.
    """
    sender.attachments = [{"name": "old.csv"}]

    sender.create_email(
        subject="Daily IQ Report",
        html_body="<h1>Hello</h1>",
        recipients=["report@example.com"],
        cc=["cc@example.com"],
        bcc=["bcc@example.com"],
    )

    assert sender.subject == "Daily IQ Report"
    assert sender.body == "<h1>Hello</h1>"
    assert sender.draft_recipients == ["report@example.com"]
    assert sender.draft_cc == ["cc@example.com"]
    assert sender.draft_bcc == ["bcc@example.com"]
    assert sender.attachments == []


def test_build_attachment_returns_graph_file_attachment(
    sender: EmailSender,
    csv_fixture_bytes: bytes,
) -> None:
    """
    Validate _build_attachment returns Microsoft Graph attachment structure.
    """
    result = sender._build_attachment(
        file_name="b3_derivatives_2026_03_18.csv",
        file_bytes=csv_fixture_bytes,
    )

    assert result["@odata.type"] == "#microsoft.graph.fileAttachment"
    assert result["name"] == "b3_derivatives_2026_03_18.csv"
    assert result["contentType"] == "text/csv"
    assert isinstance(result["contentBytes"], str)
    assert len(result["contentBytes"]) > 0


def test_add_attachment_appends_attachment_to_draft(
    sender: EmailSender,
    csv_fixture_bytes: bytes,
) -> None:
    """
    Validate add_attachment appends a built attachment to the current draft.
    """
    sender.create_email(
        subject="Daily IQ Report",
        html_body="<p>Attached</p>",
    )

    sender.add_attachment(
        file_name="b3_derivatives_2026_03_18.csv",
        file_bytes=csv_fixture_bytes,
    )

    assert len(sender.attachments) == 1
    assert sender.attachments[0]["name"] == "b3_derivatives_2026_03_18.csv"


def test_build_message_payload_includes_required_fields(sender: EmailSender) -> None:
    """
    Validate message payload includes subject, HTML body, and recipients.
    """
    result = sender._build_message_payload(
        subject="Daily IQ Report",
        recipients=["report@example.com"],
        body="<h1>IQ</h1>",
        cc=["cc@example.com"],
        bcc=["bcc@example.com"],
        attachments=[{"name": "file.csv"}],
    )

    assert result["message"]["subject"] == "Daily IQ Report"
    assert result["message"]["body"]["contentType"] == "HTML"
    assert result["message"]["body"]["content"] == "<h1>IQ</h1>"
    assert result["message"]["toRecipients"] == [
        {"emailAddress": {"address": "report@example.com"}}
    ]
    assert result["message"]["ccRecipients"] == [
        {"emailAddress": {"address": "cc@example.com"}}
    ]
    assert result["message"]["bccRecipients"] == [
        {"emailAddress": {"address": "bcc@example.com"}}
    ]
    assert result["message"]["attachments"] == [{"name": "file.csv"}]


def test_get_access_token_returns_access_token(sender: EmailSender, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Validate _get_access_token returns token from Microsoft identity response.
    """
    mock_response = Mock()
    mock_response.json.return_value = {"access_token": "fake-token"}
    mock_response.raise_for_status.return_value = None

    mock_post = Mock(return_value=mock_response)
    monkeypatch.setattr("app.interface.email_sender.requests.post", mock_post)

    result = sender._get_access_token()

    assert result == "fake-token"
    mock_post.assert_called_once()


def test_send_payload_returns_send_metadata(sender: EmailSender, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Validate _send_payload returns expected send metadata.
    """
    mock_response = Mock()
    mock_response.status_code = 202
    mock_response.raise_for_status.return_value = None

    mock_post = Mock(return_value=mock_response)
    monkeypatch.setattr("app.interface.email_sender.requests.post", mock_post)

    result = sender._send_payload(
        payload={"message": {"subject": "Hello"}},
        access_token="fake-token",
    )

    assert result["status"] == "sent"
    assert result["status_code"] == 202
    assert result["sender"] == "automation@novaflowdigi.com"


def test_send_raises_when_subject_is_missing(sender: EmailSender) -> None:
    """
    Validate send raises when draft subject is missing.
    """
    sender.body = "<p>Hello</p>"

    with pytest.raises(ValueError, match="Email subject is required"):
        sender.send()


def test_send_raises_when_body_is_missing(sender: EmailSender) -> None:
    """
    Validate send raises when draft body is missing.
    """
    sender.subject = "Hello"

    with pytest.raises(ValueError, match="Email body is required"):
        sender.send()


def test_send_builds_and_sends_email_successfully(sender: EmailSender, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Validate send orchestrates payload build, token retrieval, and payload send.
    """
    sender.create_email(
        subject="Daily IQ Report",
        html_body="<h1>IQ Report</h1>",
        recipients=["report@example.com"],
    )

    monkeypatch.setattr(sender, "_get_access_token", Mock(return_value="fake-token"))
    monkeypatch.setattr(
        sender,
        "_send_payload",
        Mock(return_value={"status": "sent", "status_code": 202, "sender": sender.sender_email}),
    )

    result = sender.send()

    assert result["status"] == "sent"
    assert sender.subject == ""
    assert sender.body == ""
    assert sender.draft_recipients is None
    assert sender.draft_cc is None
    assert sender.draft_bcc is None
    assert sender.attachments == []