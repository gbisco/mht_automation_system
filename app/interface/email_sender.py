from typing import Any, Optional
import requests
import app.config as config
from app.logger.logger import AppLogger
import base64


class EmailSender:
    """
    Service responsible for:
    - Authenticating with the email provider
    - Building outbound email payloads
    - Managing draft email content and attachments
    - Sending HTML emails
    - Returning send status and response metadata
    """

    def __init__(
        self,
        tenant_id: str = config.EMAIL_TENANT_ID,
        client_id: str = config.EMAIL_CLIENT_ID,
        client_secret: str = config.EMAIL_CLIENT_SECRET,
        sender_email: str = config.EMAIL_SENDER,
        default_recipients: Optional[list[str]] = None,
        graph_base_url: str = config.EMAIL_GRAPH_BASE_URL,
    ):
        # Store authentication and sender configuration
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.sender_email = sender_email
        self.default_recipients = default_recipients or config.DEFAULT_REPORT_RECIPIENTS
        self.graph_base_url = graph_base_url

        # Draft email state
        self.subject = ""
        self.body = ""
        self.draft_recipients = None
        self.draft_cc = None
        self.draft_bcc = None
        self.attachments = []

        # Logger
        self.logger = AppLogger("interface.email_sender")

        # Validate required config
        self._validate_config()
from typing import Any, Optional
import requests
import app.config as config
from app.logger.logger import AppLogger
import base64


class EmailSender:
    """
    Service responsible for:
    - Authenticating with the email provider
    - Building outbound email payloads
    - Managing draft email content and attachments
    - Sending HTML emails
    - Returning send status and response metadata
    """

    def __init__(
        self,
        tenant_id: str = config.EMAIL_TENANT_ID,
        client_id: str = config.EMAIL_CLIENT_ID,
        client_secret: str = config.EMAIL_CLIENT_SECRET,
        sender_email: str = config.EMAIL_SENDER,
        default_recipients: Optional[list[str]] = None,
        graph_base_url: str = config.EMAIL_GRAPH_BASE_URL,
    ):
        # Store authentication and sender configuration
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.sender_email = sender_email
        self.default_recipients = default_recipients or config.DEFAULT_REPORT_RECIPIENTS
        self.graph_base_url = graph_base_url

        # Draft email state
        self.subject = ""
        self.body = ""
        self.draft_recipients = None
        self.draft_cc = None
        self.draft_bcc = None
        self.attachments = []

        # Logger
        self.logger = AppLogger("interface.email_sender")

        # Validate required config
        self._validate_config()


    # =========================
    # Internal helper methods
    # =========================

    def _validate_config(self) -> None:
        """
        Validate required email configuration.

        Raises:
            ValueError: if required configuration is missing
        """
        missing = []

        if not self.tenant_id:
            missing.append("EMAIL_TENANT_ID")
        if not self.client_id:
            missing.append("EMAIL_CLIENT_ID")
        if not self.client_secret:
            missing.append("EMAIL_CLIENT_SECRET")
        if not self.sender_email:
            missing.append("EMAIL_SENDER")

        if missing:
            raise ValueError(
                f"EmailSender misconfigured. Missing: {', '.join(missing)}"
            )

    def _get_access_token(self) -> str:
        """
        Request an access token.

        Returns:
            str: bearer access token

        Raises:
            RuntimeError: if token request fails or access token is missing
        """
        # Build Microsoft identity platform token URL
        token_url = (
            f"https://login.microsoftonline.com/"
            f"{self.tenant_id}/oauth2/v2.0/token"
        )

        # Build token request payload
        token_data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "scope": "https://graph.microsoft.com/.default",
            "grant_type": "client_credentials",
        }

        try:
            # Request access token
            response = requests.post(
                token_url,
                data=token_data,
                timeout=30,
            )

            # Raise for HTTP errors
            response.raise_for_status()

            # Parse token response
            response_json = response.json()
            access_token = response_json.get("access_token")

            if not access_token:
                raise RuntimeError("Token response did not include an access_token.")

            self.logger.info("Successfully acquired Microsoft Graph access token.")
            return access_token

        except requests.RequestException as exc:
            self.logger.error(f"Failed to acquire Microsoft Graph access token: {exc}")
            raise RuntimeError("Failed to acquire Microsoft Graph access token.") from exc

    def _resolve_recipients(self, recipients: Optional[list[str]]) -> list[str]:
        """
        Resolve the effective recipient list.

        Args:
            recipients (Optional[list[str]]): explicit recipients

        Returns:
            list[str]: final recipient list

        Raises:
            ValueError: if no recipients are available
        """
        final_recipients = recipients or self.default_recipients

        if not final_recipients:
            raise ValueError(
                "No recipients provided and no default recipients configured."
            )

        return final_recipients

    def _build_recipient_list(self, recipients: list[str]) -> list[dict[str, Any]]:
        """
        Build Graph recipient format.

        Args:
            recipients (list[str]): email addresses

        Returns:
            list[dict[str, Any]]: Graph-formatted recipients
        """
        return [
            {
                "emailAddress": {
                    "address": email
                }
            }
            for email in recipients
        ]

    def _build_message_payload(
        self,
        subject: str,
        recipients: list[str],
        body: str,
        cc: Optional[list[str]] = None,
        bcc: Optional[list[str]] = None,
        attachments: Optional[list[dict[str, Any]]] = None,
    ) -> dict[str, Any]:
        """
        Build the Microsoft Graph email payload.

        Args:
            subject (str): email subject line
            recipients (list[str]): primary recipient email addresses
            body (str): HTML email body content
            cc (Optional[list[str]]): CC recipient email addresses
            bcc (Optional[list[str]]): BCC recipient email addresses
            attachments (Optional[list[dict[str, Any]]]): pre-built attachment payloads

        Returns:
            dict[str, Any]: formatted Microsoft Graph message payload
        """
        payload = {
            "message": {
                "subject": subject,
                "body": {
                    "contentType": "HTML",
                    "content": body,
                },
                "toRecipients": self._build_recipient_list(recipients),
            }
        }

        if cc:
            payload["message"]["ccRecipients"] = self._build_recipient_list(cc)

        if bcc:
            payload["message"]["bccRecipients"] = self._build_recipient_list(bcc)

        if attachments:
            payload["message"]["attachments"] = attachments

        return payload
    

    def _build_attachment(
        self,
        file_name: str,
        file_bytes: bytes,
        content_type: str = "application/octet-stream",
    ) -> dict[str, Any]:
        """
        Build a Microsoft Graph email attachment payload.

        Args:
            file_name (str): Name of the file as it will appear in the email.
            file_bytes (bytes): Raw file content in bytes.
            content_type (str, optional): MIME type of the file 
                (e.g., "text/csv", "application/pdf"). 
                Defaults to "application/octet-stream".

        Returns:
            dict[str, Any]: Attachment object formatted for Microsoft Graph API,
            including base64-encoded file content.
        """
        if not file_name:
            raise ValueError("file_name cannot be empty")

        if not isinstance(file_bytes, bytes):
            raise TypeError("file_bytes must be bytes")

        if not content_type:
            content_type = "application/octet-stream"

        if file_name.endswith(".csv") and content_type == "application/octet-stream":
            content_type = "text/csv"

        encoded_bytes = base64.b64encode(file_bytes).decode("utf-8")

        return {
            "@odata.type": "#microsoft.graph.fileAttachment",
            "name": file_name,
            "contentType": content_type,
            "contentBytes": encoded_bytes,
        }

    def _send_payload(
        self,
        payload: dict[str, Any],
        access_token: str,
    ) -> dict[str, Any]:
        """
        Send the prepared email payload through Microsoft Graph.

        Args:
            payload (dict[str, Any]): formatted Microsoft Graph message payload
            access_token (str): bearer access token

        Returns:
            dict[str, Any]: send operation metadata

        Raises:
            RuntimeError: if the send request fails
        """
        send_url = f"{self.graph_base_url}/users/{self.sender_email}/sendMail"

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }

        try:
            response = requests.post(
                send_url,
                headers=headers,
                json=payload,
                timeout=30,
            )
            response.raise_for_status()

            self.logger.info(
                f"Successfully sent email from {self.sender_email}."
            )

            return {
                "status": "sent",
                "status_code": response.status_code,
                "sender": self.sender_email,
            }

        except requests.RequestException as exc:
            self.logger.error(f"Failed to send email payload: {exc}")
            raise RuntimeError("Failed to send email payload.") from exc

    # =========================
    # Public methods
    # =========================

    def create_email(
        self,
        subject: str,
        html_body: str,
        recipients: Optional[list[str]] = None,
        cc: Optional[list[str]] = None,
        bcc: Optional[list[str]] = None,
    ) -> None:
        """
        Create or reset the current draft email.

        Args:
            subject (str): email subject
            html_body (str): HTML email body
            recipients (Optional[list[str]]): recipient email addresses
            cc (Optional[list[str]]): CC recipient email addresses
            bcc (Optional[list[str]]): BCC recipient email addresses
        """
        # Reset draft email state
        self.subject = subject
        self.body = html_body
        self.draft_recipients = recipients
        self.draft_cc = cc
        self.draft_bcc = bcc
        self.attachments = []

        self.logger.info("Draft email created successfully.")

    def add_attachment(
        self,
        file_name: str,
        file_bytes: bytes,
        content_type: str = "application/octet-stream",
    ) -> None:
        """
        Add an attachment to the current draft email.

        Args:
            file_name (str): attachment file name
            file_bytes (bytes): raw file bytes
            content_type (str): MIME type
        """
        attachment = self._build_attachment(
            file_name=file_name,
            file_bytes=file_bytes,
            content_type=content_type,
        )

        self.attachments.append(attachment)

        self.logger.info(f"Added attachment to draft email: {file_name}")


    def send(self) -> dict[str, Any]:
        """
        Send the current draft email.

        Returns:
            dict[str, Any]: send result

        Raises:
            ValueError: if required draft fields are missing
        """
        if not self.subject:
            raise ValueError("Email subject is required.")

        if not self.body:
            raise ValueError("Email body is required.")

        final_recipients = self._resolve_recipients(self.draft_recipients)

        payload = self._build_message_payload(
            subject=self.subject,
            recipients=final_recipients,
            body=self.body,
            cc=self.draft_cc,
            bcc=self.draft_bcc,
            attachments=self.attachments if self.attachments else None,
        )

        access_token = self._get_access_token()
        result = self._send_payload(payload, access_token)

        self.logger.info(f"Draft email sent successfully to {final_recipients}.")

        self.subject = ""
        self.body = ""
        self.draft_recipients = None
        self.draft_cc = None
        self.draft_bcc = None
        self.attachments = []

        return result

    # =========================
    # Internal helper methods
    # =========================

    def _validate_config(self) -> None:
        """
        Validate required email configuration.

        Raises:
            ValueError: if required configuration is missing
        """
        missing = []

        if not self.tenant_id:
            missing.append("EMAIL_TENANT_ID")
        if not self.client_id:
            missing.append("EMAIL_CLIENT_ID")
        if not self.client_secret:
            missing.append("EMAIL_CLIENT_SECRET")
        if not self.sender_email:
            missing.append("EMAIL_SENDER")

        if missing:
            raise ValueError(
                f"EmailSender misconfigured. Missing: {', '.join(missing)}"
            )

    def _get_access_token(self) -> str:
        """
        Request an access token.

        Returns:
            str: bearer access token

        Raises:
            RuntimeError: if token request fails or access token is missing
        """
        # Build Microsoft identity platform token URL
        token_url = (
            f"https://login.microsoftonline.com/"
            f"{self.tenant_id}/oauth2/v2.0/token"
        )

        # Build token request payload
        token_data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "scope": "https://graph.microsoft.com/.default",
            "grant_type": "client_credentials",
        }

        try:
            # Request access token
            response = requests.post(
                token_url,
                data=token_data,
                timeout=30,
            )

            # Raise for HTTP errors
            response.raise_for_status()

            # Parse token response
            response_json = response.json()
            access_token = response_json.get("access_token")

            if not access_token:
                raise RuntimeError("Token response did not include an access_token.")

            self.logger.info("Successfully acquired Microsoft Graph access token.")
            return access_token

        except requests.RequestException as exc:
            self.logger.error(f"Failed to acquire Microsoft Graph access token: {exc}")
            raise RuntimeError("Failed to acquire Microsoft Graph access token.") from exc

    def _resolve_recipients(self, recipients: Optional[list[str]]) -> list[str]:
        """
        Resolve the effective recipient list.

        Args:
            recipients (Optional[list[str]]): explicit recipients

        Returns:
            list[str]: final recipient list

        Raises:
            ValueError: if no recipients are available
        """
        final_recipients = recipients or self.default_recipients

        if not final_recipients:
            raise ValueError(
                "No recipients provided and no default recipients configured."
            )

        return final_recipients

    def _build_recipient_list(self, recipients: list[str]) -> list[dict[str, Any]]:
        """
        Build Graph recipient format.

        Args:
            recipients (list[str]): email addresses

        Returns:
            list[dict[str, Any]]: Graph-formatted recipients
        """
        return [
            {
                "emailAddress": {
                    "address": email
                }
            }
            for email in recipients
        ]

    def _build_message_payload(
    self,
    subject: str,
    recipients: list[str],
    body: str,
    cc: Optional[list[str]] = None,
    bcc: Optional[list[str]] = None,
    attachments: Optional[list[dict[str, Any]]] = None,
    ) -> dict[str, Any]:
        """
        Build the Microsoft Graph email payload.

        Args:
            subject (str): email subject line
            recipients (list[str]): primary recipient email addresses
            body (str): HTML email body content
            cc (Optional[list[str]]): CC recipient email addresses
            bcc (Optional[list[str]]): BCC recipient email addresses
            attachments (Optional[list[dict[str, Any]]]): pre-built attachment payloads

        Returns:
            dict[str, Any]: formatted Microsoft Graph message payload
        """
        payload = {
            "message": {
                "subject": subject,
                "body": {
                    "contentType": "HTML",
                    "content": body,
                },
                "toRecipients": self._build_recipient_list(recipients),
            }
        }

        if cc:
            payload["message"]["ccRecipients"] = self._build_recipient_list(cc)

        if bcc:
            payload["message"]["bccRecipients"] = self._build_recipient_list(bcc)

        if attachments:
            payload["message"]["attachments"] = attachments

        return payload
    

    def _build_attachment(
        self,
        file_name: str,
        file_bytes: bytes,
        content_type: str = "application/octet-stream",
    ) -> dict[str, Any]:
        """
        Build a Microsoft Graph email attachment payload.

        Args:
            file_name (str): Name of the file as it will appear in the email.
            file_bytes (bytes): Raw file content in bytes.
            content_type (str, optional): MIME type of the file 
                (e.g., "text/csv", "application/pdf"). 
                Defaults to "application/octet-stream".

        Returns:
            dict[str, Any]: Attachment object formatted for Microsoft Graph API,
            including base64-encoded file content.
        """
        if not file_name:
            raise ValueError("file_name cannot be empty")

        if not isinstance(file_bytes, bytes):
            raise TypeError("file_bytes must be bytes")

        if not content_type:
            content_type = "application/octet-stream"

        return {
            "@odata.type": "#microsoft.graph.fileAttachment",
            "name": file_name,
            "contentType": content_type,
            "contentBytes": base64.b64encode(file_bytes).decode("utf-8"),}

    def _send_payload(
        self,
        payload: dict[str, Any],
        access_token: str,
        ) -> dict[str, Any]:
        """
        Send the prepared email payload through Microsoft Graph.

        Args:
            payload (dict[str, Any]): formatted Microsoft Graph message payload
            access_token (str): bearer access token

        Returns:
            dict[str, Any]: send operation metadata

        Raises:
            RuntimeError: if the send request fails
        """
        send_url = f"{self.graph_base_url}/users/{self.sender_email}/sendMail"

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }

        try:
            response = requests.post(
                send_url,
                headers=headers,
                json=payload,
                timeout=30,
            )
            response.raise_for_status()

            self.logger.info(
                f"Successfully sent email from {self.sender_email}."
            )

            return {
                "status": "sent",
                "status_code": response.status_code,
                "sender": self.sender_email,
            }

        except requests.RequestException as exc:
            self.logger.error(f"Failed to send email payload: {exc}")
            raise RuntimeError("Failed to send email payload.") from exc

    # =========================
    # Public methods
    # =========================

    def create_email(
        self,
        subject: str,
        html_body: str,
        recipients: Optional[list[str]] = None,
        cc: Optional[list[str]] = None,
        bcc: Optional[list[str]] = None,
    ) -> None:
        """
        Create or reset the current draft email.

        Args:
            subject (str): email subject
            html_body (str): HTML email body
            recipients (Optional[list[str]]): recipient email addresses
            cc (Optional[list[str]]): CC recipient email addresses
            bcc (Optional[list[str]]): BCC recipient email addresses
        """
        # Reset draft email state
        self.subject = subject
        self.body = html_body
        self.draft_recipients = recipients
        self.draft_cc = cc
        self.draft_bcc = bcc
        self.attachments = []

        self.logger.info("Draft email created successfully.")

    def add_attachment(
        self,
        file_name: str,
        file_bytes: bytes,
        content_type: str = "application/octet-stream",
    ) -> None:
        """
        Add an attachment to the current draft email.

        Args:
            file_name (str): attachment file name
            file_bytes (bytes): raw file bytes
            content_type (str): MIME type
        """
        attachment = self._build_attachment(
            file_name=file_name,
            file_bytes=file_bytes,
            content_type=content_type,
        )

        self.attachments.append(attachment)

        self.logger.info(f"Added attachment to draft email: {file_name}")


    def send(self) -> dict[str, Any]:
        """
        Send the current draft email.

        Returns:
            dict[str, Any]: send result

        Raises:
            ValueError: if required draft fields are missing
        """
        if not self.subject:
            raise ValueError("Email subject is required.")

        if not self.body:
            raise ValueError("Email body is required.")

        final_recipients = self._resolve_recipients(self.draft_recipients)

        payload = self._build_message_payload(
            subject=self.subject,
            recipients=final_recipients,
            body=self.body,
            cc=self.draft_cc,
            bcc=self.draft_bcc,
            attachments=self.attachments if self.attachments else None,
        )

        access_token = self._get_access_token()
        result = self._send_payload(payload, access_token)

        self.logger.info("Draft email sent successfully.")
        return result