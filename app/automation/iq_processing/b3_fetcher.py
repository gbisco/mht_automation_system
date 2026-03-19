import requests
from typing import Any
from app.logger.logger import AppLogger


class B3Fetcher:
    """
    Service responsible for:
    - Requesting B3 download tokens
    - Downloading raw B3 file content
    - Validating B3 responses
    - Returning raw file payload with metadata

    Internal:
    - base_url (str): Base B3 URL
    - request_token_url (str): Endpoint for token request
    - download_url (str): Endpoint for file download
    - session (requests.Session): persistent HTTP session
    - default_headers (dict): default request headers
    - logger (AppLogger): logger instance for fetch operations
    """

    def __init__(self, base_url: str = "https://arquivos.b3.com.br"):
        """
        Initialize the B3 fetcher service.

        Args:
            base_url (str): Base B3 URL
        """
        # Store base URL
        self.base_url = base_url

        # Build endpoint URLs
        self.request_token_url = f"{self.base_url}/api/download/requestname"
        self.download_url = f"{self.base_url}/api/download/"

        # Initialize session and headers
        self.session = requests.Session()
        self.default_headers = {
            "User-Agent": "Mozilla/5.0",
        }

        # Initialize logger
        self.logger = AppLogger("automation.b3_fetcher")

    # =========================
    # Internal helper methods
    # =========================

    def _request_token(self, file_name: str, date_str: str) -> dict[str, Any]:
        """
        Request a one-time B3 token.

        Args:
            file_name (str): B3 file identifier
            date_str (str): Requested date in YYYY-MM-DD format

        Returns:
            dict[str, Any]: JSON payload returned by B3
        """
        # Build query params
        params = {
            "fileName": file_name,
            "date": date_str,
        }

        self.logger.write(
            f"Requesting B3 token | file_name={file_name} | date={date_str}"
        )

        # Send request
        response = self.session.get(
            self.request_token_url,
            params=params,
            headers=self.default_headers,
            timeout=30,
        )
        response.raise_for_status()

        # Parse JSON payload
        payload = response.json()

        self.logger.write(
            f"B3 token received | file_name={file_name} | date={date_str}"
        )

        return payload

    def _download_file(self, token: str) -> tuple[bytes, dict[str, str]]:
        """
        Download raw file bytes using B3 token.

        Args:
            token (str): One-time B3 download token

        Returns:
            tuple[bytes, dict[str, str]]: File content and response headers
        """
        self.logger.write("Downloading B3 file content")

        # Send request
        response = self.session.get(
            self.download_url,
            params={"token": token},
            timeout=120,
        )
        response.raise_for_status()

        self.logger.write("B3 file content downloaded successfully")

        return response.content, dict(response.headers)

    def _is_html(self, content: bytes) -> bool:
        """
        Detect whether the returned payload is HTML.

        Args:
            content (bytes): Downloaded response content

        Returns:
            bool: True if payload looks like HTML, False otherwise
        """
        # Read only the beginning of the content
        head = content[:400].lower()

        # Detect common HTML patterns
        return head.startswith(b"<!doctype html") or b"<html" in head

    def _resolve_download_name(
        self,
        headers: dict[str, str],
        token_payload: dict[str, Any],
        fallback_name: str,
    ) -> str:
        """
        Resolve output file name from headers or token payload.

        Args:
            headers (dict[str, str]): Response headers from file download
            token_payload (dict[str, Any]): JSON payload from token request
            fallback_name (str): Fallback file name

        Returns:
            str: Resolved file name
        """
        # Try content-disposition header first
        content_disposition = headers.get("content-disposition", "")
        if "filename=" in content_disposition:
            name = content_disposition.split("filename=")[-1].strip().strip('"')
            name = name.split(";")[0].strip()
            return name

        # Try token payload metadata
        file_info = token_payload.get("file", {}) if isinstance(token_payload, dict) else {}
        name = file_info.get("name")
        extension = file_info.get("extension")

        if name and extension:
            return f"{name}{extension}"

        # Fallback
        return fallback_name

    # =========================
    # Public methods
    # =========================

    def fetch(self, file_name: str, date_str: str) -> dict[str, Any]:
        """
        Fetch raw B3 file content for a given file and date.

        Args:
            file_name (str): B3 file identifier
            date_str (str): Requested date in YYYY-MM-DD format

        Returns:
            dict[str, Any]: Raw content and download metadata
        """
        self.logger.write(
            f"Starting B3 fetch | file_name={file_name} | date={date_str}"
        )

        # Request token
        token_payload = self._request_token(file_name=file_name, date_str=date_str)
        token = token_payload["token"]

        # Download file
        content, headers = self._download_file(token=token)

        # Validate payload
        if self._is_html(content):
            self.logger.write(
                f"B3 returned HTML instead of file content | file_name={file_name} | date={date_str}",
                level="error",
            )
            raise RuntimeError("B3 returned HTML instead of file content.")

        # Resolve file name
        fallback_name = f"{file_name}_{date_str.replace('-', '')}.csv"
        download_name = self._resolve_download_name(
            headers=headers,
            token_payload=token_payload,
            fallback_name=fallback_name,
        )

        result = {
            "content": content,
            "download_name": download_name,
            "request_date": date_str,
            "source_file_name": file_name,
            "size_bytes": len(content),
        }

        self.logger.write(
            f"B3 fetch completed | file_name={file_name} | date={date_str} | size_bytes={len(content)}"
        )

        return result