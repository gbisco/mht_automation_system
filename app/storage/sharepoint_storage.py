from typing import Any
import requests
import time
import app.config as config
from app.logger.logger import AppLogger


class SharePointStorage:
    """
    Service responsible for:
    - Authenticating with Microsoft Graph for SharePoint access
    - Uploading files to a SharePoint document library
    - Downloading files from a SharePoint document library
    - Checking whether a file exists in a SharePoint document library
    - Returning metadata for stored files

    Internal:
    - tenant_id (str): Microsoft tenant ID
    - client_id (str): application client ID
    - client_secret (str): application client secret
    - site_id (str): SharePoint site ID
    - drive_id (str): SharePoint document library drive ID
    - graph_base_url (str): Microsoft Graph base URL
    - logger (AppLogger): logger instance for SharePoint storage operations
    """

    def __init__(
        self,
        tenant_id: str = config.SHAREPOINT_TENANT_ID,
        client_id: str = config.SHAREPOINT_CLIENT_ID,
        client_secret: str = config.SHAREPOINT_CLIENT_SECRET,
        site_id: str = config.SHAREPOINT_SITE_ID,
        drive_id: str = config.SHAREPOINT_DRIVE_ID,
        graph_base_url: str = config.SHAREPOINT_GRAPH_BASE_URL,
    ):
        """
        Initialize the SharePoint storage service.

        Args:
            tenant_id (str): Microsoft tenant ID
            client_id (str): application client ID
            client_secret (str): application client secret
            site_id (str): SharePoint site ID
            drive_id (str): SharePoint document library drive ID
            graph_base_url (str): Microsoft Graph base URL
        """
        # Store authentication and SharePoint configuration
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.site_id = site_id
        self.drive_id = drive_id
        self.graph_base_url = graph_base_url

        # Initialize logger
        self.logger = AppLogger("storage.sharepoint")

        # Validate required config
        self._validate_config()

    # =========================
    # Internal helper methods
    # =========================
    def _validate_config(self) -> None:
            """
            Validate required SharePoint configuration.

            Raises:
                ValueError: if required configuration is missing
            """
            # Start array of missing values
            missing = []
            # Go over neccessary objects and append missing values
            if not self.tenant_id:
                missing.append("SHAREPOINT_TENANT_ID")
            if not self.client_id:
                missing.append("SHAREPOINT_CLIENT_ID")
            if not self.client_secret:
                missing.append("SHAREPOINT_CLIENT_SECRET")
            if not self.site_id:
                missing.append("SHAREPOINT_SITE_ID")
            if not self.drive_id:
                missing.append("SHAREPOINT_DRIVE_ID")
            # If missing is not empty append
            if missing:
                raise ValueError(f"SharePointStorage misconfigured. Missing: {', '.join(missing)}")


    def _get_access_token(self) -> str:
        """
        Request an access token for Microsoft Graph.

        Returns:
            str: bearer access token

        Raises:
            RuntimeError: if token request fails or access token is missing
        """
        token_url = (
            f"https://login.microsoftonline.com/"
            f"{self.tenant_id}/oauth2/v2.0/token"
        )

        token_data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "scope": "https://graph.microsoft.com/.default",
            "grant_type": "client_credentials",
        }

        max_attempts = 3
        retry_statuses = {429, 502, 503, 504}

        for attempt in range(1, max_attempts + 1):
            try:
                response = requests.post(
                    token_url,
                    data=token_data,
                    timeout=30,
                )

                if response.status_code in retry_statuses and attempt < max_attempts:
                    delay = 2 ** (attempt - 1)
                    self.logger.write(
                        f"Transient error {response.status_code} acquiring token. "
                        f"Retrying in {delay}s (attempt {attempt}/{max_attempts})",
                        level="warning",
                    )
                    time.sleep(delay)
                    continue

                response.raise_for_status()

                response_json = response.json()
                access_token = response_json.get("access_token")

                if not access_token:
                    raise RuntimeError("Token response did not include an access_token.")

                self.logger.write(
                    "Successfully acquired Microsoft Graph access token for SharePoint.",
                    level="info",
                )
                return access_token

            except requests.RequestException as exc:
                if attempt == max_attempts:
                    self.logger.write(
                        f"Failed to acquire Microsoft Graph access token after {max_attempts} attempts: {exc}",
                        level="error",
                    )
                    raise RuntimeError(
                        "Failed to acquire Microsoft Graph access token for SharePoint."
                    ) from exc

                delay = 2 ** (attempt - 1)
                self.logger.write(
                    f"Request error acquiring token. Retrying in {delay}s "
                    f"(attempt {attempt}/{max_attempts}): {exc}",
                    level="warning",
                )
                time.sleep(delay)

    def _build_file_url(self, file_path: str) -> str:
        """
        Build Microsoft Graph URL for a SharePoint file path.

        Args:
            file_path (str): path to file within the document library

        Returns:
            str: Graph API file URL
        """
        # Normalize path (avoid leading slash issues)
        normalized_path = file_path.lstrip("/")

        return (
            f"{self.graph_base_url}/sites/{self.site_id}/drives/"
            f"{self.drive_id}/root:/{normalized_path}"
        )

    # =========================
    # Public methods
    # =========================

    def upload_file_bytes(
        self,
        file_path: str,
        file_bytes: bytes,
        content_type: str = "application/octet-stream",
    ) -> dict[str, Any]:
        """
        Upload raw file bytes to SharePoint.

        Args:
            file_path (str): destination path within the document library
            file_bytes (bytes): raw file content
            content_type (str): MIME type of file

        Returns:
            dict[str, Any]: upload result metadata

        Raises:
            ValueError: if file_path is empty
            TypeError: if file_bytes is not bytes
            RuntimeError: if upload fails
        """
        if not file_path:
            raise ValueError("file_path cannot be empty")

        if not isinstance(file_bytes, bytes):
            raise TypeError("file_bytes must be bytes")

        upload_url = self._build_file_url(file_path) + ":/content"
        access_token = self._get_access_token()

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": content_type,
        }

        max_attempts = 3
        retry_status_codes = {429, 502, 503, 504}

        for attempt in range(1, max_attempts + 1):
            try:
                response = requests.put(
                    upload_url,
                    headers=headers,
                    data=file_bytes,
                    timeout=30,
                )

                if response.status_code in retry_status_codes:
                    raise requests.HTTPError(
                        f"{response.status_code} Server Error: transient upload failure",
                        response=response,
                    )

                response.raise_for_status()

                response_json = response.json()

                self.logger.write(
                    f"Successfully uploaded file to SharePoint: {file_path}",
                    level="info",
                )

                return {
                    "status": "uploaded",
                    "file_path": file_path,
                    "name": response_json.get("name"),
                    "id": response_json.get("id"),
                    "web_url": response_json.get("webUrl"),
                }

            except requests.RequestException as exc:
                status_code = getattr(exc.response, "status_code", None)

                is_retryable = status_code in retry_status_codes

                if attempt < max_attempts and is_retryable:
                    wait_seconds = 2 ** (attempt - 1)

                    self.logger.write(
                        f"Transient SharePoint upload failure | "
                        f"file_path={file_path} | "
                        f"status_code={status_code} | "
                        f"attempt={attempt}/{max_attempts} | "
                        f"retrying_in={wait_seconds}s",
                        level="warning",
                    )

                    time.sleep(wait_seconds)
                    continue

                self.logger.write(
                    f"Failed to upload file to SharePoint: {file_path} | Error: {exc}",
                    level="error",
                )
                raise RuntimeError("Failed to upload file to SharePoint.") from exc

    def download_file_bytes(self, file_path: str) -> bytes:
        """
        Download raw file bytes from SharePoint.

        Args:
            file_path (str): path to file within the document library

        Returns:
            bytes: downloaded file content

        Raises:
            ValueError: if file_path is empty
            RuntimeError: if download fails
        """
        if not file_path:
            raise ValueError("file_path cannot be empty")

        download_url = self._build_file_url(file_path) + ":/content"
        access_token = self._get_access_token()

        headers = {
            "Authorization": f"Bearer {access_token}",
        }

        max_attempts = 3
        retry_status_codes = {429, 502, 503, 504}

        for attempt in range(1, max_attempts + 1):
            try:
                response = requests.get(
                    download_url,
                    headers=headers,
                    timeout=30,
                )

                # Do NOT retry file not found
                if response.status_code == 404:
                    raise FileNotFoundError(
                        f"File not found in SharePoint: {file_path}"
                    )

                # Trigger retry for transient errors
                if response.status_code in retry_status_codes:
                    raise requests.HTTPError(
                        f"{response.status_code} Server Error: transient download failure",
                        response=response,
                    )

                response.raise_for_status()

                self.logger.write(
                    f"Successfully downloaded file from SharePoint: {file_path}",
                    level="info",
                )

                return response.content

            except requests.RequestException as exc:
                status_code = getattr(exc.response, "status_code", None)
                is_retryable = status_code in retry_status_codes

                if attempt < max_attempts and is_retryable:
                    wait_seconds = 2 ** (attempt - 1)

                    self.logger.write(
                        f"Transient SharePoint download failure | "
                        f"file_path={file_path} | "
                        f"status_code={status_code} | "
                        f"attempt={attempt}/{max_attempts} | "
                        f"retrying_in={wait_seconds}s",
                        level="warning",
                    )

                    time.sleep(wait_seconds)
                    continue

                self.logger.write(
                    f"Failed to download file from SharePoint: {file_path} | Error: {exc}",
                    level="error",
                )
                raise RuntimeError("Failed to download file from SharePoint.") from exc

    def file_exists(self, file_path: str) -> bool:
        """
        Check whether a file exists in SharePoint.

        Args:
            file_path (str): path to file within the document library

        Returns:
            bool: True if file exists, False otherwise
        """
        if not file_path:
            raise ValueError("file_path cannot be empty")

        url = self._build_file_url(file_path)
        access_token = self._get_access_token()

        headers = {
            "Authorization": f"Bearer {access_token}",
        }

        try:
            response = requests.get(
                url,
                headers=headers,
                timeout=30,
            )

            if response.status_code == 200:
                return True
            elif response.status_code == 404:
                return False

            # Unexpected status
            response.raise_for_status()
            return False

        except requests.RequestException as exc:
            self.logger.write(
                f"Error checking file existence in SharePoint: {file_path} | Error: {exc}",
                level="error",
            )
            raise RuntimeError("Failed to check file existence in SharePoint.") from exc
        
    def list_files(self, folder_path: str, top: int = 100) -> list[dict]:
        """
        List files inside a SharePoint folder.

        Args:
            folder_path (str): folder path inside the document library
            top (int): max number of items to return

        Returns:
            list[dict]: list of file metadata (name, path, lastModifiedDateTime)
        """
        access_token = self._get_access_token()

        list_url = (
            f"{self.graph_base_url}/drives/{self.drive_id}"
            f"/root:/{folder_path}:/children?$top={top}"
        )

        headers = {
            "Authorization": f"Bearer {access_token}",
        }

        max_attempts = 3
        retry_statuses = {429, 502, 503, 504}

        for attempt in range(1, max_attempts + 1):
            try:
                response = requests.get(
                    list_url,
                    headers=headers,
                    timeout=30,
                )

                if response.status_code in retry_statuses and attempt < max_attempts:
                    delay = 2 ** (attempt - 1)
                    self.logger.write(
                        f"Transient error {response.status_code} listing files. "
                        f"Retrying in {delay}s (attempt {attempt}/{max_attempts})",
                        level="warning",
                    )
                    time.sleep(delay)
                    continue

                response.raise_for_status()
                data = response.json()

                items = data.get("value", [])

                results = []
                for item in items:
                    results.append({
                        "name": item.get("name"),
                        "file_path": f"{folder_path}/{item.get('name')}",
                        "last_modified": item.get("lastModifiedDateTime"),
                    })

                self.logger.write(
                    f"Listed {len(results)} files from SharePoint folder: {folder_path}",
                    level="info",
                )

                return results

            except requests.RequestException as exc:
                if attempt == max_attempts:
                    raise RuntimeError(
                        f"Failed to list files from SharePoint after {max_attempts} attempts"
                    ) from exc

                delay = 2 ** (attempt - 1)
                self.logger.write(
                    f"Request error listing files. Retrying in {delay}s "
                    f"(attempt {attempt}/{max_attempts}): {exc}",
                    level="warning",
                )
                time.sleep(delay)