from typing import Any
import requests
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

            self.logger.write(
                "Successfully acquired Microsoft Graph access token for SharePoint.",
                level="info",
            )
            return access_token

        except requests.RequestException as exc:
            self.logger.write(
                f"Failed to acquire Microsoft Graph access token for SharePoint: {exc}",
                level="error",
            )
            raise RuntimeError("Failed to acquire Microsoft Graph access token for SharePoint.") from exc

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

        try:
            response = requests.put(
                upload_url,
                headers=headers,
                data=file_bytes,
                timeout=30,
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

        try:
            response = requests.get(
                download_url,
                headers=headers,
                timeout=30,
            )

            if response.status_code == 404:
                raise FileNotFoundError(f"File not found in SharePoint: {file_path}")

            response.raise_for_status()

            self.logger.write(
                f"Successfully downloaded file from SharePoint: {file_path}",
                level="info",
            )

            return response.content  # raw bytes

        except requests.RequestException as exc:
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
        
    def list_files(
        self,
        folder_path: str,
        top_n: int | None = None,
    ) -> list[dict[str, Any]]:
        """
        List files in a SharePoint folder.

        Args:
            folder_path (str): Path to folder in SharePoint
            top_n (int | None): Optional limit of number of files to return (most recent first)

        Returns:
            list[dict[str, Any]]: List of file metadata sorted by last_modified (descending)
        """
        self.logger.write(
            f"Listing files in SharePoint folder | folder_path={folder_path} | top_n={top_n}"
        )

        try:
            endpoint = f"/drives/{self.drive_id}/root:/{folder_path}:/children"
            url = f"{self.base_url}{endpoint}"

            response = requests.get(url, headers=self._get_headers())
            response.raise_for_status()

            items = response.json().get("value", [])

            files = []
            for item in items:
                # Skip folders
                if "file" not in item:
                    continue

                files.append({
                    "name": item.get("name"),
                    "file_path": f"{folder_path}/{item.get('name')}",
                    "web_url": item.get("webUrl"),
                    "last_modified": item.get("lastModifiedDateTime"),
                })

            # Sort by last modified (newest first)
            files.sort(
                key=lambda x: x.get("last_modified") or "",
                reverse=True,
            )

            # Apply optional limit
            if top_n is not None:
                files = files[:top_n]

        except Exception as e:
            self.logger.exception(
                f"Failed to list files in SharePoint folder | "
                f"folder_path={folder_path} | error={e}"
            )
            raise

        self.logger.write(
            f"Retrieved {len(files)} files from SharePoint folder | folder_path={folder_path}"
        )

        return files