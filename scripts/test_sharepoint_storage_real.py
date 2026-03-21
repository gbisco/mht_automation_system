from pathlib import Path

from app import config
from app.storage.sharepoint_storage import SharePointStorage


def main() -> None:
    """
    Run a real SharePoint storage integration test.

    Steps:
    - Load fixture file from tests/fixtures
    - Upload file to SharePoint
    - Check if uploaded file exists
    - Download file back from SharePoint
    - Compare downloaded bytes to original bytes
    """
    # Initialize SharePoint storage from config
    storage = SharePointStorage()

    # Select local fixture file
    fixture_path = Path("tests/fixtures/b3_derivatives_2026_03_18.csv")

    if not fixture_path.exists():
        raise FileNotFoundError(f"Fixture file not found: {fixture_path}")

    file_bytes = fixture_path.read_bytes()

    # Define test file path inside SharePoint document library
    sharepoint_file_path = (
        f"{config.SHAREPOINT_B3_RAW_FOLDER}/b3_derivatives_2026_03_18.csv"
    )

    print(f"Uploading file to SharePoint: {sharepoint_file_path}")
    upload_result = storage.upload_file_bytes(
        file_path=sharepoint_file_path,
        file_bytes=file_bytes,
        content_type="text/csv",
    )
    print("Upload result:")
    print(upload_result)

    print(f"\nChecking if file exists: {sharepoint_file_path}")
    exists = storage.file_exists(sharepoint_file_path)
    print(f"Exists: {exists}")

    if not exists:
        raise RuntimeError("Uploaded file was not found in SharePoint.")

    print(f"\nDownloading file from SharePoint: {sharepoint_file_path}")
    downloaded_bytes = storage.download_file_bytes(sharepoint_file_path)

    print("\nComparing original and downloaded file bytes...")
    if downloaded_bytes != file_bytes:
        raise RuntimeError("Downloaded file does not match uploaded file.")

    print("Success: downloaded file matches uploaded file.")


if __name__ == "__main__":
    main()