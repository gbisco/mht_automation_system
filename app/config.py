import os
from dotenv import load_dotenv

# Load .env file
load_dotenv()

# =========================
# Email / notification config
# =========================

EMAIL_TENANT_ID = os.getenv("EMAIL_TENANT_ID", "")
EMAIL_CLIENT_ID = os.getenv("EMAIL_CLIENT_ID", "")
EMAIL_CLIENT_SECRET = os.getenv("EMAIL_CLIENT_SECRET", "")

EMAIL_SENDER = os.getenv(
    "EMAIL_SENDER",
    "gabrielbisco@novaflowdigi.com",
)

EMAIL_GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"

DEFAULT_REPORT_RECIPIENTS = [
    "gbisconovo@gmail.com",
]

ALERT_RECIPIENTS = [
    "gabrielbisco@novaflowdigi.com",
]

# =========================
# SharePoint storage config
# =========================

SHAREPOINT_TENANT_ID = os.getenv("SHAREPOINT_TENANT_ID", "")
SHAREPOINT_CLIENT_ID = os.getenv("SHAREPOINT_CLIENT_ID", "")
SHAREPOINT_CLIENT_SECRET = os.getenv("SHAREPOINT_CLIENT_SECRET", "")

SHAREPOINT_SITE_ID = os.getenv("SHAREPOINT_SITE_ID", "")
SHAREPOINT_DRIVE_ID = os.getenv("SHAREPOINT_DRIVE_ID", "")

SHAREPOINT_GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"