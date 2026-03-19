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
    "automation@novaflowdigi.com",  # fallback
)

EMAIL_GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"

DEFAULT_REPORT_RECIPIENTS = [
    "gbisconovo@gmail.com",
]

ALERT_RECIPIENTS = [
    "gabrielbisco@novaflowdigi.com",
]