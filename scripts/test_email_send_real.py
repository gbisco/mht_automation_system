from pathlib import Path

import app.config as config
from app.interface.email_sender import EmailSender


def main() -> None:
    """
    Send a real test email using configured sender and default recipients.
    """
    # Initialize sender from config
    email_sender = EmailSender()

    # Use first configured default recipient for the real test
    recipient = config.DEFAULT_REPORT_RECIPIENTS[0]

    # Pick one fixture file to attach
    fixture_path = Path("tests/fixtures/b3_derivatives_2026_03_18.csv")

    if not fixture_path.exists():
        raise FileNotFoundError(f"Fixture file not found: {fixture_path}")

    file_bytes = fixture_path.read_bytes()

    # Build test email
    email_sender.create_email(
        subject="Test Email - MHT Automation System",
        html_body="""
        <html>
            <body>
                <h2>MHT Automation Test Email</h2>
                <p>This is a real test email sent from the EmailSender service.</p>
                <p>If you received this, the Microsoft Graph email integration is working.</p>
            </body>
        </html>
        """,
        recipients=[recipient],
    )

    # Attach fixture file
    email_sender.add_attachment(
        file_name=fixture_path.name,
        file_bytes=file_bytes,
    )

    # Send email
    result = email_sender.send()

    print("Email send result:")
    print(result)


if __name__ == "__main__":
    main()