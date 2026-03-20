from typing import Any, Optional

from app import config
from app.calendar.calendar_service import CalendarService
from app.interface.email_sender import EmailSender
from app.iq_processing.daily_iq_pipeline import DailyIQPipeline
from app.logger.logger import AppLogger
from app.storage.sharepoint_storage import SharePointStorage


logger = AppLogger(__name__)


class DailyIQJob:
    """
    Service responsible for:
    - Executing the Daily IQ automation workflow
    - Validating target date execution rules
    - Resolving processing date from the market calendar
    - Running the IQ pipeline
    - Storing output
    - Sending notifications if requested

    Internal:
    - pipeline (DailyIQPipeline): IQ processing pipeline
    """

    def __init__(self):
        """
        Initialize Daily IQ job dependencies.
        """
        self.pipeline = DailyIQPipeline()

    # =========================
    # Public method
    # =========================

    def execute(
        self,
        target_date: str,
        storage_method: str = "sharepoint",
        notify: bool = False,
        notification_method: str = "email",
        recipients: Optional[list[str]] = None,
    ) -> dict[str, Any]:
        """
        Execute the Daily IQ job.

        Args:
            target_date (str): Date being evaluated in YYYY-MM-DD format
            storage_method (str): Storage backend to use
            notify (bool): Whether notification should be sent
            notification_method (str): Notification backend to use
            recipients (Optional[list[str]]): Optional recipient override

        Returns:
            dict[str, Any]: Structured job result
        """
        logger.write(
            f"Starting Daily IQ job | target_date={target_date} | "
            f"storage_method={storage_method} | notify={notify} | "
            f"notification_method={notification_method}"
        )

        try:
            self._validate_request(
                target_date=target_date,
                storage_method=storage_method,
                notify=notify,
                notification_method=notification_method,
                recipients=recipients,
            )

            calendar_service = self._build_calendar_service(target_date)

            if not self._is_target_date_trading_day(calendar_service):
                logger.write(
                    f"Daily IQ job stopped | target_date={target_date} "
                    f"is not a trading day",
                    level="warning",
                )
                return {
                    "status": "skipped",
                    "message": f"Target date {target_date} is not a trading day.",
                    "target_date": target_date,
                    "processing_date": None,
                    "file_name": None,
                    "storage_method": None,
                    "storage_result": None,
                    "notification_sent": False,
                    "notification_method": None,
                    "notification_result": None,
                    "error": None,
                }

            processing_date = self._resolve_processing_date(calendar_service)
            pipeline_result = self._execute_pipeline(processing_date)

            storage_result = self._store_output(
                storage_method=storage_method,
                pipeline_result=pipeline_result,
            )

            notification_sent = False
            notification_result = None

            if notify:
                resolved_recipients = self._resolve_recipients(recipients)
                notification_result = self._send_notification(
                    notification_method=notification_method,
                    recipients=resolved_recipients,
                    pipeline_result=pipeline_result,
                    target_date=target_date,
                    processing_date=processing_date,
                    storage_result=storage_result,
                )
                notification_sent = True

            logger.write(
                f"Daily IQ job completed successfully | target_date={target_date} | "
                f"processing_date={processing_date} | "
                f"file_name={self._extract_file_name(pipeline_result)}"
            )

            return {
                "status": "success",
                "message": "Daily IQ job completed successfully.",
                "target_date": target_date,
                "processing_date": processing_date,
                "file_name": self._extract_file_name(pipeline_result),
                "storage_method": storage_method,
                "storage_result": storage_result,
                "notification_sent": notification_sent,
                "notification_method": notification_method if notify else None,
                "notification_result": notification_result,
                "error": None,
            }

        except Exception as e:
            logger.exception(
                f"Daily IQ job failed | target_date={target_date} | error={e}"
            )
            return {
                "status": "failed",
                "message": "Daily IQ job failed.",
                "target_date": target_date,
                "processing_date": None,
                "file_name": None,
                "storage_method": storage_method,
                "storage_result": None,
                "notification_sent": False,
                "notification_method": notification_method if notify else None,
                "notification_result": None,
                "error": str(e),
            }

    # =========================
    # Internal helper methods
    # =========================

    def _validate_request(
        self,
        target_date: str,
        storage_method: str,
        notify: bool,
        notification_method: str,
        recipients: Optional[list[str]],
    ) -> None:
        """
        Validate input request parameters.

        Args:
            target_date (str): Date being evaluated in YYYY-MM-DD format
            storage_method (str): Storage backend to use
            notify (bool): Whether notification should be sent
            notification_method (str): Notification backend to use
            recipients (Optional[list[str]]): Optional recipient override
        """

        logger.write("Validating Daily IQ job request")

        # =========================
        # Validate target_date
        # =========================
        if not target_date:
            raise ValueError("target_date is required")

        if not isinstance(target_date, str):
            raise ValueError("target_date must be a string in 'YYYY-MM-DD' format")

        try:
            from datetime import datetime
            datetime.strptime(target_date, "%Y-%m-%d")
        except ValueError:
            raise ValueError("target_date must be in 'YYYY-MM-DD' format")

        # =========================
        # Validate storage_method
        # =========================
        supported_storage_methods = {"sharepoint"}

        if not storage_method:
            raise ValueError("storage_method is required")

        if storage_method not in supported_storage_methods:
            raise ValueError(
                f"Unsupported storage_method: {storage_method}. "
                f"Supported: {supported_storage_methods}"
            )

        # =========================
        # Validate notify flag
        # =========================
        if not isinstance(notify, bool):
            raise ValueError("notify must be a boolean")

        # =========================
        # Validate notification_method
        # =========================
        supported_notification_methods = {"email"}

        if notify:
            if not notification_method:
                raise ValueError("notification_method is required when notify=True")

            if notification_method not in supported_notification_methods:
                raise ValueError(
                    f"Unsupported notification_method: {notification_method}. "
                    f"Supported: {supported_notification_methods}"
                )

        # =========================
        # Validate recipients
        # =========================
        if recipients is not None:
            if not isinstance(recipients, list):
                raise ValueError("recipients must be a list of strings")

            if len(recipients) == 0:
                raise ValueError("recipients list cannot be empty")

            for r in recipients:
                if not isinstance(r, str):
                    raise ValueError("all recipients must be strings")

        logger.write("Request validation successful")

    def _build_calendar_service(self, target_date: str) -> CalendarService:
        """
        Build calendar service for target date.

        Args:
            target_date (str): Date being evaluated in YYYY-MM-DD format

        Returns:
            CalendarService
        """
        logger.write(f"Building CalendarService | target_date={target_date}")

        try:
            calendar_service = CalendarService(
                calendar_path=config.MARKET_CALENDAR_PATH,
                target_date=target_date,
            )
        except Exception as e:
            logger.exception(
                f"Failed to initialize CalendarService | target_date={target_date} | error={e}"
            )
            raise

        logger.write("CalendarService initialized successfully")

        return calendar_service

    def _is_target_date_trading_day(self, calendar_service: CalendarService) -> bool:
        """
        Check if target date is a trading day.

        Args:
            calendar_service (CalendarService): Initialized calendar service

        Returns:
            bool: True if target date is open, False otherwise
        """
        target_date = calendar_service.get_target_date()

        logger.write(f"Checking trading day status | target_date={target_date}")

        try:
            is_trading_day = calendar_service.is_trading_day()
        except Exception as e:
            logger.exception(
                f"Failed to check trading day | target_date={target_date} | error={e}"
            )
            raise

        if is_trading_day:
            logger.write(f"Trading day confirmed | target_date={target_date}")
        else:
            logger.write(
                f"Non-trading day detected | target_date={target_date}",
                level="warning",
            )

        return is_trading_day

    def _resolve_processing_date(self, calendar_service: CalendarService) -> str:
        """
        Resolve the processing date for the IQ pipeline.

        Business rule:
        - target_date must be a trading day
        - processing_date is the previous trading day before target_date

        Args:
            calendar_service (CalendarService): Initialized calendar service

        Returns:
            str: Processing date in YYYY-MM-DD format
        """
        target_date = calendar_service.get_target_date()

        logger.write(f"Resolving processing date | target_date={target_date}")

        try:
            previous_trading_day = calendar_service.get_previous_trading_day()
        except Exception as e:
            logger.exception(
                f"Failed to resolve processing date | target_date={target_date} | error={e}"
            )
            raise

        if previous_trading_day is None:
            raise ValueError(
                f"No previous trading day found for target_date={target_date}"
            )

        processing_date = previous_trading_day.strftime("%Y-%m-%d")

        logger.write(
            f"Processing date resolved | target_date={target_date} | "
            f"processing_date={processing_date}"
        )

        return processing_date

    def _execute_pipeline(self, processing_date: str) -> dict[str, Any]:
        """
        Run the IQ pipeline for the resolved processing date.

        Args:
            processing_date (str): Previous trading day in YYYY-MM-DD format

        Returns:
            dict[str, Any]: Pipeline output
        """
        logger.write(f"Executing IQ pipeline | processing_date={processing_date}")

        try:
            pipeline_result = self.pipeline.run(processing_date)
        except Exception as e:
            logger.exception(
                f"Failed to execute IQ pipeline | processing_date={processing_date} | error={e}"
            )
            raise

        if not isinstance(pipeline_result, dict):
            raise ValueError("Pipeline result must be a dictionary")

        if "csv_content" not in pipeline_result:
            raise ValueError("Pipeline result missing 'csv_content'")

        if "file_name" not in pipeline_result:
            raise ValueError("Pipeline result missing 'file_name'")

        logger.write(
            f"IQ pipeline executed successfully | processing_date={processing_date} | "
            f"file_name={pipeline_result.get('file_name')}"
        )

        return pipeline_result

    def _store_output(
        self,
        storage_method: str,
        pipeline_result: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Store generated output using requested backend.

        Args:
            storage_method (str): Storage backend to use
            pipeline_result (dict[str, Any]): Output returned by pipeline

        Returns:
            dict[str, Any]: Storage result
        """
        file_name = pipeline_result.get("file_name")
        csv_content = pipeline_result.get("csv_content")

        logger.write(
            f"Storing pipeline output | storage_method={storage_method} | file_name={file_name}"
        )

        try:
            if storage_method == "sharepoint":
                storage = SharePointStorage()

                # Keep path simple and consistent
                file_path = f"test/{file_name}"

                # Normalize content to bytes for SharePoint upload
                if isinstance(csv_content, str):
                    file_bytes = csv_content.encode("utf-8")
                elif isinstance(csv_content, bytes):
                    file_bytes = csv_content
                else:
                    raise ValueError("Pipeline csv_content must be str or bytes")

                storage_result = storage.upload_file_bytes(
                    file_path=file_path,
                    file_bytes=file_bytes,
                    content_type="text/csv",
                )

            else:
                raise ValueError(f"Unsupported storage_method: {storage_method}")

        except Exception as e:
            logger.exception(
                f"Failed to store output | storage_method={storage_method} | "
                f"file_name={file_name} | error={e}"
            )
            raise

        logger.write(
            f"Output stored successfully | storage_method={storage_method} | "
            f"file_name={file_name}"
        )

        return storage_result

    def _send_notification(
        self,
        notification_method: str,
        recipients: list[str],
        pipeline_result: dict[str, Any],
        target_date: str,
        processing_date: str,
        storage_result: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Send post-run notification.

        Args:
            notification_method (str): Notification backend to use
            recipients (list[str]): Final recipient list
            pipeline_result (dict[str, Any]): Output returned by pipeline
            target_date (str): Date evaluated for execution
            processing_date (str): Previous trading day actually processed
            storage_result (dict[str, Any]): Result returned by storage backend

        Returns:
            dict[str, Any]: Notification result
        """
        file_name = pipeline_result.get("file_name")
        csv_content = pipeline_result.get("csv_content")

        logger.write(
            f"Sending notification | notification_method={notification_method} | "
            f"target_date={target_date} | processing_date={processing_date}"
        )

        try:
            if notification_method != "email":
                raise ValueError(
                    f"Unsupported notification_method: {notification_method}"
                )

            email_sender = EmailSender()

            subject = self._build_notification_subject(
                target_date=target_date,
                processing_date=processing_date,
            )

            body = self._build_notification_body(
                target_date=target_date,
                processing_date=processing_date,
                pipeline_result=pipeline_result,
                storage_result=storage_result,
            )

            email_sender.create_email(
                subject=subject,
                html_body=body,
                recipients=recipients,
            )

            if isinstance(csv_content, str):
                csv_bytes = csv_content.encode("utf-8")
            elif isinstance(csv_content, bytes):
                csv_bytes = csv_content
            else:
                raise ValueError("Pipeline csv_content must be str or bytes")

            email_sender.add_attachment(
                file_name=file_name,
                file_bytes=csv_bytes,
                content_type="text/csv",
            )

            notification_result = email_sender.send()

        except Exception as e:
            logger.exception(
                f"Failed to send notification | notification_method={notification_method} | "
                f"target_date={target_date} | processing_date={processing_date} | error={e}"
            )
            raise

        logger.write(
            f"Notification sent successfully | notification_method={notification_method} | "
            f"recipients={recipients}"
        )

        return notification_result

    def _resolve_recipients(self, recipients: Optional[list[str]]) -> list[str]:
        """
        Resolve notification recipients.

        Rules:
        - Use passed recipients if provided
        - Otherwise use config.DEFAULT_REPORT_RECIPIENTS

        Args:
            recipients (Optional[list[str]]): Optional recipient override

        Returns:
            list[str]: Final recipient list
        """
        if recipients is not None:
            logger.write(f"Using provided recipients | count={len(recipients)}")
            return recipients

        default_recipients = config.DEFAULT_REPORT_RECIPIENTS

        if not default_recipients:
            raise ValueError("DEFAULT_REPORT_RECIPIENTS is not configured")

        logger.write(
            f"Using default recipients from config | count={len(default_recipients)}"
        )

        return default_recipients

    def _build_notification_subject(
        self,
        target_date: str,
        processing_date: str,
    ) -> str:
        """
        Build notification subject.

        Args:
            target_date (str): Date evaluated for execution
            processing_date (str): Previous trading day actually processed

        Returns:
            str
        """
        return f"IQ Report | processing_date={processing_date}"

    def _build_notification_body(
        self,
        target_date: str,
        processing_date: str,
        pipeline_result: dict[str, Any],
        storage_result: dict[str, Any],
    ) -> str:
        """
        Build notification body.

        Args:
            target_date (str): Date evaluated for execution
            processing_date (str): Previous trading day actually processed
            pipeline_result (dict[str, Any]): Output returned by pipeline
            storage_result (dict[str, Any]): Result returned by storage backend

        Returns:
            str
        """
        file_name = pipeline_result.get("file_name")
        web_url = storage_result.get("web_url")

        body = f"""
        <html>
            <body>
                <p>Daily IQ report generated successfully.</p>

                <p><strong>Target Date:</strong> {target_date}</p>
                <p><strong>Processing Date:</strong> {processing_date}</p>
                <p><strong>File Name:</strong> {file_name}</p>

                {f'<p><strong>View File:</strong> <a href="{web_url}">Open in SharePoint</a></p>' if web_url else ''}

                <br>

                <p>This is an automated message.</p>
            </body>
        </html>
        """

        return body

    def _extract_file_name(self, pipeline_result: dict[str, Any]) -> Optional[str]:
        """
        Extract file name from pipeline result.

        Args:
            pipeline_result (dict[str, Any]): Output returned by pipeline

        Returns:
            Optional[str]
        """
        file_name = pipeline_result.get("file_name")

        if file_name is None:
            logger.write("No file_name found in pipeline_result", level="warning")

        return file_name