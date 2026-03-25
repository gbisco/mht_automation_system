from typing import Any, Optional

from app import config
from app.calendar.calendar_service import CalendarService
from app.interface.email_sender import EmailSender
from app.iq_processing.daily_iq_pipeline import DailyIQPipeline
from app.logger.logger import AppLogger
from app.storage.sharepoint_storage import SharePointStorage
from app.iq_processing.daily_delta_pipeline import DailyIQDeltaPipeline


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
        self.delta_pipeline = DailyIQDeltaPipeline()

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

            # =========================
            # Store IQ output
            # =========================
            iq_storage_result = self._store_output(
                storage_method=storage_method,
                file_name=pipeline_result["file_name"],
                file_content=pipeline_result["csv_content"],
                folder=config.SHAREPOINT_IQ_OUTPUT_FOLDER,
            )

            # =========================
            # Store raw B3 file
            # =========================
            raw_b3_storage_result = self._store_output(
                storage_method=storage_method,
                file_name=pipeline_result["raw_b3_file_name"],
                file_content=pipeline_result["raw_b3_content"],
                folder=config.SHAREPOINT_B3_RAW_FOLDER,
            )

            # =========================
            # Resolve and load previous IQ output
            # =========================
            storage = SharePointStorage()

            previous_iq_file_path = self._resolve_previous_iq_file_path(
                storage=storage,
                folder_path=config.SHAREPOINT_IQ_OUTPUT_FOLDER,
                processing_date=processing_date,
            )

            previous_iq_content = self._load_previous_iq_output(previous_iq_file_path)

            previous_iq_file_name = previous_iq_file_path.rsplit("/", 1)[-1]
            previous_iq_date = self._extract_iq_date(previous_iq_file_name)

            if not previous_iq_date:
                raise ValueError(
                    f"Could not extract previous IQ date from path: {previous_iq_file_path}"
                )

            # =========================
            # Execute delta pipeline
            # =========================
            current_iq_content = pipeline_result["csv_content"]

            if isinstance(current_iq_content, str):
                current_iq_bytes = current_iq_content.encode("utf-8")
            elif isinstance(current_iq_content, bytes):
                current_iq_bytes = current_iq_content
            else:
                raise ValueError("Pipeline csv_content must be str or bytes")

            delta_result = self._execute_delta_pipeline(
                current_iq_content=current_iq_bytes,
                previous_iq_content=previous_iq_content,
            )

            # =========================
            # Store delta IQ output
            # =========================
            delta_storage_result = self._store_output(
                storage_method=storage_method,
                file_name=delta_result["file_name"],
                file_content=delta_result["csv_content"],
                folder=config.SHAREPOINT_IQ_OUTPUT_FOLDER,
            )

            # Combine results
            storage_result = {
                "iq_file": iq_storage_result,
                "raw_b3_file": raw_b3_storage_result,
                "delta_iq_file": delta_storage_result,
            }

            notification_sent = False
            notification_result = None

            if notify:
                resolved_recipients = self._resolve_recipients(recipients)
                notification_result = self._send_notification(
                    notification_method=notification_method,
                    recipients=resolved_recipients,
                    pipeline_result=pipeline_result,
                    delta_result=delta_result,
                    target_date=target_date,
                    processing_date=processing_date,
                    previous_iq_date=previous_iq_date,
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

            error_log_storage_result = self._upload_error_log(
                storage_method=storage_method
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
                "error_log_storage_result": error_log_storage_result,
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

        if "raw_b3_content" not in pipeline_result:
            raise ValueError("Pipeline result missing 'raw_b3_content'")

        if "raw_b3_file_name" not in pipeline_result:
            raise ValueError("Pipeline result missing 'raw_b3_file_name'")

        logger.write(
            f"IQ pipeline executed successfully | processing_date={processing_date} | "
            f"file_name={pipeline_result.get('file_name')} | "
            f"raw_b3_file_name={pipeline_result.get('raw_b3_file_name')}"
        )

        return pipeline_result

    def _store_output(
        self,
        storage_method: str,
        file_name: str,
        file_content: Any,
        folder: str,
    ) -> dict[str, Any]:
        """
        Store a single generated file using requested backend.

        Args:
            storage_method (str): Storage backend to use
            file_name (str): Name of file to store
            file_content (Any): File content as str or bytes
            folder (str): SharePoint folder path

        Returns:
            dict[str, Any]: Storage result
        """
        logger.write(
            f"Storing file output | storage_method={storage_method} | "
            f"folder={folder} | file_name={file_name}"
        )

        try:
            if storage_method == "sharepoint":
                storage = SharePointStorage()

                # Build SharePoint path
                file_path = f"{folder}/{file_name}"

                # Normalize content to bytes for SharePoint upload
                if isinstance(file_content, str):
                    file_bytes = file_content.encode("utf-8")
                elif isinstance(file_content, bytes):
                    file_bytes = file_content
                else:
                    raise ValueError("file_content must be str or bytes")

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
                f"folder={folder} | file_name={file_name} | error={e}"
            )
            raise

        logger.write(
            f"Output stored successfully | storage_method={storage_method} | "
            f"folder={folder} | file_name={file_name}"
        )

        return storage_result
    
    def _execute_delta_pipeline(
        self,
        current_iq_content: bytes,
        previous_iq_content: bytes,
    ) -> dict[str, str]:
        """
        Execute the delta IQ pipeline using current and previous IQ CSV content.

        Both inputs are expected as raw UTF-8 encoded CSV bytes. This method
        decodes them, runs the delta pipeline, and packages the resulting CSV
        content together with the generic delta output filename.

        Args:
            current_iq_content (bytes): Current IQ CSV content as raw bytes.
            previous_iq_content (bytes): Previous IQ CSV content as raw bytes.

        Raises:
            ValueError: If either input is not bytes.

        Returns:
            dict[str, str]: Dictionary containing:
                - csv_content: Delta CSV content
                - file_name: Delta output file name
        """
        logger.write("Executing delta IQ pipeline")

        try:
            if not isinstance(current_iq_content, bytes):
                raise ValueError("current_iq_content must be bytes")

            if not isinstance(previous_iq_content, bytes):
                raise ValueError("previous_iq_content must be bytes")

            current_csv = current_iq_content.decode("utf-8")
            previous_csv = previous_iq_content.decode("utf-8")

            delta_csv_content = self.delta_pipeline.run(
                current_csv=current_csv,
                previous_csv=previous_csv,
            )

            delta_result = {
                "csv_content": delta_csv_content,
                "file_name": self.delta_pipeline.get_output_filename(),
            }

        except Exception as e:
            logger.exception(f"Failed to execute delta IQ pipeline | error={e}")
            raise

        logger.write(
            f"Delta IQ pipeline executed successfully | "
            f"file_name={delta_result['file_name']}"
        )

        return delta_result
    
    def _load_previous_iq_output(
        self,
        previous_iq_file_path: str,
    ) -> bytes:
        """
        Load the previous IQ CSV output from SharePoint.

        Args:
            previous_iq_file_path (str): Full SharePoint file path to previous IQ CSV

        Returns:
            bytes: Previous IQ CSV file content as raw bytes

        Raises:
            FileNotFoundError: If the expected previous IQ file does not exist
        """
        logger.write(
            f"Loading previous IQ output from SharePoint | "
            f"file_path={previous_iq_file_path}"
        )

        try:
            storage = SharePointStorage()

            if not storage.file_exists(previous_iq_file_path):
                raise FileNotFoundError(
                    f"Previous IQ output not found in SharePoint: {previous_iq_file_path}"
                )

            previous_iq_bytes = storage.download_file_bytes(previous_iq_file_path)

        except Exception as e:
            logger.exception(
                f"Failed to load previous IQ output | "
                f"file_path={previous_iq_file_path} | error={e}"
            )
            raise

        logger.write(
            f"Previous IQ output loaded successfully | "
            f"file_path={previous_iq_file_path}"
        )

        return previous_iq_bytes

    def _send_notification(
        self,
        notification_method: str,
        recipients: list[str],
        pipeline_result: dict[str, Any],
        delta_result: dict[str, Any],
        target_date: str,
        processing_date: str,
        previous_iq_date: str,
        storage_result: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Send post-run notification.

        Args:
            notification_method (str): Notification backend to use
            recipients (list[str]): Final recipient list
            pipeline_result (dict[str, Any]): Output returned by IQ pipeline
            delta_result (dict[str, Any]): Output returned by delta pipeline
            target_date (str): Date evaluated for execution
            processing_date (str): Previous trading day actually processed
            previous_iq_date (str): Previous trading day used for delta comparison
            storage_result (dict[str, Any]): Result returned by storage backend

        Returns:
            dict[str, Any]: Notification result
        """
        file_name = pipeline_result.get("file_name")
        csv_content = pipeline_result.get("csv_content")

        delta_file_name = delta_result.get("file_name")
        delta_csv_content = delta_result.get("csv_content")

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
                previous_iq_date=previous_iq_date,
                pipeline_result=pipeline_result,
                delta_result=delta_result,
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

            if isinstance(delta_csv_content, str):
                delta_csv_bytes = delta_csv_content.encode("utf-8")
            elif isinstance(delta_csv_content, bytes):
                delta_csv_bytes = delta_csv_content
            else:
                raise ValueError("Delta pipeline csv_content must be str or bytes")

            email_sender.add_attachment(
                file_name=delta_file_name,
                file_bytes=delta_csv_bytes,
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
        return f"IQ Report | Delta IQ | processing_date={processing_date}"

    def _build_notification_body(
        self,
        target_date: str,
        processing_date: str,
        previous_iq_date: str,
        pipeline_result: dict[str, Any],
        delta_result: dict[str, Any],
        storage_result: dict[str, Any],
    ) -> str:
        """
        Build notification body.

        Args:
            target_date (str): Date evaluated for execution
            processing_date (str): Previous trading day actually processed
            previous_iq_date (str): Previous trading day used for delta comparison
            pipeline_result (dict[str, Any]): Output returned by IQ pipeline
            delta_result (dict[str, Any]): Output returned by delta pipeline
            storage_result (dict[str, Any]): Result returned by storage backend

        Returns:
            str
        """
        file_name = pipeline_result.get("file_name")
        web_url = storage_result.get("iq_file", {}).get("web_url")

        delta_file_name = delta_result.get("file_name")
        delta_web_url = storage_result.get("delta_iq_file", {}).get("web_url")

        body = f"""
        <html>
            <body>
                <p>Daily IQ report generated successfully.</p>

                <p><strong>Target Date:</strong> {target_date}</p>
                <p><strong>Processing Date:</strong> {processing_date}</p>

                <p><strong>IQ File:</strong> {file_name}</p>
                {f'<p><strong>View IQ File:</strong> <a href="{web_url}">Open in SharePoint</a></p>' if web_url else ''}

                <br>

                <p><strong>Delta File:</strong> {delta_file_name}</p>
                <p><strong>Delta Comparison:</strong> {previous_iq_date} vs {processing_date}</p>
                {f'<p><strong>View Delta File:</strong> <a href="{delta_web_url}">Open in SharePoint</a></p>' if delta_web_url else ''}

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
    

    def _upload_error_log(self, storage_method: str) -> Optional[dict[str, Any]]:
        """
        Upload the current error log file to storage.

        Args:
            storage_method (str): Storage backend to use

        Returns:
            Optional[dict[str, Any]]: Storage result if uploaded, otherwise None
        """
        logger.write(
            f"Uploading error log | storage_method={storage_method}",
            level="warning",
        )

        try:
            if storage_method != "sharepoint":
                raise ValueError(f"Unsupported storage_method: {storage_method}")

            storage = SharePointStorage()

            error_log_path = logger.get_error_log_path()
            if not error_log_path.exists():
                logger.write("Error log file does not exist. Nothing to upload.", level="warning")
                return None

            file_name = error_log_path.name
            file_bytes = error_log_path.read_bytes()
            file_path = f"{config.SHAREPOINT_ERROR_LOGS_FOLDER}/{file_name}"

            result = storage.upload_file_bytes(
                file_path=file_path,
                file_bytes=file_bytes,
                content_type="text/plain",
            )

            logger.write(
                f"Error log uploaded successfully | file_path={file_path}",
                level="info",
            )
            return result

        except Exception as upload_exc:
            logger.exception(f"Failed to upload error log | error={upload_exc}")
            return None
        
            
    def _extract_iq_date(self, filename: str) -> str | None:
        """
        Extract IQ date from a dated IQ output file name.

        Expected format:
            iq_coef_YYYYMMDD.csv

        Args:
            filename (str): IQ file name

        Returns:
            str | None: Extracted date in YYYY-MM-DD format, or None if invalid
        """
        if not filename.startswith("iq_coef_") or not filename.endswith(".csv"):
            return None

        raw = filename.replace("iq_coef_", "").replace(".csv", "")

        if len(raw) != 8 or not raw.isdigit():
            return None

        return f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"


    def _resolve_previous_iq_file_path(
        self,
        storage: SharePointStorage,
        folder_path: str,
        processing_date: str,
    ) -> str:
        """
        Resolve the latest prior IQ file path in SharePoint based on processing date.

        Args:
            storage (SharePointStorage): SharePoint storage client
            folder_path (str): SharePoint folder path containing IQ outputs
            processing_date (str): Current processing date in YYYY-MM-DD format

        Returns:
            str: Full SharePoint file path for the latest prior IQ file

        Raises:
            FileNotFoundError: If no prior IQ file is found
        """
        logger.write(
            f"Resolving previous IQ file path | "
            f"folder_path={folder_path} | processing_date={processing_date}"
        )

        files = storage.list_files(folder_path)

        candidates = []

        for file_info in files:
            name = file_info.get("name")
            path = file_info.get("file_path")

            if not name or not path:
                continue

            file_date = self._extract_iq_date(name)

            if not file_date:
                continue

            if file_date >= processing_date:
                continue

            candidates.append((file_date, path))

        if not candidates:
            raise FileNotFoundError(
                f"No previous IQ file found for processing_date={processing_date}"
            )

        candidates.sort(key=lambda x: x[0], reverse=True)

        previous_file_path = candidates[0][1]

        logger.write(
            f"Resolved previous IQ file path successfully | "
            f"processing_date={processing_date} | previous_file_path={previous_file_path}"
        )

        return previous_file_path
