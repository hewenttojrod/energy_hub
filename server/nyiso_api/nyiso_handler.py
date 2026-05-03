import requests
from django.utils import timezone

from .nyiso_models import nyiso_report
from .nyiso_page_handler import NyisoPageHandler
from .nyiso_page_parser import report_menu_parser, report_parser


class nyiso_handler:
    """Orchestrate NYISO bootstrap by composing page-data and parsing handlers."""

    INCOMPLETE_TASK_STATES = {
        nyiso_report.task_state.QUEUED,
        nyiso_report.task_state.RUNNING,
    }

    def __init__(self, force_reinsert: bool = False, queue_tasks: bool = True) -> None:
        """Create handler instance, sync menu records, and optionally queue per-report refresh tasks."""
        self.queued_report_ids: list[int] = []
        self.page_data_handler = NyisoPageHandler()
        self.queue_tasks = queue_tasks

        if force_reinsert:
            nyiso_report.objects.all().delete()

        self.initialize_report_data()

    def initialize_report_data(self) -> None:
        """Ensure nyiso_report rows exist from menu links and queue background detail refreshes."""
        try:
            menu_html = self.page_data_handler.get_menu_html()
            parsed_reports = report_menu_parser(menu_html, nyiso_report)
            menu_records: list[dict[str, object]] = []
            for report in parsed_reports:
                report_code = str(report.get("code", ""))
                if not report_code:
                    continue
                menu_records.append(report)

            saved_reports = self._populate_report_table(menu_records)
            if self.queue_tasks:
                for report in saved_reports:
                    if self._enqueue_report_refresh_task(report) and report.pk is not None:
                        self.queued_report_ids.append(report.pk)
        except requests.RequestException:
            # Preserve existing DB rows when NYISO is unavailable.
            pass



    def _has_incomplete_task(self, report: nyiso_report) -> bool:
        """Return True when a report already has an active queue/running Celery task."""
        return report.task_status in self.INCOMPLETE_TASK_STATES and bool(report.active_task_id)

    def _enqueue_report_refresh_task(self, report: nyiso_report) -> bool:
        """Queue detail refresh when needed and always retry PARTIAL parse rows."""
        if self._has_incomplete_task(report):
            return False

        page_ref = (report.source_page or "").strip() or f"{report.code}list"
        report_cache_max_age = report.get_page_cache_max_age()
       
        if (not report.parse_status == nyiso_report.parse_state.PARTIAL 
        and not self.page_data_handler.requires_page_pull(page_ref, cache_max_age=report_cache_max_age)):
            return False

        from ..tasks import refresh_nyiso_report_details

        async_result = refresh_nyiso_report_details.delay(report.nyiso_report_id)
        report.task_status = nyiso_report.task_state.QUEUED
        report.active_task_id = async_result.id
        report.task_updated_at = timezone.now()
        report.save(update_fields=["task_status", "active_task_id", "task_updated_at", "updated_at"])
        return True

    def _populate_report_table(self, records: list[dict[str, object]]) -> list[nyiso_report]:
        """Upsert menu-derived rows and return the associated model records."""
        if not records:
            return []

        model_records: list[nyiso_report] = []

        for record in records:
            report_code = str(record.get("code", ""))
            report_name = str(record.get("name", ""))
            if not report_code:
                continue

            source_page = str(record.get("source_page", "")).strip()
            content_type = str(record.get("content_type", nyiso_report.report_content_type.FILE_LIST))
            menu_frequency = [
                item
                for item in list(record.get("frequency", []))
                if isinstance(item, str) and item
            ]

            model_record, _ = nyiso_report.objects.update_or_create(
                code=report_code,
                name=report_name,
                defaults={
                    "source_page": source_page,
                    "content_type": content_type,
                    "is_deprecated": False,
                },
            )

            if menu_frequency:
                model_record.frequency = menu_frequency
                model_record.save(update_fields=["frequency", "updated_at"])

            model_records.append(model_record)

        return model_records
