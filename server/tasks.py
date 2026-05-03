from celery import shared_task
from datetime import timedelta
from django.utils import timezone


@shared_task(bind=True, max_retries=3, default_retry_delay=60)
def refresh_nyiso_report_details(self, report_id: int, force_refresh: bool = False) -> str:
    """Refresh one NYISO report's detail metadata and persist status back to the same row."""
    from .nyiso_api.nyiso_page_handler import NyisoPageHandler
    from .nyiso_api.nyiso_page_parser import report_parser
    from .nyiso_api.nyiso_models import nyiso_report

    report = nyiso_report.objects.filter(pk=report_id).first()
    if report is None:
        return f"missing report: {report_id}"

    report.task_status = nyiso_report.task_state.RUNNING
    report.active_task_id = self.request.id or ""
    report.task_updated_at = timezone.now()
    report.save(update_fields=["task_status", "active_task_id", "task_updated_at", "updated_at"])

    page_handler = NyisoPageHandler(
        request_delay_min_seconds=3.0,
        request_delay_max_seconds=9.0,
    )
    page_ref = (report.source_page or "").strip() or f"{report.code}list"
    is_partial_retry = report.parse_status == nyiso_report.parse_state.PARTIAL
    report_cache_max_age = timedelta(seconds=0) if force_refresh else report.get_page_cache_max_age()

    # PARTIAL reports must always attempt to reparse. Replace the None skip-signal with a large
    # timedelta so the task reads from whatever cached HTML exists, or fetches fresh if missing.
    if report_cache_max_age is None and is_partial_retry:
        report_cache_max_age = timedelta(days=365)

    if report_cache_max_age is None:
        report.task_status = nyiso_report.task_state.COMPLETED
        report.active_task_id = ""
        report.task_updated_at = timezone.now()
        report.save(update_fields=["task_status", "active_task_id", "task_updated_at", "updated_at"])
        return f"skipped deprecated: {report.code}"

    if not page_handler.is_page_accessible(page_ref, cache_max_age=report_cache_max_age):
        report.parse_status = nyiso_report.parse_state.FAILED
        report.last_scanned_at = timezone.now()
        report.task_status = nyiso_report.task_state.FAILED
        report.active_task_id = ""
        report.task_updated_at = timezone.now()
        report.save(
            update_fields=[
                "parse_status",
                "last_scanned_at",
                "task_status",
                "active_task_id",
                "task_updated_at",
                "updated_at",
            ]
        )
        return f"inaccessible: {report.code}"

    try:
        detail_html = page_handler.get_page_content_html(page_ref, cache_max_age=report_cache_max_age)
        base_record: dict[str, object] = {
            "code": report.code,
            "name": report.name,
            "frequency": list(report.frequency),
            "file_type": list(report.file_type),
            "content_type": report.content_type,
            "source_page": report.source_page,
            "file_name_format": report.file_name_format,
            "parse_status": report.parse_status,
            "last_scanned_at": report.last_scanned_at,
            "latest_report_stamp": report.latest_report_stamp,
            "earliest_report_stamp": report.earliest_report_stamp,
            "is_deprecated": report.is_deprecated,
        }

        enriched = report_parser(base_record, detail_html, nyiso_report)
        latest_stamp = enriched.get("latest_report_stamp")

        report.frequency = [
            item
            for item in list(enriched.get("frequency", []))
            if isinstance(item, str) and item
        ]
        report.file_type = [
            item
            for item in list(enriched.get("file_type", []))
            if isinstance(item, str) and item
        ]
        report.content_type = str(enriched.get("content_type", report.content_type))
        report.file_name_format = str(enriched.get("file_name_format", "") or "")
        report.parse_status = str(enriched.get("parse_status", nyiso_report.parse_state.PARTIAL))
        report.latest_report_stamp = enriched.get("latest_report_stamp")
        report.earliest_report_stamp = enriched.get("earliest_report_stamp")
        report.last_scanned_at = enriched.get("last_scanned_at")
        report.task_status = nyiso_report.task_state.COMPLETED
        report.active_task_id = ""
        report.task_updated_at = timezone.now()
        report.save(
            latest_stamp=latest_stamp,
            update_fields=[
                "frequency",
                "file_type",
                "content_type",
                "file_name_format",
                "parse_status",
                "latest_report_stamp",
                "earliest_report_stamp",
                "last_scanned_at",
                "task_status",
                "active_task_id",
                "task_updated_at",
                "is_deprecated",
                "updated_at",
            ]
        )
        return f"ok: {report.code}"
    except Exception as exc:
        report.parse_status = nyiso_report.parse_state.FAILED
        report.last_scanned_at = timezone.now()
        report.task_status = nyiso_report.task_state.FAILED
        report.active_task_id = ""
        report.task_updated_at = timezone.now()
        report.save(
            update_fields=[
                "parse_status",
                "last_scanned_at",
                "task_status",
                "active_task_id",
                "task_updated_at",
                "updated_at",
            ]
        )
        raise self.retry(exc=exc)


@shared_task(bind=True, max_retries=3, default_retry_delay=60)
def refresh_nyiso_reports(self, force_reinsert: bool = False) -> str:
    """Fetch and upsert NYISO report metadata in the background.

    Args:
        force_reinsert: When True, deletes all existing rows before re-populating.

    Returns:
        A short status string with the final report count.
    """
    try:
        from .nyiso_api.nyiso_handler import nyiso_handler
        from .nyiso_api.nyiso_models import nyiso_report

        handler = nyiso_handler(force_reinsert=force_reinsert, queue_tasks=True)
        count = nyiso_report.objects.count()
        return f"ok: {count} reports, queued={len(handler.queued_report_ids)}"
    except Exception as exc:
        raise self.retry(exc=exc)
