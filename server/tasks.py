import fnmatch
import re
from pathlib import Path

from celery import shared_task
from datetime import date, timedelta
from django.db import models
from django.db import transaction
from django.utils import timezone

# Register ingestor tasks so Celery autodiscovery picks them up via this tasks.py
from .nyiso_ingestor import ingest_nyiso_source_file, normalize_nyiso_source_file  # noqa: F401


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


def _resolve_schedule_dates(schedule, report=None) -> list[date]:
    """Return run dates for FILE_DOWNLOAD_RANGE schedule mode.

    Open-ended behavior:
    - blank end_date => today
    - blank start_date => report earliest date (when available), else end_date
    """
    today = timezone.now().date()
    if schedule.rolling_window_days and schedule.rolling_window_days > 0:
        start = today - timedelta(days=int(schedule.rolling_window_days) - 1)
        end = today
    else:
        end = schedule.end_date or today

        earliest_date = None
        if report is not None and getattr(report, "earliest_report_stamp", None) is not None:
            earliest_stamp = report.earliest_report_stamp
            earliest_date = earliest_stamp.date() if hasattr(earliest_stamp, "date") else earliest_stamp

        start = schedule.start_date or earliest_date or end

    if end < start:
        return []

    days = (end - start).days + 1
    return [start + timedelta(days=offset) for offset in range(days)]


def _coerce_bool(raw_value: object) -> bool:
    if isinstance(raw_value, bool):
        return raw_value
    if raw_value is None:
        return False
    return str(raw_value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _coerce_int(raw_value: object, default: int) -> int:
    if raw_value is None:
        return default
    try:
        return int(str(raw_value).strip())
    except (TypeError, ValueError):
        return default


def _parse_force_tokens(raw_value: object) -> list[str]:
    if raw_value is None:
        return []
    if isinstance(raw_value, (list, tuple, set)):
        values = [str(item).strip() for item in raw_value]
        return [item for item in values if item]

    text = str(raw_value).strip()
    if not text:
        return []
    return [token.strip() for token in re.split(r"[,;|\n]+", text) if token.strip()]


def _get_schedule_force_redownload_rules(schedule) -> tuple[bool, set[str], list[str], int]:
    config = dict(schedule.module_config_json or {})
    force_all = _coerce_bool(config.get("force_redownload_all"))
    forced_dates = set(_parse_force_tokens(config.get("force_redownload_dates")))
    forced_patterns = _parse_force_tokens(
        config.get("force_redownload_patterns") or config.get("force_redownload_files")
    )
    # By default, always revalidate the most recent day so late-arriving/corrected files are picked up.
    auto_recent_days = max(0, _coerce_int(config.get("force_redownload_recent_days"), default=1))
    return force_all, forced_dates, forced_patterns, auto_recent_days


def _should_force_redownload(
    *,
    run_date: date,
    matched_href: str,
    force_all: bool,
    forced_dates: set[str],
    forced_patterns: list[str],
    auto_recent_days: int = 0,
) -> bool:
    if force_all:
        return True

    if auto_recent_days > 0:
        today = timezone.now().date()
        recent_cutoff = today - timedelta(days=auto_recent_days)
        if run_date >= recent_cutoff:
            return True

    run_date_key = run_date.isoformat()
    if run_date_key in forced_dates:
        return True

    file_name = Path((matched_href or "").split("?", maxsplit=1)[0]).name
    file_name_lower = file_name.lower()
    href_lower = (matched_href or "").lower()
    for pattern in forced_patterns:
        candidate = pattern.strip().lower()
        if not candidate:
            continue
        if fnmatch.fnmatch(file_name_lower, candidate) or fnmatch.fnmatch(href_lower, candidate):
            return True

    return False


@shared_task(bind=True, max_retries=1, default_retry_delay=15)
def execute_nyiso_report_schedule(
    self,
    schedule_id: int,
    triggered_by: str = "scheduler",
    run_async_override: bool | None = None,
    use_cache_override: bool | None = None,
) -> str:
    """Execute one schedule definition and persist a run log row."""
    from core.models import schedule_definition, schedule_run
    from .nyiso_api.nyiso_models import nyiso_report
    from .nyiso_api.nyiso_page_handler import NyisoPageHandler
    from .nyiso_api.nyiso_page_parser import (
        build_download_href_index,
        build_file_name_candidates,
        resolve_download_href_from_index,
    )
    from .nyiso_api.nyiso_tasks import get_active_task_id_for_report

    schedule = schedule_definition.objects.filter(pk=schedule_id, module_name="energy_hub").first()
    if schedule is None:
        return f"missing schedule: {schedule_id}"

    use_cache = schedule.use_cache if use_cache_override is None else bool(use_cache_override)
    run_async = schedule.run_async if run_async_override is None else bool(run_async_override)

    report = None
    if schedule.target_ref_id:
        report = nyiso_report.objects.filter(nyiso_report_id=schedule.target_ref_id).first()

    run = schedule_run.objects.create(
        schedule_definition=schedule,
        module_name="energy_hub",
        triggered_by=triggered_by,
        state_value=schedule_run.state.RUNNING,
        celery_task_id=str(self.request.id or ""),
        started_at=timezone.now(),
    )

    schedule.last_state = schedule_definition.run_state.RUNNING
    schedule.last_message = "Running"
    schedule.save(update_fields=["last_state", "last_message", "updated_at"])

    completed_count = 0
    failed_count = 0
    files_downloaded = 0
    reports_targeted = 0

    try:
        if schedule.mode == "METADATA_REFRESH":
            reports_qs = nyiso_report.objects.all().order_by("nyiso_report_id")
            if report is not None:
                reports_qs = reports_qs.filter(nyiso_report_id=report.nyiso_report_id)
            reports = list(reports_qs)
            reports_targeted = len(reports)

            for report in reports:
                if run_async:
                    active_task = get_active_task_id_for_report(int(report.nyiso_report_id))
                    if active_task:
                        continue

                    async_result = refresh_nyiso_report_details.delay(
                        int(report.nyiso_report_id),
                        force_refresh=not use_cache,
                    )
                    report.task_status = nyiso_report.task_state.QUEUED
                    report.active_task_id = async_result.id
                    report.task_updated_at = timezone.now()
                    report.save(update_fields=["task_status", "active_task_id", "task_updated_at", "updated_at"])
                    completed_count += 1
                    continue

                result = refresh_nyiso_report_details.apply(
                    args=[int(report.nyiso_report_id)],
                    kwargs={"force_refresh": not use_cache},
                )
                if result.failed():
                    failed_count += 1
                else:
                    completed_count += 1

        elif schedule.mode == "FILE_DOWNLOAD_RANGE":
            if report is None:
                raise ValueError("FILE_DOWNLOAD_RANGE schedules require a report")

            page_handler = NyisoPageHandler()
            page_ref = (report.source_page or "").strip() or f"{report.code}list"
            cache_max_age = report.get_page_cache_max_age() if use_cache else timedelta(seconds=0)
            detail_html = page_handler.get_page_content_html(page_ref, cache_max_age=cache_max_age)
            href_index = build_download_href_index(detail_html)

            dates = _resolve_schedule_dates(schedule, report=report)
            reports_targeted = 1
            base_template_values = dict(schedule.module_config_json or {})
            force_all, forced_dates, forced_patterns, auto_recent_days = _get_schedule_force_redownload_rules(schedule)

            for run_date in dates:
                template_values = dict(base_template_values)
                template_values.setdefault("report", report.code)
                template_values["date"] = run_date.isoformat()

                candidates = build_file_name_candidates(report.file_name_format, template_values)
                href = resolve_download_href_from_index(href_index, candidates)
                if not href:
                    failed_count += 1
                    continue

                force_redownload = _should_force_redownload(
                    run_date=run_date,
                    matched_href=href,
                    force_all=force_all,
                    forced_dates=forced_dates,
                    forced_patterns=forced_patterns,
                    auto_recent_days=auto_recent_days,
                )
                matched_name = Path(href.split("?", maxsplit=1)[0]).name
                already_downloaded = report.report_files.filter(
                    source_file__source_file_name__iexact=matched_name
                ).exists()

                if use_cache and already_downloaded and not force_redownload:
                    completed_count += 1
                    continue

                dl = page_handler.download_public_file(href, report_id=report.nyiso_report_id)
                files_downloaded += 1
                completed_count += 1
                if dl.get("created"):
                    ingest_nyiso_source_file.delay(dl["source_file_id"])

        else:
            raise ValueError(f"Unsupported schedule mode: {schedule.mode}")

        schedule.last_state = schedule_definition.run_state.COMPLETED
        schedule.last_message = "Run completed"
        schedule.last_run_at = timezone.now()
        schedule.next_run_at = timezone.now() + timedelta(minutes=max(1, int(schedule.interval_minutes or 1)))
        schedule.save(
            update_fields=[
                "last_state",
                "last_message",
                "last_run_at",
                "next_run_at",
                "updated_at",
            ]
        )

        run.state_value = schedule_run.state.COMPLETED
        run.finished_at = timezone.now()
        run.records_targeted = reports_targeted
        run.files_downloaded = files_downloaded
        run.completed_count = completed_count
        run.failed_count = failed_count
        run.message = "Run completed"
        run.save(
            update_fields=[
                "state_value",
                "finished_at",
            "records_targeted",
                "files_downloaded",
                "completed_count",
                "failed_count",
                "message",
                "updated_at",
            ]
        )
        return f"ok: schedule={schedule_id} completed={completed_count} failed={failed_count}"
    except Exception as exc:
        schedule.last_state = schedule_definition.run_state.FAILED
        schedule.last_message = str(exc)
        schedule.last_run_at = timezone.now()
        schedule.next_run_at = timezone.now() + timedelta(minutes=max(1, int(schedule.interval_minutes or 1)))
        schedule.save(
            update_fields=[
                "last_state",
                "last_message",
                "last_run_at",
                "next_run_at",
                "updated_at",
            ]
        )

        run.state_value = schedule_run.state.FAILED
        run.finished_at = timezone.now()
        run.records_targeted = reports_targeted
        run.files_downloaded = files_downloaded
        run.completed_count = completed_count
        run.failed_count = failed_count + 1
        run.message = str(exc)
        run.save(
            update_fields=[
                "state_value",
                "finished_at",
            "records_targeted",
                "files_downloaded",
                "completed_count",
                "failed_count",
                "message",
                "updated_at",
            ]
        )
        raise


@shared_task(bind=True, max_retries=0)
def process_nyiso_report_schedules(self) -> str:
    """Queue due schedules based on next_run_at/interval settings."""
    from core.models import schedule_definition

    now = timezone.now()
    queued = 0
    queued_schedule_ids: list[int] = []

    # Lock due rows while marking them queued so overlapping scheduler ticks cannot queue duplicates.
    with transaction.atomic():
        due_schedules = (
            schedule_definition.objects.select_for_update(skip_locked=True)
            .filter(module_name="energy_hub", is_active=True)
            .filter(models.Q(next_run_at__isnull=True) | models.Q(next_run_at__lte=now))
            .order_by("schedule_definition_id")
        )

        for schedule in due_schedules:
            schedule.last_state = schedule_definition.run_state.QUEUED
            schedule.last_message = "Queued by scheduler"
            schedule.next_run_at = now + timedelta(minutes=max(1, int(schedule.interval_minutes or 1)))
            schedule.save(update_fields=["last_state", "last_message", "next_run_at", "updated_at"])
            queued_schedule_ids.append(int(schedule.schedule_definition_id))

    for schedule_id in queued_schedule_ids:
        execute_nyiso_report_schedule.delay(schedule_id, triggered_by="scheduler")
        queued += 1

    return f"ok: queued={queued}"
