from datetime import date, timedelta
from pathlib import Path

from celery import shared_task
from django.db import models
from django.db import transaction
from django.utils import timezone

from .ingestion_tasks import ingest_nyiso_source_file
from .schedule_utils import (
    get_schedule_force_redownload_rules,
    resolve_schedule_dates,
    should_force_redownload,
)
from .task_refresh import refresh_nyiso_report_details


def _resolve_schedule_dates(schedule, report=None) -> list[date]:
    return resolve_schedule_dates(schedule, report=report)


def _get_schedule_force_redownload_rules(schedule) -> tuple[bool, set[str], list[str], int]:
    return get_schedule_force_redownload_rules(schedule)


def _should_force_redownload(
    *,
    run_date: date,
    matched_href: str,
    force_all: bool,
    forced_dates: set[str],
    forced_patterns: list[str],
    auto_recent_days: int = 0,
) -> bool:
    return should_force_redownload(
        run_date=run_date,
        matched_href=matched_href,
        force_all=force_all,
        forced_dates=forced_dates,
        forced_patterns=forced_patterns,
        auto_recent_days=auto_recent_days,
    )


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
    from .nyiso_api.nyiso_tasks import get_active_tasks

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
            active_report_ids = set(get_active_tasks()) if run_async else set()

            for report in reports:
                if run_async:
                    if int(report.nyiso_report_id) in active_report_ids:
                        continue

                    async_result = refresh_nyiso_report_details.delay(
                        int(report.nyiso_report_id),
                        force_refresh=not use_cache,
                    )
                    report.task_status = nyiso_report.task_state.QUEUED
                    report.active_task_id = async_result.id
                    report.task_updated_at = timezone.now()
                    report.save(update_fields=["task_status", "active_task_id", "task_updated_at", "updated_at"])
                    active_report_ids.add(int(report.nyiso_report_id))
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
