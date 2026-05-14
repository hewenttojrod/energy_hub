"""Schedule-centric NYISO endpoints."""
from pathlib import Path

from django.core.exceptions import ValidationError
from django.utils import timezone
from ninja import Router
from ninja.errors import HttpError

from .nyiso_api_schema import (
    NyisoScheduleActionResponseSchema,
    NyisoScheduleCreateRequestSchema,
    NyisoScheduleRunSchema,
    NyisoScheduleSchema,
    NyisoScheduleTestResponseSchema,
)
from .nyiso_models import nyiso_report, nyiso_schedule_definition
from .nyiso_page_handler import NyisoPageHandler
from .nyiso_page_parser import (
    build_download_href_index,
    build_file_name_candidates,
    resolve_download_href_from_index,
)
from .nyiso_schedule_service import (
    build_report_lookup_for_schedules,
    parse_optional_date,
    serialize_schedule,
)
from ..schedule_utils import get_schedule_force_redownload_rules, resolve_schedule_dates, should_force_redownload

schedule_router = Router()


@schedule_router.get("report_schedule/list/", response=list[NyisoScheduleSchema])
def list_report_schedules(request):
    schedules = list(
        nyiso_schedule_definition.objects.filter(module_name="energy_hub").order_by(
            "name", "schedule_definition_id"
        )
    )
    report_lookup = build_report_lookup_for_schedules(schedules)
    return [serialize_schedule(schedule, report_lookup=report_lookup) for schedule in schedules]


@schedule_router.post("report_schedule/create/", response=NyisoScheduleSchema)
def create_report_schedule(request, payload: NyisoScheduleCreateRequestSchema):
    try:
        start_date_value = parse_optional_date(payload.start_date)
        end_date_value = parse_optional_date(payload.end_date)
    except ValueError as exc:
        raise HttpError(422, f"Invalid date format. Use YYYY-MM-DD. {exc}")

    report_id = int(payload.report_id) if payload.report_id is not None else None

    schedule = nyiso_schedule_definition(
        module_name="energy_hub",
        name=str(payload.name).strip(),
        mode=str(payload.mode).strip() or "METADATA_REFRESH",
        target_ref_id=report_id,
        is_active=bool(payload.is_active),
        interval_minutes=max(1, int(payload.interval_minutes)),
        use_cache=bool(payload.use_cache),
        run_async=bool(payload.run_async),
        start_date=start_date_value,
        end_date=end_date_value,
        rolling_window_days=payload.rolling_window_days,
        module_config_json=dict(payload.template_values_json or {}),
        next_run_at=timezone.now(),
    )

    try:
        schedule.full_clean()
    except ValidationError as exc:
        error_payload = exc.message_dict if hasattr(exc, "message_dict") else {"detail": exc.messages}
        raise HttpError(422, error_payload)

    schedule.save()
    return serialize_schedule(schedule)


@schedule_router.post("report_schedule/test/", response=NyisoScheduleTestResponseSchema)
def test_report_schedule(request, payload: NyisoScheduleCreateRequestSchema):
    try:
        start_date_value = parse_optional_date(payload.start_date)
        end_date_value = parse_optional_date(payload.end_date)
    except ValueError as exc:
        raise HttpError(422, f"Invalid date format. Use YYYY-MM-DD. {exc}")

    report_id = int(payload.report_id) if payload.report_id is not None else None
    use_cache = bool(payload.use_cache)
    run_async = bool(payload.run_async)

    schedule = nyiso_schedule_definition(
        module_name="energy_hub",
        name=str(payload.name).strip(),
        mode=str(payload.mode).strip() or "METADATA_REFRESH",
        target_ref_id=report_id,
        is_active=bool(payload.is_active),
        interval_minutes=max(1, int(payload.interval_minutes)),
        use_cache=use_cache,
        run_async=run_async,
        start_date=start_date_value,
        end_date=end_date_value,
        rolling_window_days=payload.rolling_window_days,
        module_config_json=dict(payload.template_values_json or {}),
        next_run_at=timezone.now(),
    )

    try:
        schedule.full_clean()
    except ValidationError as exc:
        error_payload = exc.message_dict if hasattr(exc, "message_dict") else {"detail": exc.messages}
        raise HttpError(422, error_payload)

    calls: list[dict[str, object]] = []

    if schedule.mode == nyiso_schedule_definition.schedule_mode.METADATA_REFRESH:
        reports_qs = nyiso_report.objects.all().order_by("nyiso_report_id")
        if schedule.target_ref_id is not None:
            reports_qs = reports_qs.filter(nyiso_report_id=schedule.target_ref_id)
        reports = list(reports_qs)

        for report in reports:
            calls.append(
                {
                    "task_name": "refresh_nyiso_report_details",
                    "report_id": int(report.nyiso_report_id),
                    "report_code": report.code,
                    "run_date": None,
                    "force_refresh": not use_cache,
                    "run_async": run_async,
                    "note": "Would queue .delay()" if run_async else "Would run inline .apply()",
                }
            )

        return {
            "valid": True,
            "message": "Schedule is valid. No NYISO calls were made.",
            "resolved_mode": schedule.mode,
            "report_count": len(reports),
            "estimated_call_count": len(calls),
            "calls": calls,
        }

    report = nyiso_report.objects.get(nyiso_report_id=int(schedule.target_ref_id))
    page_handler = NyisoPageHandler()
    page_ref = (report.source_page or "").strip() or f"{report.code}list"
    cache_max_age = report.get_page_cache_max_age() if use_cache else None
    fetched_page = False
    page_note = ""
    href_index: dict[str, str] = {}

    try:
        detail_html = page_handler.get_page_content_html(page_ref, cache_max_age=cache_max_age)
        fetched_page = True
        href_index = build_download_href_index(detail_html)
    except Exception as exc:
        detail_html = ""
        page_note = f"Could not fetch report page: {exc}"

    dates = resolve_schedule_dates(schedule, report=report)
    base_template_values = dict(schedule.module_config_json or {})
    force_all, forced_dates, forced_patterns, auto_recent_days = get_schedule_force_redownload_rules(schedule)
    scanned_count = len(dates)

    for run_date in dates:
        template_values = dict(base_template_values)
        template_values.setdefault("report", report.code)
        template_values["date"] = run_date.isoformat()
        candidates = build_file_name_candidates(report.file_name_format, template_values)

        matched_href = None
        resolved_url = None
        found_on_page = None

        if fetched_page and detail_html:
            matched_href = resolve_download_href_from_index(href_index, candidates)
            found_on_page = matched_href is not None
            if matched_href:
                if matched_href.lower().startswith("http"):
                    resolved_url = matched_href
                else:
                    resolved_url = page_handler.index_url.rstrip("/") + "/" + matched_href.lstrip("/")

        if found_on_page:
            matched_name = Path((matched_href or "").split("?", maxsplit=1)[0]).name
            already_downloaded = report.report_files.filter(
                source_file__source_file_name__iexact=matched_name
            ).exists()
            force_redownload = should_force_redownload(
                run_date=run_date,
                matched_href=matched_href or "",
                force_all=force_all,
                forced_dates=forced_dates,
                forced_patterns=forced_patterns,
                auto_recent_days=auto_recent_days,
            )

            if use_cache and already_downloaded and not force_redownload:
                note = "File found on report page. Already downloaded; would skip due to cache."
            elif force_redownload:
                note = "File found on report page. Force redownload rule matched (recent-day/checksum revalidation enabled)."
            else:
                note = "File found on report page."

            calls.append(
                {
                    "task_name": "download_public_file",
                    "report_id": int(report.nyiso_report_id),
                    "report_code": report.code,
                    "run_date": run_date.isoformat(),
                    "force_refresh": not use_cache,
                    "run_async": run_async,
                    "matched_href": matched_href,
                    "resolved_url": resolved_url,
                    "found_on_page": True,
                    "note": note,
                }
            )

    found_count = len(calls)
    summary = f"Fetched report page listing. Found {found_count} matching file(s) across {scanned_count} scanned date(s)."
    if not fetched_page:
        summary = page_note or "Could not fetch report page."

    return {
        "valid": True,
        "message": summary,
        "resolved_mode": schedule.mode,
        "report_count": 1,
        "estimated_call_count": len(calls),
        "calls": calls,
    }


@schedule_router.post("report_schedule/toggle/", response=NyisoScheduleActionResponseSchema)
def toggle_report_schedule(request, schedule_id: int, is_active: bool):
    schedule = nyiso_schedule_definition.objects.get(schedule_definition_id=schedule_id, module_name="energy_hub")
    schedule.is_active = bool(is_active)
    if schedule.is_active:
        if schedule.next_run_at is None:
            schedule.next_run_at = timezone.now()
        schedule.last_message = "Schedule resumed."
    else:
        schedule.next_run_at = None
        schedule.last_message = "Schedule paused."
    schedule.save(update_fields=["is_active", "next_run_at", "last_message", "updated_at"])
    return {
        "schedule_id": schedule_id,
        "ok": True,
        "message": "Schedule resumed." if schedule.is_active else "Schedule paused.",
        "task_id": None,
    }


@schedule_router.post("report_schedule/run/", response=NyisoScheduleActionResponseSchema)
def run_report_schedule_now(
    request,
    schedule_id: int,
    async_mode: bool | None = None,
    use_cache: bool | None = None,
):
    from ..tasks import execute_nyiso_report_schedule

    schedule = nyiso_schedule_definition.objects.get(schedule_definition_id=schedule_id, module_name="energy_hub")

    resolved_async = schedule.run_async if async_mode is None else bool(async_mode)
    if resolved_async:
        task = execute_nyiso_report_schedule.delay(
            int(schedule.schedule_definition_id),
            triggered_by="manual",
            run_async_override=async_mode,
            use_cache_override=use_cache,
        )
        schedule.last_state = nyiso_schedule_definition.run_state.QUEUED
        schedule.last_message = "Manual run queued"
        schedule.save(update_fields=["last_state", "last_message", "updated_at"])
        return {
            "schedule_id": schedule_id,
            "ok": True,
            "message": "Schedule run queued.",
            "task_id": task.id,
        }

    result = execute_nyiso_report_schedule.apply(
        args=[int(schedule.schedule_definition_id)],
        kwargs={
            "triggered_by": "manual",
            "run_async_override": False,
            "use_cache_override": use_cache,
        },
    )
    return {
        "schedule_id": schedule_id,
        "ok": not result.failed(),
        "message": "Schedule run completed." if not result.failed() else "Schedule run failed.",
        "task_id": None,
    }


@schedule_router.patch("report_schedule/update/", response=NyisoScheduleSchema)
def update_report_schedule(request, schedule_id: int, payload: NyisoScheduleCreateRequestSchema):
    from django.core.exceptions import ValidationError as DjangoValidationError

    schedule = nyiso_schedule_definition.objects.get(schedule_definition_id=schedule_id, module_name="energy_hub")

    try:
        start_date_value = parse_optional_date(payload.start_date)
        end_date_value = parse_optional_date(payload.end_date)
    except ValueError as exc:
        raise HttpError(422, f"Invalid date format. Use YYYY-MM-DD. {exc}")

    schedule.name = str(payload.name).strip()
    schedule.mode = str(payload.mode).strip() or "METADATA_REFRESH"
    schedule.target_ref_id = int(payload.report_id) if payload.report_id is not None else None
    schedule.is_active = bool(payload.is_active)
    schedule.interval_minutes = max(1, int(payload.interval_minutes))
    schedule.use_cache = bool(payload.use_cache)
    schedule.run_async = bool(payload.run_async)
    schedule.start_date = start_date_value
    schedule.end_date = end_date_value
    schedule.rolling_window_days = payload.rolling_window_days
    schedule.module_config_json = dict(payload.template_values_json or {})

    try:
        schedule.full_clean()
    except DjangoValidationError as exc:
        error_payload = exc.message_dict if hasattr(exc, "message_dict") else {"detail": exc.messages}
        raise HttpError(422, error_payload)

    schedule.save()
    return serialize_schedule(schedule)


@schedule_router.delete("report_schedule/delete/", response=NyisoScheduleActionResponseSchema)
def delete_report_schedule(request, schedule_id: int):
    schedule = nyiso_schedule_definition.objects.get(schedule_definition_id=schedule_id, module_name="energy_hub")
    schedule.delete()
    return {"schedule_id": schedule_id, "ok": True, "message": "Schedule deleted.", "task_id": None}


@schedule_router.get("report_schedule/runs/", response=list[NyisoScheduleRunSchema])
def list_schedule_runs(request, schedule_id: int):
    from core.scheduling.models import schedule_run

    runs = schedule_run.objects.filter(schedule_definition_id=schedule_id).order_by("-created_at")[:50]
    return [
        {
            "schedule_run_id": run.schedule_run_id,
            "state_value": run.state_value,
            "triggered_by": run.triggered_by,
            "celery_task_id": run.celery_task_id,
            "started_at": run.started_at.isoformat() if run.started_at else None,
            "finished_at": run.finished_at.isoformat() if run.finished_at else None,
            "records_targeted": run.records_targeted,
            "files_downloaded": run.files_downloaded,
            "completed_count": run.completed_count,
            "failed_count": run.failed_count,
            "message": run.message,
            "created_at": run.created_at.isoformat() if run.created_at else None,
        }
        for run in runs
    ]


@schedule_router.post("report_schedule/process_due/", response=NyisoScheduleActionResponseSchema)
def process_due_report_schedules(request, async_mode: bool = True):
    from ..tasks import process_nyiso_report_schedules

    if async_mode:
        task = process_nyiso_report_schedules.delay()
        return {
            "schedule_id": 0,
            "ok": True,
            "message": "Due schedule processing queued.",
            "task_id": task.id,
        }

    result = process_nyiso_report_schedules.apply(args=[])
    return {
        "schedule_id": 0,
        "ok": not result.failed(),
        "message": "Due schedule processing completed." if not result.failed() else "Due schedule processing failed.",
        "task_id": None,
    }
