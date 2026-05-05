from datetime import date, datetime, timezone as dt_timezone
from pathlib import Path

from django.core.exceptions import ValidationError
from django.utils import timezone
from ninja import NinjaAPI
from ninja.errors import HttpError

from .nyiso_api_schema import (
    NyisoReportFileResolveRequestSchema,
    NyisoReportFileResolveResponseSchema,
    NyisoReportRefreshActionSchema,
    NyisoRefreshAllReportsSchema,
    NyisoScheduleActionResponseSchema,
    NyisoScheduleCreateRequestSchema,
    NyisoScheduleRunSchema,
    NyisoScheduleSchema,
    NyisoScheduleTestResponseSchema,
    NyisoReportSchema,
    NyisoTaskPollSchema,
    NyisoTaskStartSchema,
)
from .nyiso_handler import nyiso_handler
from .nyiso_models import nyiso_report, nyiso_schedule_definition
from .nyiso_page_handler import NyisoPageHandler
from .nyiso_page_parser import (
    build_download_href_index,
    build_file_name_candidates,
    build_report_detail_grid_payload,
    resolve_download_href_from_index,
)
from .nyiso_tasks import get_active_task_id_for_report, get_active_tasks

nyiso_app = NinjaAPI(urls_namespace="energy_hub_api", docs_url="/docs")


@nyiso_app.get("report_list/", response=list[NyisoReportSchema])
def get_report_list(request, force_reinsert: bool = False):
    """Return stored NYISO reports ordered by code/name."""
    if force_reinsert:
        nyiso_handler(force_reinsert=True, queue_tasks=False)
    return nyiso_report.objects.all().order_by("code", "name")


@nyiso_app.get("report_rows/", response=list[NyisoReportSchema])
def get_report_rows(request, ids: str):
    """Return only selected report rows by nyiso_report_id for partial client-side grid patching."""
    parsed_ids: list[int] = []
    for token in ids.split(","):
        token = token.strip()
        if not token:
            continue
        try:
            parsed_ids.append(int(token))
        except ValueError:
            continue

    if not parsed_ids:
        return []

    return list(nyiso_report.objects.filter(nyiso_report_id__in=parsed_ids).order_by("code", "name"))


@nyiso_app.get("report_row/", response=NyisoReportSchema)
def get_report_row(request, report_id: int):
    """Return one nyiso_report row by id."""
    return nyiso_report.objects.get(nyiso_report_id=report_id)


@nyiso_app.get("report_row/content/", response=dict)
def get_report_row_content(request, report_id: int):
    """Return parsed detail-grid content for one nyiso_report row."""
    report = nyiso_report.objects.get(nyiso_report_id=report_id)
    page_handler = NyisoPageHandler()
    page_ref = (report.source_page or "").strip() or f"{report.code}list"
    detail_html = page_handler.get_page_content_html(
        page_ref,
        cache_max_age=report.get_page_cache_max_age(),
    )

    base_record: dict[str, object] = {
        "code": report.code,
        "name": report.name,
        "content_type": report.content_type,
        "source_page": report.source_page,
    }
    payload = build_report_detail_grid_payload(base_record, detail_html, nyiso_report)
    payload["report_id"] = report.nyiso_report_id
    return payload


@nyiso_app.post("report_row/refresh/", response=NyisoReportRefreshActionSchema)
def refresh_report_row(request, report_id: int):
    """Queue a forced refresh task for one report when no active task is already bound to it."""
    report = nyiso_report.objects.get(nyiso_report_id=report_id)

    active_task_id = get_active_task_id_for_report(report_id)
    if active_task_id:
        return {
            "report_id": report_id,
            "task_id": active_task_id,
            "queued": False,
            "message": "Report already has an active task.",
        }

    from ..tasks import refresh_nyiso_report_details

    async_result = refresh_nyiso_report_details.delay(report_id, force_refresh=True)
    report.task_status = nyiso_report.task_state.QUEUED
    report.active_task_id = async_result.id
    report.task_updated_at = timezone.now()
    report.save(update_fields=["task_status", "active_task_id", "task_updated_at", "updated_at"])

    return {
        "report_id": report_id,
        "task_id": async_result.id,
        "queued": True,
        "message": "Refresh task queued.",
    }


@nyiso_app.post("report_row/refresh/all/", response=NyisoRefreshAllReportsSchema)
def refresh_all_reports(request, use_cache: bool = True, async_mode: bool = True):
    """Refresh all reports either async (queue and return) or sync (wait until done)."""
    from ..tasks import refresh_nyiso_report_details

    all_reports = nyiso_report.objects.all()
    queued_report_ids: list[int] = []
    already_active_count = 0
    completed_count = 0
    failed_count = 0

    # Get set of already-active report IDs to avoid duplicate queueing
    active_tasks = get_active_tasks()
    active_set = set(active_tasks)

    for report in all_reports:
        if report.nyiso_report_id in active_set:
            already_active_count += 1
            continue

        if async_mode:
            # Queue the refresh task.
            # When use_cache=False, force_refresh=True bypasses cache (timedelta(seconds=0)).
            # When use_cache=True, force_refresh=False respects cache_max_age.
            async_result = refresh_nyiso_report_details.delay(
                report.nyiso_report_id,
                force_refresh=not use_cache,
            )
            report.task_status = nyiso_report.task_state.QUEUED
            report.active_task_id = async_result.id
            report.task_updated_at = timezone.now()
            report.save(update_fields=["task_status", "active_task_id", "task_updated_at", "updated_at"])
            queued_report_ids.append(report.nyiso_report_id)
            continue

        # Synchronous mode: run the same task logic inline and return only when complete.
        try:
            result = refresh_nyiso_report_details.apply(
                args=[report.nyiso_report_id],
                kwargs={"force_refresh": not use_cache},
            )
            if result.failed():
                failed_count += 1
            else:
                completed_count += 1
        except Exception:
            failed_count += 1

    if async_mode:
        message = (
            f"Queued {len(queued_report_ids)} reports for refresh "
            f"({already_active_count} already active)."
        )
    else:
        message = (
            f"Completed refresh for {completed_count} reports with {failed_count} failures "
            f"({already_active_count} already active)."
        )

    return {
        "total_reports": all_reports.count(),
        "queued_count": len(queued_report_ids) if async_mode else 0,
        "already_active_count": already_active_count,
        "queued_report_ids": queued_report_ids,
        "cache_bypassed": not use_cache,
        "async_mode": async_mode,
        "completed_count": None if async_mode else completed_count,
        "failed_count": None if async_mode else failed_count,
        "message": message,
    }


def _parse_optional_date(raw_value: str | None) -> date | None:
    """Parse YYYY-MM-DD to date, returning None for empty input."""
    value = (raw_value or "").strip()
    if not value:
        return None
    return date.fromisoformat(value)


def _serialize_schedule(schedule: nyiso_schedule_definition) -> dict[str, object]:
    report = None
    if schedule.target_ref_id:
        report = nyiso_report.objects.filter(nyiso_report_id=schedule.target_ref_id).first()
    return {
        "nyiso_report_schedule_id": schedule.schedule_definition_id,
        "name": schedule.name,
        "mode": schedule.mode,
        "report_id": report.nyiso_report_id if report else None,
        "report_code": report.code if report else None,
        "report_name": report.name if report else None,
        "is_active": schedule.is_active,
        "interval_minutes": schedule.interval_minutes,
        "use_cache": schedule.use_cache,
        "run_async": schedule.run_async,
        "start_date": schedule.start_date.isoformat() if schedule.start_date else None,
        "end_date": schedule.end_date.isoformat() if schedule.end_date else None,
        "rolling_window_days": schedule.rolling_window_days,
        "template_values_json": dict(schedule.module_config_json or {}),
        "next_run_at": schedule.next_run_at.isoformat() if schedule.next_run_at else None,
        "last_run_at": schedule.last_run_at.isoformat() if schedule.last_run_at else None,
        "last_state": schedule.last_state,
        "last_message": schedule.last_message,
    }


@nyiso_app.get("report_schedule/list/", response=list[NyisoScheduleSchema])
def list_report_schedules(request):
    schedules = nyiso_schedule_definition.objects.filter(module_name="energy_hub").order_by(
        "name", "schedule_definition_id"
    )
    return [_serialize_schedule(schedule) for schedule in schedules]


@nyiso_app.post("report_schedule/create/", response=NyisoScheduleSchema)
def create_report_schedule(request, payload: NyisoScheduleCreateRequestSchema):
    try:
        start_date_value = _parse_optional_date(payload.start_date)
        end_date_value = _parse_optional_date(payload.end_date)
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
    return _serialize_schedule(schedule)


@nyiso_app.post("report_schedule/test/", response=NyisoScheduleTestResponseSchema)
def test_report_schedule(request, payload: NyisoScheduleCreateRequestSchema):
    try:
        start_date_value = _parse_optional_date(payload.start_date)
        end_date_value = _parse_optional_date(payload.end_date)
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
    from ..tasks import (
        _get_schedule_force_redownload_rules,
        _resolve_schedule_dates,
        _should_force_redownload,
    )

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

    dates = _resolve_schedule_dates(schedule, report=report)
    base_template_values = dict(schedule.module_config_json or {})
    force_all, forced_dates, forced_patterns, auto_recent_days = _get_schedule_force_redownload_rules(schedule)
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
            force_redownload = _should_force_redownload(
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


@nyiso_app.post("report_schedule/toggle/", response=NyisoScheduleActionResponseSchema)
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


@nyiso_app.post("report_schedule/run/", response=NyisoScheduleActionResponseSchema)
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


@nyiso_app.patch("report_schedule/update/", response=NyisoScheduleSchema)
def update_report_schedule(request, schedule_id: int, payload: NyisoScheduleCreateRequestSchema):
    from django.core.exceptions import ValidationError

    schedule = nyiso_schedule_definition.objects.get(schedule_definition_id=schedule_id, module_name="energy_hub")

    try:
        start_date_value = _parse_optional_date(payload.start_date)
        end_date_value = _parse_optional_date(payload.end_date)
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
    except ValidationError as exc:
        error_payload = exc.message_dict if hasattr(exc, "message_dict") else {"detail": exc.messages}
        raise HttpError(422, error_payload)

    schedule.save()
    return _serialize_schedule(schedule)


@nyiso_app.delete("report_schedule/delete/", response=NyisoScheduleActionResponseSchema)
def delete_report_schedule(request, schedule_id: int):
    schedule = nyiso_schedule_definition.objects.get(schedule_definition_id=schedule_id, module_name="energy_hub")
    schedule.delete()
    return {"schedule_id": schedule_id, "ok": True, "message": "Schedule deleted.", "task_id": None}


@nyiso_app.get("report_schedule/runs/", response=list[NyisoScheduleRunSchema])
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


@nyiso_app.post("report_schedule/process_due/", response=NyisoScheduleActionResponseSchema)
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


@nyiso_app.post("report_row/file/resolve/", response=NyisoReportFileResolveResponseSchema)
def resolve_report_row_file(request, payload: NyisoReportFileResolveRequestSchema):
    """Build candidate filenames from report template + input values. When download=False, returns only
    built filename candidates without hitting the report page. When download=True, verifies the file
    exists on the report page and downloads it."""
    report = nyiso_report.objects.get(nyiso_report_id=payload.report_id)

    template_values = dict(payload.template_values or {})
    template_values.setdefault("report", report.code)
    candidate_file_names = build_file_name_candidates(report.file_name_format, template_values)
    built_file_name = candidate_file_names[0] if candidate_file_names else None

    if not payload.download:
        return {
            "report_id": report.nyiso_report_id,
            "built_file_name": built_file_name,
            "exists_on_report_page": False,
            "requested_file_name_candidates": candidate_file_names,
            "matched_href": None,
            "resolved_url": None,
            "downloaded": False,
            "local_path": None,
            "source_file_id": None,
            "checksum": None,
            "file_type": None,
            "size_bytes": None,
            "created": False,
        }

    page_handler = NyisoPageHandler()

    page_ref = (report.source_page or "").strip() or f"{report.code}list"
    detail_html = page_handler.get_page_content_html(
        page_ref,
        cache_max_age=report.get_page_cache_max_age(),
    )
    matched_href = page_handler.find_download_href(detail_html, candidate_file_names)
    exists_on_report_page = bool(matched_href)

    response: dict[str, object] = {
        "report_id": report.nyiso_report_id,
        "built_file_name": built_file_name,
        "exists_on_report_page": exists_on_report_page,
        "requested_file_name_candidates": candidate_file_names,
        "matched_href": matched_href,
        "resolved_url": None,
        "downloaded": False,
        "local_path": None,
        "source_file_id": None,
        "checksum": None,
        "file_type": None,
        "size_bytes": None,
        "created": False,
    }

    if not exists_on_report_page:
        return response

    if matched_href:
        if matched_href.lower().startswith("http://") or matched_href.lower().startswith("https://"):
            response["resolved_url"] = matched_href
        else:
            response["resolved_url"] = page_handler.index_url.rstrip("/") + "/" + matched_href.lstrip("/")

    if payload.download and matched_href:
        download_result = page_handler.download_public_file(matched_href, report_id=report.nyiso_report_id)
        response["downloaded"] = True
        response["local_path"] = download_result.get("local_path")
        response["resolved_url"] = download_result.get("url")
        response["source_file_id"] = download_result.get("source_file_id")
        response["checksum"] = download_result.get("checksum")
        response["file_type"] = download_result.get("file_type")
        response["size_bytes"] = download_result.get("size_bytes")
        response["created"] = download_result.get("created", False)

    return response


@nyiso_app.get("report_refresh/start/", response=NyisoTaskStartSchema)
def start_report_refresh_tasks(request, force_reinsert: bool = False):
    """Sync menu rows and queue per-report detail refresh tasks in the background."""
    handler = nyiso_handler(force_reinsert=force_reinsert, queue_tasks=True)
    active_from_db = get_active_tasks()
    return {
        "queued_report_ids": handler.queued_report_ids,
        "active_report_ids": active_from_db,
        "queued_count": len(handler.queued_report_ids),
        "active_count": len(active_from_db),
        "cursor": timezone.now().isoformat(),
    }


@nyiso_app.get("report_refresh/status/", response=NyisoTaskPollSchema)
def poll_report_refresh_status(request, since: str | None = None):
    """Return active reports and newly finished reports since the provided cursor timestamp."""
    active_report_ids = get_active_tasks()

    finished_queryset = nyiso_report.objects.filter(
        task_status__in=[nyiso_report.task_state.COMPLETED, nyiso_report.task_state.FAILED]
    )

    if since:
        try:
            since_dt = timezone.datetime.fromisoformat(since)
            if timezone.is_naive(since_dt):
                since_dt = timezone.make_aware(since_dt, timezone=dt_timezone.utc)
            finished_queryset = finished_queryset.filter(task_updated_at__gt=since_dt)
        except ValueError:
            pass

    finished_report_ids = list(finished_queryset.values_list("nyiso_report_id", flat=True))
    return {
        "active_report_ids": active_report_ids,
        "finished_report_ids": finished_report_ids,
        "active_count": len(active_report_ids),
        "finished_count": len(finished_report_ids),
        "cursor": timezone.now().isoformat(),
    }


@nyiso_app.get("/nyiso/page/", response=str)
def get_page(request, page: str = "menu.htm"):
    """Return raw HTML for a specific NYISO page for testing/debugging.
    
    Query parameters:
    - page (str): Page path relative to NYISO public root (e.g., 'menu.htm', 'A01list.htm')
    
    Example: /api/energy_hub/nyiso/page/?page=menu.htm
    """
    page_handler = NyisoPageHandler()
    return page_handler.get_page_html(page)
