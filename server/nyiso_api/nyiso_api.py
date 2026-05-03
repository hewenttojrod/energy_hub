from datetime import timezone as dt_timezone

from django.utils import timezone
from ninja import NinjaAPI

from .nyiso_api_schema import (
    NyisoReportFileResolveRequestSchema,
    NyisoReportFileResolveResponseSchema,
    NyisoReportRefreshActionSchema,
    NyisoReportSchema,
    NyisoTaskPollSchema,
    NyisoTaskStartSchema,
)
from .nyiso_handler import nyiso_handler
from .nyiso_models import nyiso_report
from .nyiso_page_handler import NyisoPageHandler
from .nyiso_page_parser import build_file_name_candidates, build_report_detail_grid_payload
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
    }

    if not exists_on_report_page:
        return response

    if matched_href:
        if matched_href.lower().startswith("http://") or matched_href.lower().startswith("https://"):
            response["resolved_url"] = matched_href
        else:
            response["resolved_url"] = page_handler.index_url.rstrip("/") + "/" + matched_href.lstrip("/")

    if payload.download and matched_href:
        download_result = page_handler.download_public_file(matched_href)
        response["downloaded"] = True
        response["local_path"] = download_result.get("local_path")
        response["resolved_url"] = download_result.get("url")

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
