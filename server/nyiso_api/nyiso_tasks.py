from django.apps import apps

from .nyiso_models import nyiso_report

INCOMPLETE_TASK_RESULT_STATES = {"PENDING", "RECEIVED", "STARTED", "RETRY"}


def get_active_tasks() -> list[int]:
    """Return report ids whose active_task_id maps to a currently incomplete Celery task."""
    task_result_model = apps.get_model("django_celery_results", "TaskResult")

    candidates = list(
        nyiso_report.objects.filter(
            task_status__in=[nyiso_report.task_state.QUEUED, nyiso_report.task_state.RUNNING]
        )
        .exclude(active_task_id="")
        .values("nyiso_report_id", "active_task_id")
    )

    if not candidates:
        return []

    task_ids = [str(item["active_task_id"]).strip() for item in candidates if str(item["active_task_id"]).strip()]
    if not task_ids:
        return []

    task_status_by_id = dict(
        task_result_model.objects.filter(task_id__in=task_ids).values_list("task_id", "status")
    )

    active_report_ids: list[int] = []
    for item in candidates:
        task_id = str(item["active_task_id"]).strip()
        task_status = task_status_by_id.get(task_id, "")
        if task_status in INCOMPLETE_TASK_RESULT_STATES:
            active_report_ids.append(int(item["nyiso_report_id"]))

    return active_report_ids


def get_active_task_id_for_report(report_id: int) -> str | None:
    """Return active Celery task id for a report when one is currently incomplete."""
    report = (
        nyiso_report.objects.filter(pk=report_id)
        .exclude(active_task_id="")
        .values("active_task_id")
        .first()
    )
    if not report:
        return None

    task_id = str(report["active_task_id"]).strip()
    if not task_id:
        return None

    task_result_model = apps.get_model("django_celery_results", "TaskResult")
    task_status = task_result_model.objects.filter(task_id=task_id).values_list("status", flat=True).first()
    if task_status in INCOMPLETE_TASK_RESULT_STATES:
        return task_id

    return None
