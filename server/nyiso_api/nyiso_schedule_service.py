from datetime import date

from .nyiso_models import nyiso_report, nyiso_schedule_definition


def parse_optional_date(raw_value: str | None) -> date | None:
    """Parse YYYY-MM-DD to date, returning None for empty input."""
    value = (raw_value or "").strip()
    if not value:
        return None
    return date.fromisoformat(value)


def build_report_lookup_for_schedules(schedules: list[nyiso_schedule_definition]) -> dict[int, nyiso_report]:
    target_report_ids = {int(s.target_ref_id) for s in schedules if s.target_ref_id is not None}
    if not target_report_ids:
        return {}
    reports = nyiso_report.objects.filter(nyiso_report_id__in=target_report_ids)
    return {int(report.nyiso_report_id): report for report in reports}


def serialize_schedule(
    schedule: nyiso_schedule_definition,
    report_lookup: dict[int, nyiso_report] | None = None,
) -> dict[str, object]:
    report = None
    if schedule.target_ref_id:
        if report_lookup is not None:
            report = report_lookup.get(int(schedule.target_ref_id))
        else:
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
