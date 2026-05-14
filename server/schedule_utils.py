import fnmatch
import re
from datetime import date, timedelta
from pathlib import Path

from django.utils import timezone


def resolve_schedule_dates(schedule, report=None) -> list[date]:
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


def coerce_bool(raw_value: object) -> bool:
    if isinstance(raw_value, bool):
        return raw_value
    if raw_value is None:
        return False
    return str(raw_value).strip().lower() in {"1", "true", "yes", "y", "on"}


def coerce_int(raw_value: object, default: int) -> int:
    if raw_value is None:
        return default
    try:
        return int(str(raw_value).strip())
    except (TypeError, ValueError):
        return default


def parse_force_tokens(raw_value: object) -> list[str]:
    if raw_value is None:
        return []
    if isinstance(raw_value, (list, tuple, set)):
        values = [str(item).strip() for item in raw_value]
        return [item for item in values if item]

    text = str(raw_value).strip()
    if not text:
        return []
    return [token.strip() for token in re.split(r"[,;|\n]+", text) if token.strip()]


def get_schedule_force_redownload_rules(schedule) -> tuple[bool, set[str], list[str], int]:
    config = dict(schedule.module_config_json or {})
    force_all = coerce_bool(config.get("force_redownload_all"))
    forced_dates = set(parse_force_tokens(config.get("force_redownload_dates")))
    forced_patterns = parse_force_tokens(
        config.get("force_redownload_patterns") or config.get("force_redownload_files")
    )
    # By default, always revalidate the most recent day so late-arriving/corrected files are picked up.
    auto_recent_days = max(0, coerce_int(config.get("force_redownload_recent_days"), default=1))
    return force_all, forced_dates, forced_patterns, auto_recent_days


def should_force_redownload(
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