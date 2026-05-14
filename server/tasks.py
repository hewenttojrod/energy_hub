"""Compatibility task module for Celery autodiscovery.

Task implementations are split across:
- task_refresh.py
- task_schedule.py
- ingestion_tasks.py
"""

from .ingestion_tasks import ingest_nyiso_source_file, normalize_nyiso_source_file
from .task_refresh import refresh_nyiso_report_details, refresh_nyiso_reports
from .task_schedule import (
    _get_schedule_force_redownload_rules,
    _resolve_schedule_dates,
    _should_force_redownload,
    execute_nyiso_report_schedule,
    process_nyiso_report_schedules,
)

__all__ = [
    "refresh_nyiso_report_details",
    "refresh_nyiso_reports",
    "execute_nyiso_report_schedule",
    "process_nyiso_report_schedules",
    "ingest_nyiso_source_file",
    "normalize_nyiso_source_file",
    "_resolve_schedule_dates",
    "_get_schedule_force_redownload_rules",
    "_should_force_redownload",
]
