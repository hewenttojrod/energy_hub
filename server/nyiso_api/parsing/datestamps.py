from datetime import datetime, timezone as dt_timezone
from pathlib import Path
from typing import Any

from .constants import DATESTAMP_PATTERN, DOWNLOAD_EXTENSION_MAP


def _parse_datestamp(day_part: str, time_part: str | None) -> datetime | None:
    """Parse a datestamp token into a UTC-aware datetime when valid."""
    try:
        if time_part:
            fmt = "%Y%m%d%H%M" if len(time_part) == 4 else "%Y%m%d%H"
            parsed = datetime.strptime(day_part + time_part, fmt)
        else:
            parsed = datetime.strptime(day_part, "%Y%m%d")
        return parsed.replace(tzinfo=dt_timezone.utc)
    except ValueError:
        pass

    # Fallback: try MDDYYYY / MMDDYYYY (some NYISO files use month-first ordering)
    try:
        padded = day_part.zfill(8)
        parsed = datetime.strptime(padded, "%m%d%Y")
        return parsed.replace(tzinfo=dt_timezone.utc)
    except ValueError:
        return None


def _extract_datestamps(file_names: list[str]) -> tuple[list[datetime], bool]:
    """Extract descending unique datestamps and whether any stamp had a time part."""
    seen: set[datetime] = set()
    stamps: list[datetime] = []
    has_time_component = False

    for candidate in file_names:
        basename = Path(candidate).name
        if not basename:
            continue

        for match in DATESTAMP_PATTERN.finditer(basename):
            parsed = _parse_datestamp(match.group("day"), match.group("time"))
            if parsed is None or parsed in seen:
                continue
            has_time_component = has_time_component or bool(match.group("time"))
            seen.add(parsed)
            stamps.append(parsed)

    stamps.sort(reverse=True)
    return stamps, has_time_component


def _infer_file_types(report_model: Any, file_names: list[str]) -> list[str]:
    """Infer nyiso_report.download_type values from observed file-name extensions."""
    inferred: list[str] = []

    for candidate in file_names:
        ext = Path(candidate).suffix.lower().lstrip(".")
        mapped_name = DOWNLOAD_EXTENSION_MAP.get(ext)
        mapped = getattr(report_model.download_type, mapped_name) if mapped_name else None
        if mapped and mapped not in inferred:
            inferred.append(mapped)

    return inferred


def _infer_frequency_from_datestamps(report_model: Any, datestamps: list[datetime]) -> str | None:
    """Infer report frequency from intervals between recent report datestamps."""
    if len(datestamps) < 2:
        return None

    deltas_hours: list[float] = []
    for i in range(min(len(datestamps) - 1, 6)):
        delta = datestamps[i] - datestamps[i + 1]
        if delta.total_seconds() > 0:
            deltas_hours.append(delta.total_seconds() / 3600)

    if not deltas_hours:
        return None

    avg_hours = sum(deltas_hours) / len(deltas_hours)
    if avg_hours <= 1.5:
        return report_model.freq_type.REAL
    if avg_hours <= 30:
        return report_model.freq_type.HOUR
    if avg_hours <= 24 * 45:
        return report_model.freq_type.DAY
    return report_model.freq_type.YEAR


def _infer_frequencies_from_report_files(
    report_model: Any,
    file_names: list[str],
    datestamps: list[datetime],
    has_time_component: bool,
) -> list[str]:
    """Infer one or more report frequencies from actual files present on the list page."""
    if not datestamps:
        if 1 <= len(file_names) <= 3:
            return [report_model.freq_type.SINGLE]
        return []

    inferred: list[str] = []

    interval_frequency = _infer_frequency_from_datestamps(report_model, datestamps)
    if interval_frequency:
        inferred.append(interval_frequency)

    if has_time_component and report_model.freq_type.HOUR not in inferred:
        inferred.append(report_model.freq_type.HOUR)

    return inferred
