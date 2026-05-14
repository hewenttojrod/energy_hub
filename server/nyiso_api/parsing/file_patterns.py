import re
from datetime import datetime
from pathlib import Path

from .constants import (
    DATESTAMP_PATTERN,
    DOWNLOAD_EXTENSION_MAP,
    FILE_NAME_TEMPLATE_SEPARATOR,
)
from .html_utils import _iter_anchor_pairs


def _normalize_template_pattern(pattern: str) -> str:
    """Normalize one filename pattern into tokenized template form."""
    base = Path(pattern).name.strip()
    if not base:
        return ""

    replaced_date = DATESTAMP_PATTERN.sub("{date}", base)
    stem, ext = Path(replaced_date).stem, Path(replaced_date).suffix.lower().lstrip(".")
    if ext in DOWNLOAD_EXTENSION_MAP:
        return f"{stem}.{{fileextension}}"
    return replaced_date


def _split_file_name_templates(file_name_format: str) -> list[str]:
    """Split persisted file_name_format into one or more template patterns."""
    raw_parts = re.split(r"\s*\|\|\s*|\s*;\s*|\s*\n\s*", file_name_format or "")
    templates: list[str] = []
    seen: set[str] = set()

    for raw in raw_parts:
        normalized = _normalize_template_pattern(raw)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        templates.append(normalized)

    return templates


def _format_date_value(raw: str, style: str) -> str:
    """Render an incoming date string into a specific output style when possible."""
    cleaned = (raw or "").strip()
    if not cleaned:
        return ""

    parsed_dt: datetime | None = None
    try:
        parsed_dt = datetime.fromisoformat(cleaned)
    except ValueError:
        parse_formats = [
            "%m-%d-%Y",
            "%m/%d/%Y",
            "%Y/%m/%d",
            "%Y%m%d",
            "%m%d%Y",
        ]
        for fmt in parse_formats:
            try:
                parsed_dt = datetime.strptime(cleaned, fmt)
                break
            except ValueError:
                continue

    if parsed_dt is None:
        return cleaned

    output_formats = {
        "date": "%Y%m%d",
        "date_yyyymmdd": "%Y%m%d",
        "date_mmddyyyy": "%m%d%Y",
        "date_mm-dd-yyyy": "%m-%d-%Y",
        "date_yyyy-mm-dd": "%Y-%m-%d",
    }
    return parsed_dt.strftime(output_formats.get(style, "%m-%d-%Y"))


def build_file_name_candidates(
    file_name_format: str,
    template_values: dict[str, str] | None = None,
) -> list[str]:
    """Build candidate filenames from one or more templates and replacement values."""
    values = {str(key).lower(): str(value) for key, value in (template_values or {}).items()}
    extensions: list[str] = []
    requested_extension = (values.get("fileextension") or values.get("fileextention") or "").lower().strip(".")
    if requested_extension:
        extensions.append(requested_extension)
    if "zip" not in extensions:
        extensions.append("zip")

    date_seed = values.get("date", "")
    report_seed = values.get("report") or values.get("code") or ""
    templates = _split_file_name_templates(file_name_format)
    if not templates:
        return []

    date_values = {
        "date": _format_date_value(date_seed, "date"),
        "date_yyyymmdd": _format_date_value(date_seed, "date_yyyymmdd"),
        "date_mmddyyyy": _format_date_value(date_seed, "date_mmddyyyy"),
        "date_mm-dd-yyyy": _format_date_value(date_seed, "date_mm-dd-yyyy"),
        "date_yyyy-mm-dd": _format_date_value(date_seed, "date_yyyy-mm-dd"),
    }

    candidates: list[str] = []
    seen: set[str] = set()
    for template in templates:
        for ext in extensions:
            replacements = {
                "report": report_seed,
                "code": report_seed,
                "fileextension": ext,
                "fileextention": ext,
                **date_values,
            }
            rendered = template
            for key, value in replacements.items():
                rendered = rendered.replace("{" + key + "}", value)

            normalized = Path(rendered).name.strip()
            if not normalized:
                continue
            if normalized in seen:
                continue
            seen.add(normalized)
            candidates.append(normalized)

    return candidates


def resolve_download_href(index_html: str, file_name_candidates: list[str]) -> str | None:
    """Return the first matching href from report page anchors for provided filenames."""
    href_index = build_download_href_index(index_html)
    return resolve_download_href_from_index(href_index, file_name_candidates)


def build_download_href_index(index_html: str) -> dict[str, str]:
    """Build anchor lookup map keyed by filename (lower-cased basename)."""
    href_index: dict[str, str] = {}

    for href, label in _iter_anchor_pairs(index_html):
        for value in (href, label):
            base = Path((value or "").split("?", maxsplit=1)[0]).name.lower()
            if not base or base in href_index:
                continue
            href_index[base] = href

    return href_index


def resolve_download_href_from_index(href_index: dict[str, str], file_name_candidates: list[str]) -> str | None:
    """Resolve href using a prebuilt filename->href index."""
    if not file_name_candidates:
        return None

    candidate_set = {Path(name).name.lower() for name in file_name_candidates if name}
    if not candidate_set:
        return None

    for base, href in href_index.items():
        if base in candidate_set:
            return href

    return None


def _extract_file_name_candidates(html: str) -> list[str]:
    """Extract unique candidate file paths/names from report index anchors."""
    names: list[str] = []
    seen: set[str] = set()

    for href, label in _iter_anchor_pairs(html):
        candidate = href or label
        if not candidate:
            continue

        normalized = candidate.split("?", maxsplit=1)[0].strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        names.append(normalized)

    return names


def _extract_download_file_names(file_names: list[str]) -> list[str]:
    """Return only likely downloadable report files based on supported extensions."""
    download_file_names: list[str] = []
    for candidate in file_names:
        extension = Path(candidate).suffix.lower().lstrip(".")
        if extension in DOWNLOAD_EXTENSION_MAP:
            download_file_names.append(candidate)
    return download_file_names


def _infer_file_name_format(file_names: list[str]) -> str:
    """Infer one or more filename templates and join them into persisted format text."""
    templates: list[str] = []
    seen: set[str] = set()

    for candidate in file_names:
        normalized = _normalize_template_pattern(candidate)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        templates.append(normalized)
        if len(templates) >= 5:
            break

    return FILE_NAME_TEMPLATE_SEPARATOR.join(templates)
