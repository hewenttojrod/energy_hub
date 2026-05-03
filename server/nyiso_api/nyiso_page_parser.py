import re
from datetime import datetime, timezone as dt_timezone
from pathlib import Path
from typing import Any

from bs4 import BeautifulSoup
from django.utils import timezone


WHITESPACE_PATTERN = re.compile(r"\s+")
REPORT_CODE_PATTERN = re.compile(r"\b(?P<code>[A-Z]-\d{1,3}[A-Z]?)\b")
LIST_TOKEN_PATTERN = re.compile(r"list", flags=re.IGNORECASE)
DATESTAMP_PATTERN = re.compile(r"(?<!\d)(?P<day>\d{7,8})(?P<time>\d{2}(?:\d{2})?)?(?!\d)")
INLINE_FEED_TIMESTAMP_PATTERN = re.compile(r"\b\d{2}-[A-Za-z]{3}-\d{4}\s+\d{2}:\d{2}:\d{2}\b")
DOWNLOAD_EXTENSION_MAP = {
    "htm": "HTML",
    "html": "HTML",
    "csv": "CSV",
    "pdf": "PDF",
    "zip": "ZIP",
}
NYISO_PUBLIC_BASE_URL = "https://mis.nyiso.com/public/"
FILE_NAME_TEMPLATE_SEPARATOR = " || "


def _normalize_whitespace(text: str) -> str:
    """Collapse repeated whitespace into single spaces and trim surrounding whitespace."""
    return WHITESPACE_PATTERN.sub(" ", text).strip()


def _iter_anchor_pairs(html: str) -> list[tuple[str, str]]:
    """Extract href/text pairs from anchor tags."""
    soup = BeautifulSoup(html, "html.parser")
    return [
        (
            (anchor.get("href") or "").strip(),
            anchor.get_text(" ", strip=True),
        )
        for anchor in soup.find_all("a")
    ]


def _extract_source_page(href: str) -> str:
    """Normalize menu href values into NYISO page paths for downstream fetches."""
    normalized = href.strip().lstrip("/")
    if not normalized:
        return ""
    if "/public/" in normalized:
        normalized = normalized.split("/public/", maxsplit=1)[1]
    normalized = normalized.split("#", maxsplit=1)[0]
    normalized = normalized.split("?", maxsplit=1)[0]
    return normalized


def _normalize_code(source_text: str, href: str) -> str:
    """Extract and normalize a report code from text/href context."""
    code_match = REPORT_CODE_PATTERN.search(source_text.upper())
    raw_code = code_match.group("code") if code_match else Path(href).stem.upper()[:25]
    return LIST_TOKEN_PATTERN.sub("", raw_code)[:25]


def _infer_content_type(source_page: str, report_model: Any) -> str:
    """Infer high-level report content type from known NYISO page paths."""
    normalized_source = source_page.lower()
    if "opermessages/currentopermessages.htm" in normalized_source:
        return report_model.report_content_type.INLINE_FEED
    return report_model.report_content_type.FILE_LIST


def _extract_inline_feed_datestamps(html: str) -> list[datetime]:
    """Extract descending unique timestamps from inline feed message tables."""
    seen: set[datetime] = set()
    stamps: list[datetime] = []
    soup = BeautifulSoup(html, "html.parser")

    for cell in soup.find_all(["td", "th"]):
        text = cell.get_text(" ", strip=True)
        if not text:
            continue
        match = INLINE_FEED_TIMESTAMP_PATTERN.search(text)
        if not match:
            continue
        try:
            parsed = datetime.strptime(match.group(0), "%d-%b-%Y %H:%M:%S").replace(
                tzinfo=dt_timezone.utc
            )
        except ValueError:
            continue
        if parsed in seen:
            continue
        seen.add(parsed)
        stamps.append(parsed)

    stamps.sort(reverse=True)
    return stamps


def _extract_inline_feed_rows(html: str) -> list[dict[str, str]]:
    """Extract inline feed rows with message type, time, and message text."""
    rows: list[dict[str, str]] = []
    soup = BeautifulSoup(html, "html.parser")

    for heading in soup.find_all("h3"):
        message_type = _normalize_whitespace(heading.get_text(" ", strip=True))
        if not message_type:
            continue

        table = heading.find_next("table")
        if table is None:
            continue

        for tr in table.find_all("tr"):
            cells = tr.find_all("td")
            if len(cells) < 2:
                continue

            time_text = _normalize_whitespace(cells[0].get_text(" ", strip=True))
            message_text = _normalize_whitespace(cells[1].get_text(" ", strip=True))
            if not (time_text or message_text):
                continue

            rows.append(
                {
                    "message_type": message_type,
                    "time": time_text,
                    "message": message_text,
                }
            )

    return rows


def _to_public_download_url(candidate: str) -> str:
    """Resolve NYISO public-relative hrefs into absolute download URLs."""
    normalized = candidate.strip().lstrip("/")
    if normalized.lower().startswith("http://") or normalized.lower().startswith("https://"):
        return normalized
    return NYISO_PUBLIC_BASE_URL + normalized


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

    parse_formats = [
        "%m-%d-%Y",
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%Y/%m/%d",
        "%Y%m%d",
        "%m%d%Y",
    ]
    parsed_dt: datetime | None = None
    for fmt in parse_formats:
        try:
            parsed_dt = datetime.strptime(cleaned, fmt)
            break
        except ValueError:
            continue

    if parsed_dt is None:
        return cleaned

    output_formats = {
        "date": "%m-%d-%Y",
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

    candidates: list[str] = []
    seen: set[str] = set()
    for template in templates:
        for ext in extensions:
            replacements = {
                "report": report_seed,
                "code": report_seed,
                "fileextension": ext,
                "fileextention": ext,
                "date": _format_date_value(date_seed, "date"),
                "date_yyyymmdd": _format_date_value(date_seed, "date_yyyymmdd"),
                "date_mmddyyyy": _format_date_value(date_seed, "date_mmddyyyy"),
                "date_mm-dd-yyyy": _format_date_value(date_seed, "date_mm-dd-yyyy"),
                "date_yyyy-mm-dd": _format_date_value(date_seed, "date_yyyy-mm-dd"),
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
    if not file_name_candidates:
        return None

    candidate_set = {Path(name).name.lower() for name in file_name_candidates if name}
    if not candidate_set:
        return None

    for href, label in _iter_anchor_pairs(index_html):
        for value in (href, label):
            base = Path((value or "").split("?", maxsplit=1)[0]).name.lower()
            if not base:
                continue
            if base in candidate_set:
                return href

    return None


def build_report_detail_grid_payload(
    record: dict[str, object],
    index_html: str,
    report_model: Any,
) -> dict[str, object]:
    """Build a detail-grid payload tailored to the report's content type and files."""
    content_type = str(record.get("content_type", report_model.report_content_type.FILE_LIST))
    if content_type == report_model.report_content_type.INLINE_FEED:
        return {
            "mode": "INLINE_FEED",
            "file_types": [],
            "rows": _extract_inline_feed_rows(index_html),
        }

    download_file_names = _extract_download_file_names(_extract_file_name_candidates(index_html))
    if not download_file_names:
        return {"mode": "FILE_MATRIX", "file_types": [], "rows": []}

    items: list[dict[str, object]] = []
    has_datestamps = False
    for candidate in download_file_names:
        basename = Path(candidate).name
        extension = Path(candidate).suffix.lower().lstrip(".")
        mapped_name = DOWNLOAD_EXTENSION_MAP.get(extension)
        datestamps, _ = _extract_datestamps([candidate])
        stamp = datestamps[0] if datestamps else None
        has_datestamps = has_datestamps or stamp is not None
        items.append(
            {
                "candidate": candidate,
                "basename": basename,
                "file_type": mapped_name or extension.upper() or "FILE",
                "stamp": stamp,
                "url": _to_public_download_url(candidate),
            }
        )

    if len(items) <= 3 and not has_datestamps:
        singular_rows = [
            {
                "label": str(item["basename"]),
                "url": str(item["url"]),
            }
            for item in items
        ]
        return {
            "mode": "SINGULAR_FILES",
            "file_types": [],
            "rows": singular_rows,
        }

    file_types: list[str] = []
    grouped_rows: dict[str, dict[str, object]] = {}
    for item in items:
        file_type = str(item["file_type"])
        if file_type != "ZIP" and file_type not in file_types:
            file_types.append(file_type)

        stamp = item["stamp"]
        if isinstance(stamp, datetime):
            row_key = stamp.strftime("%Y-%m-%d")
            date_label = row_key
            time_label = stamp.strftime("%H:%M:%S") if stamp.time().isoformat() != "00:00:00" else "-"
            sort_key = stamp.isoformat()
        else:
            row_key = str(item["basename"])
            date_label = "-"
            time_label = "-"
            sort_key = ""

        row = grouped_rows.setdefault(
            row_key,
            {
                "date": date_label,
                "last_updated": time_label,
                "links": {},
                "zip_links": [],
                "_sort": sort_key,
            },
        )
        if file_type == "ZIP":
            zip_links = row["zip_links"]
            if isinstance(zip_links, list):
                zip_links.append(str(item["url"]))
            continue

        links = row["links"]
        if isinstance(links, dict):
            links[file_type] = str(item["url"])

    if not file_types and any(item.get("file_type") == "ZIP" for item in items):
        file_types.append("CSV")

    for row in grouped_rows.values():
        links = row.get("links")
        zip_links = row.get("zip_links")
        if not isinstance(links, dict) or not isinstance(zip_links, list):
            continue

        for zip_url in zip_links:
            target_column = next((column for column in file_types if column not in links), None)
            if target_column is None:
                target_column = "CSV"
                if target_column not in file_types:
                    file_types.append(target_column)
                if target_column in links:
                    continue
            links[target_column] = str(zip_url)

    rows = sorted(
        grouped_rows.values(),
        key=lambda item: str(item.get("_sort", "")),
        reverse=True,
    )
    for row in rows:
        row.pop("zip_links", None)
        row.pop("_sort", None)

    return {
        "mode": "FILE_MATRIX",
        "file_types": file_types,
        "rows": rows,
    }


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


def report_menu_parser(html: str, report_model: Any) -> list[dict[str, object]]:
    """Parse NYISO menu HTML into report records."""
    records: list[dict[str, object]] = []
    seen: set[tuple[str, str]] = set()

    for href, label_text in _iter_anchor_pairs(html):
        label = label_text or Path(href).name

        is_supported_href = href and not href.startswith("#") and not href.lower().startswith("javascript:")
        source_page = _extract_source_page(href) if is_supported_href else ""

        clean_label = _normalize_whitespace(label)[:500]
        source = f"{clean_label} {href}"
        code = _normalize_code(source, href)

        context: dict[str, object] = {
            "code": code,
            "name": clean_label,
            "frequency": [],
            "content_type": _infer_content_type(source_page, report_model),
            "source_page": source_page,
            "parse_status": report_model.parse_state.PARTIAL,
            "is_deprecated": False,
        }

        if not (context["code"] and context["name"] and context["source_page"]):
            continue

        key = (str(context["code"]), str(context["name"]))
        if key in seen:
            continue
        seen.add(key)
        records.append(context)

    return records


def report_parser(record: dict[str, object], index_html: str, report_model: Any) -> dict[str, object]:
    """Parse a report list page and enrich a report record with derived metadata."""
    if str(record.get("content_type", "")) == report_model.report_content_type.INLINE_FEED:
        feed_datestamps = _extract_inline_feed_datestamps(index_html)
        record["content_type"] = report_model.report_content_type.INLINE_FEED
        record["file_type"] = []
        record["file_name_format"] = ""
        record["latest_report_stamp"] = feed_datestamps[0] if feed_datestamps else None
        record["earliest_report_stamp"] = feed_datestamps[-1] if feed_datestamps else None
        record["parse_status"] = report_model.parse_state.OK
        record["last_scanned_at"] = timezone.now()
        return record

    file_names = _extract_file_name_candidates(index_html)
    download_file_names = _extract_download_file_names(file_names) or file_names

    inferred_file_types = _infer_file_types(report_model, download_file_names)
    if inferred_file_types:
        record["file_type"] = inferred_file_types

    inferred_name_format = _infer_file_name_format(download_file_names)
    if inferred_name_format:
        record["file_name_format"] = inferred_name_format

    datestamps, has_time_component = _extract_datestamps(download_file_names)
    inferred_frequencies = _infer_frequencies_from_report_files(
        report_model,
        download_file_names,
        datestamps,
        has_time_component,
    )
    if inferred_frequencies:
        record["frequency"] = inferred_frequencies

    if not datestamps:
        if report_model.freq_type.SINGLE in inferred_frequencies and inferred_name_format:
            record["latest_report_stamp"] = None
            record["earliest_report_stamp"] = None
            record["parse_status"] = report_model.parse_state.OK
        else:
            record["parse_status"] = report_model.parse_state.PARTIAL
        record["last_scanned_at"] = timezone.now()
        return record

    record["latest_report_stamp"] = datestamps[0]
    record["earliest_report_stamp"] = datestamps[-1]
    record["parse_status"] = report_model.parse_state.OK
    record["last_scanned_at"] = timezone.now()
    return record
