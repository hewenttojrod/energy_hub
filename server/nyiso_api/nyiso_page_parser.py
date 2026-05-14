"""
Public API for NYISO page parsing. Implementation lives in the parsing/ subpackage.
This module re-exports all public symbols for backward compatibility.
"""
from pathlib import Path
from typing import Any

from django.utils import timezone

from .parsing.datestamps import (
    _extract_datestamps,
    _infer_file_types,
    _infer_frequencies_from_report_files,
)
from .parsing.file_patterns import (
    _extract_download_file_names,
    _extract_file_name_candidates,
    _infer_file_name_format,
    build_download_href_index,
    build_file_name_candidates,
    resolve_download_href,
    resolve_download_href_from_index,
)
from .parsing.grid_layout import build_report_detail_grid_payload
from .parsing.html_utils import (
    _extract_inline_feed_datestamps,
    _extract_source_page,
    _infer_content_type,
    _iter_anchor_pairs,
    _normalize_code,
    _normalize_whitespace,
)

__all__ = [
    "build_download_href_index",
    "build_file_name_candidates",
    "build_report_detail_grid_payload",
    "resolve_download_href",
    "resolve_download_href_from_index",
    "report_menu_parser",
    "report_parser",
]


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

