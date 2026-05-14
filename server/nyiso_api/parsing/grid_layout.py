from datetime import datetime
from pathlib import Path
from typing import Any

from .constants import DOWNLOAD_EXTENSION_MAP
from .datestamps import _extract_datestamps
from .file_patterns import _extract_download_file_names, _extract_file_name_candidates
from .html_utils import _extract_inline_feed_rows, _to_public_download_url


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
