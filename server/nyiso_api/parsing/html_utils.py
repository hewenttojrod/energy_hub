from datetime import datetime, timezone as dt_timezone
from pathlib import Path
from typing import Any

from bs4 import BeautifulSoup

from .constants import (
    INLINE_FEED_TIMESTAMP_PATTERN,
    LIST_TOKEN_PATTERN,
    NYISO_PUBLIC_BASE_URL,
    REPORT_CODE_PATTERN,
    WHITESPACE_PATTERN,
)


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
