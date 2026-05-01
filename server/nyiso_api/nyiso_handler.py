import os
import re
from datetime import datetime
from datetime import timedelta, timezone as dt_timezone
from pathlib import Path
from typing import Any

import requests
from django.utils import timezone
from core.src.util.requests_wrapper import delayed_get

from .nyiso_models import nyiso_report


class NyisoPageHandler:
    """Handle NYISO page retrieval, URL resolution, and cache lifecycle."""

    def __init__(
        self,
        *,
        index_url: str,
        index_filename: str,
        menu_filename: str,
        cache_max_age: timedelta,
        user_agent: str,
    ) -> None:
        self.index_url = index_url
        self.index_filename = index_filename
        self.menu_filename = menu_filename
        self.cache_max_age = cache_max_age
        self.user_agent = user_agent

    def _cache_dir(self) -> Path:
        """Return nyiso cache dir from DATA_ROOT env var or local project root."""
        data_root = os.getenv("DATA_ROOT")
        if data_root:
            return Path(data_root) / "nyiso"
        # Local dev: modules/energy_hub/server/nyiso_api -> parents[4] = project root
        project_root = Path(__file__).resolve().parents[4]
        return project_root / "data" / "nyiso"

    def _cache_path(self) -> Path:
        """Return cache path for index (frameset shell) HTML."""
        return self._cache_dir() / self.index_filename

    def _menu_cache_path(self) -> Path:
        """Return cache path for menu (report links) HTML."""
        return self._cache_dir() / self.menu_filename

    def _report_index_cache_path(self, report_code: str) -> Path:
        """Return cache path for a specific report index page HTML."""
        safe_code = re.sub(r"[^A-Za-z0-9_-]+", "", report_code).lower() or "unknown"
        return self._cache_dir() / "report_indexes" / f"{safe_code}list.htm"

    def _is_fresh(self, path: Path) -> bool:
        """Return True when a cache file exists and is within cache_max_age."""
        if not path.exists():
            return False
        modified = timezone.datetime.fromtimestamp(path.stat().st_mtime, tz=dt_timezone.utc)
        return timezone.now() - modified <= self.cache_max_age

    def get_index_html(self) -> str:
        """Resolve NYISO frameset index HTML from cache or network."""
        cache_path = self._cache_path()
        cache_path.parent.mkdir(parents=True, exist_ok=True)

        if self._is_fresh(cache_path):
            return cache_path.read_text(encoding="utf-8", errors="ignore")

        try:
            response = delayed_get(
                self.index_url,
                request_timeout_seconds=30,
                headers={"User-Agent": self.user_agent},
            )
            response.raise_for_status()
            html = response.text
            cache_path.write_text(html, encoding="utf-8")
            return html
        except requests.RequestException:
            if cache_path.exists():
                return cache_path.read_text(encoding="utf-8", errors="ignore")
            raise

    def resolve_menu_url(self, index_html: str) -> str:
        """Parse the frameset index HTML and return the absolute menu frame URL."""
        frame_src_pattern = re.compile(r"<frame[^>]+src=[\"']([^\"']+)[\"']", re.IGNORECASE)
        for src in frame_src_pattern.findall(index_html):
            src_lower = src.lower()
            if "menu" in src_lower:
                if src_lower.startswith("http"):
                    return src
                return self.index_url.rstrip("/") + "/" + src.lstrip("/")
        return self.index_url.rstrip("/") + "/menu.htm"

    def get_menu_html(self) -> str:
        """Resolve NYISO menu HTML from cache or network."""
        menu_path = self._menu_cache_path()
        menu_path.parent.mkdir(parents=True, exist_ok=True)

        if self._is_fresh(menu_path):
            return menu_path.read_text(encoding="utf-8", errors="ignore")

        index_html = self.get_index_html()
        menu_url = self.resolve_menu_url(index_html)

        try:
            response = delayed_get(
                menu_url,
                request_timeout_seconds=30,
                headers={"User-Agent": self.user_agent},
            )
            response.raise_for_status()
            html = response.text
            menu_path.write_text(html, encoding="utf-8")
            return html
        except requests.RequestException:
            if menu_path.exists():
                return menu_path.read_text(encoding="utf-8", errors="ignore")
            raise

    def get_report_index_html(self, report_code: str) -> str:
        """Resolve a report index page (e.g. /public/{code}list.htm) from cache or network."""
        cache_path = self._report_index_cache_path(report_code)
        cache_path.parent.mkdir(parents=True, exist_ok=True)

        if self._is_fresh(cache_path):
            return cache_path.read_text(encoding="utf-8", errors="ignore")

        report_code_clean = re.sub(r"[^A-Za-z0-9]+", "", report_code)
        report_index_url = self.index_url.rstrip("/") + f"/{report_code_clean}list.htm"

        try:
            response = delayed_get(
                report_index_url,
                request_timeout_seconds=30,
                headers={"User-Agent": self.user_agent},
            )
            response.raise_for_status()
            html = response.text
            cache_path.write_text(html, encoding="utf-8")
            return html
        except requests.RequestException:
            if cache_path.exists():
                return cache_path.read_text(encoding="utf-8", errors="ignore")
            raise


class NyisoReportMetaHandler:
    """Handle NYISO report parsing, code normalization, and frequency inference."""

    def __init__(self, report_model: Any) -> None:
        self.report_model = report_model

    def parse_reports_from_index(self, html: str) -> list[dict[str, object]]:
        """Extract normalized report records from NYISO menu HTML."""
        href_pattern = re.compile(r"<a[^>]*href=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>", re.IGNORECASE | re.DOTALL)
        tag_pattern = re.compile(r"<[^>]+>")

        records: list[dict[str, object]] = []
        seen: set[tuple[str, str]] = set()

        for href, label_raw in href_pattern.findall(html):
            if href.startswith("#") or href.lower().startswith("javascript:"):
                continue

            label = tag_pattern.sub("", label_raw).strip()
            if not label:
                label = Path(href).name

            source = f"{label} {href}"
            name = re.sub(r"\s+", " ", label).strip()[:500]
            if not name:
                continue

            code_match = re.search(r"\b([A-Z]-\d{1,3}[A-Z]?)\b", source.upper())
            code = code_match.group(1) if code_match else Path(href).stem.upper()[:25]
            code = self.normalize_code(code)
            if not code:
                continue

            frequencies = self.infer_frequencies(source)

            key = (code, name)
            if key in seen:
                continue
            seen.add(key)

            records.append(
                {
                    "code": code,
                    "name": name,
                    "frequency": frequencies,
                    "is_deprecated": False,
                }
            )

        return records

    def infer_frequencies(self, source_text: str) -> list[str]:
        """Infer nyiso_report.freq_type values from link label/path text."""
        text = source_text.lower()
        frequencies: list[str] = []

        if any(token in text for token in ("real time", "realtime", " real-time ")):
            frequencies.append(self.report_model.freq_type.REAL)
        if any(token in text for token in ("hourly", "hour ")):
            frequencies.append(self.report_model.freq_type.HOUR)
        if any(token in text for token in ("daily", "day-ahead", "day ")):
            frequencies.append(self.report_model.freq_type.DAY)
        if any(token in text for token in ("annual", "yearly", "year ")):
            frequencies.append(self.report_model.freq_type.YEAR)

        return frequencies

    def normalize_code(self, code: str) -> str:
        """Normalize code values by removing LIST fragments."""
        normalized = re.sub(r"list", "", code, flags=re.IGNORECASE)
        return normalized[:25]

    def enrich_report_from_index_html(self, record: dict[str, object], index_html: str) -> dict[str, object]:
        """Update a report record using date/frequency signals from its report index page."""
        datestamps = self._extract_datestamps(index_html)
        if not datestamps:
            # Keep menu-derived frequency when no dated files are visible.
            return record

        inferred_frequency = self._infer_frequency_from_datestamps(datestamps)
        if inferred_frequency:
            record["frequency"] = [inferred_frequency]

        record["latest_report_stamp"] = datestamps[0]
        return record

    def _extract_datestamps(self, html: str) -> list[datetime]:
        """Extract descending unique datestamps from report index page content."""
        # Supports patterns commonly present in NYISO file names, e.g. YYYYMMDD or YYYYMMDDHHMM.
        stamp_pattern = re.compile(r"(?<!\d)(\d{8})(\d{2}(?:\d{2})?)?(?!\d)")
        seen: set[datetime] = set()
        stamps: list[datetime] = []

        for day_part, time_part in stamp_pattern.findall(html):
            try:
                if time_part:
                    fmt = "%Y%m%d%H%M" if len(time_part) == 4 else "%Y%m%d%H"
                    parsed = datetime.strptime(day_part + time_part, fmt)
                else:
                    parsed = datetime.strptime(day_part, "%Y%m%d")
            except ValueError:
                continue

            if parsed in seen:
                continue
            seen.add(parsed)
            stamps.append(parsed)

        stamps.sort(reverse=True)
        return stamps

    def _infer_frequency_from_datestamps(self, datestamps: list[datetime]) -> str | None:
        """Infer report frequency from intervals between recent report datestamps."""
        if len(datestamps) < 2:
            return None

        # Use up to the 6 most recent gaps to avoid older archival irregularities.
        deltas_hours: list[float] = []
        for i in range(min(len(datestamps) - 1, 6)):
            delta = datestamps[i] - datestamps[i + 1]
            if delta.total_seconds() > 0:
                deltas_hours.append(delta.total_seconds() / 3600)

        if not deltas_hours:
            return None

        avg_hours = sum(deltas_hours) / len(deltas_hours)
        if avg_hours <= 1.5:
            return self.report_model.freq_type.REAL
        if avg_hours <= 30:
            return self.report_model.freq_type.HOUR
        if avg_hours <= 24 * 45:
            return self.report_model.freq_type.DAY
        return self.report_model.freq_type.YEAR

class nyiso_handler:
    """Orchestrate NYISO bootstrap by composing page-data and formatting handlers."""

    INDEX_URL = "https://mis.nyiso.com/public/"
    INDEX_FILENAME = "mis_nyiso_public_index.html"
    MENU_FILENAME = "mis_nyiso_public_menu.htm"
    CACHE_MAX_AGE = timedelta(days=1)
    USER_AGENT = "pvrd-framework-energy-hub/1.0"

    # Kept for backwards compatibility — previously used as CACHE_FILENAME.
    CACHE_FILENAME = INDEX_FILENAME

    def __init__(self, force_reinsert: bool = False) -> None:
        """Create handler instance and eagerly load report metadata."""
        self.report_data: list[dict[str, object]] = []
        self.page_data_handler = NyisoPageHandler(
            index_url=self.INDEX_URL,
            index_filename=self.INDEX_FILENAME,
            menu_filename=self.MENU_FILENAME,
            cache_max_age=self.CACHE_MAX_AGE,
            user_agent=self.USER_AGENT,
        )
        self.data_formatter = NyisoReportMetaHandler(report_model=nyiso_report)

        if force_reinsert:
            nyiso_report.objects.all().delete()

        self.initialize_report_data()

    def initialize_report_data(self) -> None:
        """Ensure nyiso_report is populated and mirrored onto this instance."""
        if not nyiso_report.objects.exists():
            menu_html = self.page_data_handler.get_menu_html()
            parsed_reports = self.data_formatter.parse_reports_from_index(menu_html)
            for report in parsed_reports:
                report_code = str(report.get("code", ""))
                if not report_code:
                    continue
                try:
                    report_index_html = self.page_data_handler.get_report_index_html(report_code)
                    self.data_formatter.enrich_report_from_index_html(report, report_index_html)
                except requests.RequestException:
                    # Keep menu-level data when a report-specific index page is unavailable.
                    continue
            self._populate_report_table(parsed_reports)

        self.report_data = list(
            nyiso_report.objects.values("code", "name", "frequency", "is_deprecated")
        )

    def _populate_report_table(self, records: list[dict[str, object]]) -> None:
        """Wipe table and save each report so model save hooks can run."""
        if not records:
            return

        nyiso_report.objects.all().delete()
        for record in records:
            latest_stamp = record.pop("latest_report_stamp", None)
            model_record = nyiso_report(**record)
            model_record.save(latest_stamp=latest_stamp)
