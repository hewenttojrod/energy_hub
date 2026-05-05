import os
from datetime import timedelta, timezone as dt_timezone
from pathlib import Path
from urllib.parse import urlparse
import hashlib

import requests
from bs4 import BeautifulSoup, Comment
from django.utils import timezone
from core.src.util.requests_wrapper import delayed_get
from core.models import source_file
from .nyiso_models import nyiso_report, nyiso_report_file


class NyisoPageHandler:
    """Handle NYISO page retrieval and cache lifecycle."""

    INDEX_URL = "https://mis.nyiso.com/public/"
    CACHE_MAX_AGE = timedelta(days=1)
    USER_AGENT = "pvrd-framework-energy-hub/1.0"
    REQUEST_DELAY_MIN_SECONDS = 1.0
    REQUEST_DELAY_MAX_SECONDS = 5.0

    def __init__(
        self,
        *,
        index_url: str = INDEX_URL,
        cache_max_age: timedelta = CACHE_MAX_AGE,
        user_agent: str = USER_AGENT,
        request_delay_min_seconds: float = REQUEST_DELAY_MIN_SECONDS,
        request_delay_max_seconds: float = REQUEST_DELAY_MAX_SECONDS,
    ) -> None:
        self.index_url = index_url
        self.cache_max_age = cache_max_age
        self.user_agent = user_agent
        self.request_delay_min_seconds = request_delay_min_seconds
        self.request_delay_max_seconds = request_delay_max_seconds

    def _cache_dir(self) -> Path:
        """Return nyiso cache dir from DATA_ROOT env var or local project root."""
        data_root = os.getenv("DATA_ROOT")
        if data_root:
            return Path(data_root) / "nyiso"
        # Local dev: modules/energy_hub/server/nyiso_api -> parents[4] = project root
        project_root = Path(__file__).resolve().parents[4]
        return project_root / "data" / "nyiso"

    def _downloads_dir(self) -> Path:
        """Return local NYISO downloads dir used for persisted downloaded files."""
        return self._cache_dir() / "downloads"

    def _normalize_page_path(self, page: str) -> str:
        """Return a normalized NYISO page path, defaulting extensionless inputs to .htm."""
        normalized_page = page.strip().lstrip("/")
        if not normalized_page:
            raise ValueError("page must be a non-empty path relative to /public/")

        if not Path(normalized_page).suffix:
            normalized_page = f"{normalized_page}.htm"
        return normalized_page

    def _cache_path_for_page(self, page: str) -> Path:
        """Return cache path for an NYISO page relative to /public/."""
        normalized_page = self._normalize_page_path(page)
        return self._cache_dir() / "pages" / Path(*normalized_page.split("/"))

    def _is_fresh(self, path: Path, cache_max_age: timedelta | None = None) -> bool:
        """Return True when a cache file exists and is within the selected cache age."""
        if not path.exists():
            return False
        age_limit = self.cache_max_age if cache_max_age is None else cache_max_age
        modified = timezone.datetime.fromtimestamp(path.stat().st_mtime, tz=dt_timezone.utc)
        return timezone.now() - modified <= age_limit

    def _remove_commented_html(self, html: str) -> str:
        """Strip HTML comments so commented-out markup is not processed downstream."""
        soup = BeautifulSoup(html, "html.parser")
        for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
            comment.extract()
        return str(soup)

    def get_page_html(self, page: str, cache_max_age: timedelta | None = None) -> str:
        """Return NYISO page HTML, downloading only when cache is stale or missing."""
        normalized_page = self._normalize_page_path(page)

        cache_path = self._cache_path_for_page(normalized_page)
        cache_path.parent.mkdir(parents=True, exist_ok=True)

        if self._is_fresh(cache_path, cache_max_age=cache_max_age):
            cached_html = cache_path.read_text(encoding="utf-8", errors="ignore")
            sanitized_cached_html = self._remove_commented_html(cached_html)
            if sanitized_cached_html != cached_html:
                cache_path.write_text(sanitized_cached_html, encoding="utf-8")
            return sanitized_cached_html

        page_url = self.index_url.rstrip("/") + f"/{normalized_page}"

        try:
            response = delayed_get(
                page_url,
                min_timeout_seconds=self.request_delay_min_seconds,
                max_timeout_seconds=self.request_delay_max_seconds,
                request_timeout_seconds=30,
                headers={"User-Agent": self.user_agent},
            )
            response.raise_for_status()
            html = self._remove_commented_html(response.text)
            cache_path.write_text(html, encoding="utf-8")
            return html
        except requests.RequestException:
            if cache_path.exists():
                cached_html = cache_path.read_text(encoding="utf-8", errors="ignore")
                sanitized_cached_html = self._remove_commented_html(cached_html)
                if sanitized_cached_html != cached_html:
                    cache_path.write_text(sanitized_cached_html, encoding="utf-8")
                return sanitized_cached_html
            raise

    def is_page_accessible(self, page: str, cache_max_age: timedelta | None = None) -> bool:
        """Return True when page retrieval succeeds via the standard cached NYISO fetch path."""
        try:
            self.get_page_html(page, cache_max_age=cache_max_age)
            return True
        except requests.RequestException:
            return False

    def requires_page_pull(self, page: str, cache_max_age: timedelta | None = None) -> bool:
        """Return True when page cache is missing/stale and a network pull would be required."""
        if cache_max_age is None:
            return False
        return not self._is_fresh(self._cache_path_for_page(page), cache_max_age=cache_max_age)

    def get_menu_html(self) -> str:
        """Resolve NYISO menu HTML from cache or network."""
        return self.get_page_html("menu.htm")

    def get_report_index_html(self, report_code: str) -> str:
        """Resolve a report index page (e.g. /public/{code}list.htm) from cache or network."""
        return self.get_page_html(f"{report_code}list.htm")

    def get_page_content_html(self, page: str, cache_max_age: timedelta | None = None) -> str:
        """Return usable content HTML for a page, following frameset inner frames when needed."""
        html = self.get_page_html(page, cache_max_age=cache_max_age)
        inner_frames = self._extract_inner_frame_pages(html)
        if not inner_frames:
            return html

        combined_parts: list[str] = []
        for frame_page in inner_frames:
            try:
                combined_parts.append(self.get_page_html(frame_page, cache_max_age=cache_max_age))
            except requests.RequestException:
                continue
        return "\n".join(combined_parts) if combined_parts else html

    def _extract_inner_frame_pages(self, html: str) -> list[str]:
        """Return page paths for inner frames when the HTML is a frameset, else empty list."""
        soup = BeautifulSoup(html, "html.parser")
        frames = soup.find_all("frame")
        if not frames:
            return []

        inner_pages: list[str] = []
        for frame in frames:
            src = (frame.get("src") or "").strip()
            if src:
                inner_pages.append(self._normalize_page_path(src.lstrip("/")))
        return inner_pages

    def find_download_href(self, index_html: str, file_name_candidates: list[str]) -> str | None:
        """Return first href from index html matching one of the requested filenames."""
        from .nyiso_page_parser import resolve_download_href

        return resolve_download_href(index_html, file_name_candidates)

    def download_public_file(self, href_or_url: str, destination_file_name: str | None = None, report_id: int | None = None) -> dict[str, object]:
        """Download a NYISO public file to local cache, save to database, and return destination metadata."""
        normalized = (href_or_url or "").strip()
        if not normalized:
            raise ValueError("download href is required")

        if normalized.lower().startswith("http://") or normalized.lower().startswith("https://"):
            download_url = normalized
        else:
            download_url = self.index_url.rstrip("/") + "/" + normalized.lstrip("/")

        parsed = urlparse(download_url)
        resolved_name = destination_file_name or Path(parsed.path).name
        if not resolved_name:
            raise ValueError("unable to derive destination file name")

        destination_path = self._downloads_dir() / resolved_name
        destination_path.parent.mkdir(parents=True, exist_ok=True)

        response = delayed_get(
            download_url,
            min_timeout_seconds=self.request_delay_min_seconds,
            max_timeout_seconds=self.request_delay_max_seconds,
            request_timeout_seconds=60,
            headers={"User-Agent": self.user_agent},
        )
        response.raise_for_status()
        file_content = response.content
        destination_path.write_bytes(file_content)
        
        # Calculate SHA256 checksum for deduplication
        checksum = hashlib.sha256(file_content).hexdigest()
        
        # Determine file type from extension
        file_ext = Path(resolved_name).suffix.lower().lstrip(".")
        file_type_map = {
            "csv": "CSV",
            "zip": "ZIP",
            "pdf": "PDF",
            "xlsx": "XLSX",
            "xls": "XLS",
            "json": "JSON",
        }
        file_type = file_type_map.get(file_ext, file_ext.upper() if file_ext else "UNKNOWN")
        
        # Create or get source_file record
        src_file, created = source_file.objects.get_or_create(
            checksum_sha256=checksum,
            defaults={
                "source_system": "nyiso",
                "source_url": download_url,
                "source_file_name": resolved_name,
                "storage_path": str(destination_path),
                "file_type": file_type,
            }
        )
        
        # Create bridge record if report_id provided
        if report_id:
            try:
                report_obj = nyiso_report.objects.get(nyiso_report_id=report_id)
                nyiso_report_file.objects.get_or_create(
                    nyiso_report=report_obj,
                    source_file=src_file,
                )
            except nyiso_report.DoesNotExist:
                pass  # Report doesn't exist, skip bridge creation

        return {
            "url": download_url,
            "file_name": resolved_name,
            "local_path": str(destination_path),
            "source_file_id": src_file.source_file_id,
            "checksum": checksum,
            "file_type": file_type,
            "size_bytes": len(file_content),
            "created": created,
        }
