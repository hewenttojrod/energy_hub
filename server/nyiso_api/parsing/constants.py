import re

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
