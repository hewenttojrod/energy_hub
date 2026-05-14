import csv
import io
import zipfile
from pathlib import Path


def extract_csv_dicts(storage_path: str, file_type: str) -> list[dict]:
    """Return a list of row dicts from a CSV or ZIP-containing-CSV file."""
    file_content = []
    rows = []
    if file_type.upper() == "ZIP":
        with zipfile.ZipFile(storage_path) as zf:
            csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            if not csv_names:
                raise ValueError(f"No CSV found inside ZIP: {storage_path}")
            for csv_name in csv_names:
                file_content.append(zf.read(csv_name).decode("utf-8-sig"))
    else:
        file_content.append(Path(storage_path).read_text(encoding="utf-8-sig"))

    for content in file_content:
        
        rows += [dict(row) for row in csv.DictReader(io.StringIO(content))]
    return rows


TS_FORMATS = [
    "%m/%d/%Y %H:%M:%S",
    "%m/%d/%Y %H:%M",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%dT%H:%M:%S",
]


def iter_in_batches(items, batch_size: int):
    """Yield fixed-size batches from an iterator."""
    batch = []
    for item in items:
        batch.append(item)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def is_five_minute_boundary(ts) -> bool:
    return ts.minute % 5 == 0 and ts.second == 0 and ts.microsecond == 0


def summarize_timestamp_quality(parsed_timestamps: list):
    valid = [ts for ts in parsed_timestamps if ts is not None]
    invalid_count = len(parsed_timestamps) - len(valid)
    off_interval_count = sum(1 for ts in valid if not is_five_minute_boundary(ts))

    gap_count = 0
    sorted_unique = sorted(set(valid))
    for idx in range(1, len(sorted_unique)):
        delta_seconds = int((sorted_unique[idx] - sorted_unique[idx - 1]).total_seconds())
        if delta_seconds > 300 and delta_seconds % 300 == 0:
            gap_count += (delta_seconds // 300) - 1

    return {
        "invalid_timestamp_count": invalid_count,
        "off_interval_count": off_interval_count,
        "gap_count": gap_count,
    }


def parse_timestamp(raw: str):
    """Try common NYISO timestamp formats and return a timezone-marked datetime or None."""
    from datetime import datetime, timezone as dt_timezone

    raw = (raw or "").strip()
    if not raw:
        return None

    for fmt in TS_FORMATS:
        try:
            dt = datetime.strptime(raw, fmt)
            # Attach UTC marker without any timezone shift — preserves local time value.
            return dt.replace(tzinfo=dt_timezone.utc)
        except ValueError:
            continue
    return None
