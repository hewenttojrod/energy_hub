import csv
import hashlib
import io
import json
import zipfile
from decimal import Decimal, InvalidOperation
from pathlib import Path

from celery import shared_task
from django.db import transaction
from django.utils import timezone


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extract_csv_dicts(storage_path: str, file_type: str) -> list[dict]:
    """Return a list of row dicts from a CSV or ZIP-containing-CSV file."""
    if file_type.upper() == "ZIP":
        with zipfile.ZipFile(storage_path) as zf:
            csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            if not csv_names:
                raise ValueError(f"No CSV found inside ZIP: {storage_path}")
            content = zf.read(csv_names[0]).decode("utf-8-sig")
    else:
        content = Path(storage_path).read_text(encoding="utf-8-sig")

    reader = csv.DictReader(io.StringIO(content))
    return [dict(row) for row in reader]


_TS_FORMATS = [
    "%m/%d/%Y %H:%M:%S",
    "%m/%d/%Y %H:%M",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%dT%H:%M:%S",
]


def _iter_in_batches(items, batch_size: int):
    """Yield fixed-size batches from an iterator."""
    batch = []
    for item in items:
        batch.append(item)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def _is_five_minute_boundary(ts) -> bool:
    return ts.minute % 5 == 0 and ts.second == 0 and ts.microsecond == 0


def _summarize_timestamp_quality(parsed_timestamps: list):
    valid = [ts for ts in parsed_timestamps if ts is not None]
    invalid_count = len(parsed_timestamps) - len(valid)
    off_interval_count = sum(1 for ts in valid if not _is_five_minute_boundary(ts))

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


def _parse_timestamp(raw: str):
    """Try common NYISO timestamp formats and return a timezone-marked datetime or None.

    Timezone conversion is intentionally skipped — the source local time value is
    preserved as-is (e.g. 5 PM EST is stored as 5 PM, not converted to UTC).
    tzinfo=utc is attached only to satisfy Django's USE_TZ requirement without shifting.
    """
    from datetime import datetime, timezone as dt_timezone

    raw = (raw or "").strip()
    if not raw:
        return None

    for fmt in _TS_FORMATS:
        try:
            dt = datetime.strptime(raw, fmt)
            # Attach UTC marker without any timezone shift — preserves local time value
            return dt.replace(tzinfo=dt_timezone.utc)
        except ValueError:
            continue
    return None


# ---------------------------------------------------------------------------
# Task 1: raw_record stage
# ---------------------------------------------------------------------------

@shared_task(bind=True, max_retries=2, default_retry_delay=30)
def ingest_nyiso_source_file(self, source_file_id: int) -> str:
    """Parse a downloaded NYISO file into raw_record rows, then chain normalization."""
    from core.models import source_file, parse_run, parse_error, raw_record

    src = source_file.objects.filter(pk=source_file_id).first()
    if src is None:
        return f"missing source_file: {source_file_id}"

    run = parse_run.objects.create(
        source_system="nyiso",
        runner_name="ingest_nyiso_source_file",
        context_json={"source_file_id": source_file_id, "file_name": src.source_file_name},
    )

    try:
        rows = _extract_csv_dicts(src.storage_path, src.file_type)
    except Exception as exc:
        run.status = parse_run.RunStatus.FAILED
        run.finished_at = timezone.now()
        run.save(update_fields=["status", "finished_at", "updated_at"])
        parse_error.objects.create(
            parse_run=run,
            source_file=src,
            error_type="extraction_error",
            error_message=str(exc),
        )
        return f"failed extraction: {exc}"

    records_to_create = []
    for i, row in enumerate(rows):
        row_hash = hashlib.sha256(
            json.dumps(row, sort_keys=True, default=str).encode()
        ).hexdigest()
        records_to_create.append(
            raw_record(
                source_file=src,
                parse_run=run,
                row_number=i,
                row_payload_json=row,
                row_hash=row_hash,
            )
        )

    try:
        created = raw_record.objects.bulk_create(records_to_create)
    except Exception as exc:
        run.status = parse_run.RunStatus.FAILED
        run.finished_at = timezone.now()
        run.save(update_fields=["status", "finished_at", "updated_at"])
        parse_error.objects.create(
            parse_run=run,
            source_file=src,
            error_type="raw_record_insert_error",
            error_message=str(exc),
        )
        return f"failed raw_record insert: {exc}"

    run.status = parse_run.RunStatus.COMPLETED
    run.finished_at = timezone.now()
    run.save(update_fields=["status", "finished_at", "updated_at"])

    normalize_nyiso_source_file.delay(source_file_id)

    return f"ok: {len(created)} rows ingested from {src.source_file_name}"


# ---------------------------------------------------------------------------
# Task 2: timeseries_point normalization stage
# ---------------------------------------------------------------------------

@shared_task(bind=True, max_retries=2, default_retry_delay=30)
def normalize_nyiso_source_file(self, source_file_id: int) -> str:
    """Map raw_record rows to timeseries_point by querying column_mapping directly."""
    from core.models import (
        source_file,
        timeseries_point,
        timeseries_batch_audit,
        parse_run,
        parse_error,
        column_mapping,
    )
    from .nyiso_api.nyiso_models import nyiso_report_file

    src = source_file.objects.filter(pk=source_file_id).first()
    if src is None:
        return f"missing source_file: {source_file_id}"

    report_file = (
        nyiso_report_file.objects
        .filter(source_file=src)
        .select_related("nyiso_report")
        .first()
    )
    if report_file is None:
        return "skipped: no nyiso_report_file link"

    report = report_file.nyiso_report
    dataset_key = report.code

    # Get all column_mappings for this report's dataset
    all_mappings = list(
        column_mapping.objects
        .filter(source_system="nyiso", dataset_key=dataset_key, include_in_ingestion=True)
        .select_related("unit_type")
    )
    if not all_mappings:
        return f"skipped: no active column_mappings for {report.code}"

    # Categorize mappings by field name → role
    def _get_role(cm):
        if cm.unit_type_id and cm.unit_type and cm.unit_type.base_data_type == "datetime":
            return "timestamp"
        if cm.unit_type_id:
            return "value"
        if cm.column_label:
            return "dimension"
        return "unmapped"

    ts_mappings = [m for m in all_mappings if _get_role(m) == "timestamp"]
    value_mappings = [m for m in all_mappings if _get_role(m) == "value"]
    dim_mappings = [m for m in all_mappings if _get_role(m) == "dimension"]

    if len(ts_mappings) != 1:
        return f"failed: expected exactly 1 timestamp mapping for {report.code}, found {len(ts_mappings)}"
    ts_mapping = ts_mappings[0]

    if not value_mappings:
        return f"skipped: no value column_mappings for {report.code}"
    if len(dim_mappings) != 1:
        return f"failed: expected exactly 1 dimension mapping for {report.code}, found {len(dim_mappings)}"
    dim_mapping = dim_mappings[0]

    run = parse_run.objects.create(
        source_system="nyiso",
        runner_name="normalize_nyiso_source_file",
        context_json={"source_file_id": source_file_id, "report_code": report.code},
    )

    ts_field = ts_mapping.raw_column

    try:
        batch_size = 50
        batch_index = 0
        attempted_points = 0
        inserted_points = 0
        duplicate_points = 0
        warning_batches = 0
        value_mapping_ids = [m.column_mapping_id for m in value_mappings]

        raw_rows = src.raw_records.all().order_by("row_number").iterator(chunk_size=2000)
        for batch in _iter_in_batches(raw_rows, batch_size=batch_size):
            batch_index += 1
            batch_start_row = batch[0].row_number
            batch_end_row = batch[-1].row_number

            parsed_timestamps = [_parse_timestamp((rr.row_payload_json or {}).get(ts_field, "")) for rr in batch]
            ts_quality = _summarize_timestamp_quality(parsed_timestamps)
            pre_validation_ok = ts_quality["invalid_timestamp_count"] == 0
            if not pre_validation_ok:
                timeseries_batch_audit.objects.create(
                    source_file=src,
                    parse_run=run,
                    report_code=report.code,
                    batch_index=batch_index,
                    batch_start_row=batch_start_row,
                    batch_end_row=batch_end_row,
                    rows_read=len(batch),
                    pre_validation_ok=False,
                    post_validation_ok=False,
                    invalid_timestamp_count=ts_quality["invalid_timestamp_count"],
                    off_interval_count=ts_quality["off_interval_count"],
                    gap_count=ts_quality["gap_count"],
                    status=timeseries_batch_audit.AuditStatus.FAILED,
                    details_json={"error": "invalid timestamps in batch"},
                )
                raise ValueError(
                    f"batch {batch_index} rows {batch_start_row}-{batch_end_row}: "
                    f"found {ts_quality['invalid_timestamp_count']} invalid timestamps"
                )

            points_by_key: dict[tuple[int, object], dict[str, object]] = {}
            batch_attempted_points = 0

            for rr, ts_utc in zip(batch, parsed_timestamps):
                payload = rr.row_payload_json or {}

                dim_val = payload.get(dim_mapping.raw_column)
                if dim_val is None or str(dim_val).strip() == "":
                    raise ValueError(
                        f"row {rr.row_number}: missing dimension value in column '{dim_mapping.raw_column}'"
                    )
                dim_key = str(dim_val).strip()

                for value_m in value_mappings:
                    raw_val = payload.get(value_m.raw_column)
                    if raw_val is None or str(raw_val).strip() == "":
                        continue

                    try:
                        value = Decimal(str(raw_val).replace(",", "").strip())
                    except InvalidOperation as exc:
                        raise ValueError(
                            f"row {rr.row_number}: invalid numeric value '{raw_val}' in column '{value_m.raw_column}'"
                        ) from exc

                    bucket_key = (value_m.column_mapping_id, ts_utc)
                    bucket = points_by_key.setdefault(
                        bucket_key,
                        {
                            "column_mapping": value_m,
                            "ts_utc": ts_utc,
                            "source_row_hash": rr.row_hash,
                            "value_json": {},
                        },
                    )

                    value_json = bucket["value_json"]
                    if dim_key in value_json:
                        raise ValueError(
                            f"row {rr.row_number}: duplicate value for dim_key '{dim_key}' at ts={ts_utc} "
                            f"column_mapping_id={value_m.column_mapping_id}"
                        )

                    value_json[dim_key] = float(value)
                    batch_attempted_points += 1

            points_to_create = [
                timeseries_point(
                    source_file=src,
                    column_mapping=item["column_mapping"],
                    ts_utc=item["ts_utc"],
                    value_json=item["value_json"],
                    source_row_hash=item["source_row_hash"],
                )
                for item in points_by_key.values()
            ]

            with transaction.atomic():
                timeseries_point.objects.bulk_create(
                    points_to_create,
                    update_conflicts=True,
                    update_fields=["source_file", "source_row_hash", "value_json", "updated_at"],
                    unique_fields=["column_mapping", "ts_utc"],
                )

            batch_written_points = len(points_to_create)
            batch_duplicates = max(0, batch_attempted_points - batch_written_points)

            affected_ts = [item["ts_utc"] for item in points_by_key.values()]
            written_count_check = timeseries_point.objects.filter(
                source_file=src,
                column_mapping_id__in=value_mapping_ids,
                ts_utc__in=affected_ts,
            ).count()
            post_validation_ok = written_count_check == batch_written_points

            status = timeseries_batch_audit.AuditStatus.SUCCESS
            if ts_quality["off_interval_count"] > 0 or ts_quality["gap_count"] > 0:
                status = timeseries_batch_audit.AuditStatus.WARNING
                warning_batches += 1
            if not post_validation_ok:
                status = timeseries_batch_audit.AuditStatus.FAILED

            timeseries_batch_audit.objects.create(
                source_file=src,
                parse_run=run,
                report_code=report.code,
                batch_index=batch_index,
                batch_start_row=batch_start_row,
                batch_end_row=batch_end_row,
                rows_read=len(batch),
                points_attempted=batch_attempted_points,
                points_written=batch_written_points,
                duplicates_estimated=batch_duplicates,
                pre_validation_ok=True,
                post_validation_ok=post_validation_ok,
                invalid_timestamp_count=ts_quality["invalid_timestamp_count"],
                off_interval_count=ts_quality["off_interval_count"],
                gap_count=ts_quality["gap_count"],
                status=status,
                details_json={
                    "unique_keys_in_batch": batch_written_points,
                    "post_write_count_check": written_count_check,
                },
            )

            if not post_validation_ok:
                raise ValueError(
                    f"batch {batch_index} rows {batch_start_row}-{batch_end_row}: "
                    f"post-validation failed (expected {batch_written_points} writes, found {written_count_check})"
                )

            attempted_points += batch_attempted_points
            inserted_points += batch_written_points
            duplicate_points += batch_duplicates

        run.status = parse_run.RunStatus.COMPLETED
        run.finished_at = timezone.now()
        run.save(update_fields=["status", "finished_at", "updated_at"])

        return (
            f"ok: attempted={attempted_points} inserted={inserted_points} "
            f"duplicates={duplicate_points} warning_batches={warning_batches} "
            f"report={report.code} file={src.source_file_name}"
        )
    except Exception as exc:
        run.status = parse_run.RunStatus.FAILED
        run.finished_at = timezone.now()
        run.save(update_fields=["status", "finished_at", "updated_at"])
        parse_error.objects.create(
            parse_run=run,
            source_file=src,
            error_type="normalization_error",
            error_message=str(exc),
        )
        return f"failed: {exc}"
