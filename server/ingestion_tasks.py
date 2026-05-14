import hashlib
import json
from decimal import Decimal, InvalidOperation

from celery import shared_task
from django.db import transaction
from django.utils import timezone

from .ingestion_helpers import extract_csv_dicts, iter_in_batches, parse_timestamp, summarize_timestamp_quality


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

        rows = extract_csv_dicts(src.storage_path, src.file_type)
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

    all_mappings = list(
        column_mapping.objects
        .filter(source_system="nyiso", dataset_key=dataset_key, include_in_ingestion=True)
        .select_related("unit_type")
    )
    if not all_mappings:
        return f"skipped: no active column_mappings for {report.code}"



    ts_mappings = [m for m in all_mappings if m.get_role() == "timestamp"]
    value_mappings = [m for m in all_mappings if m.get_role() == "value"]
    dim_mappings = [m for m in all_mappings if m.get_role() == "dimension"]

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

    raw_rows = src.raw_records.all().order_by("row_number").iterator(chunk_size=2000)
    try:
        batch_size = 50
        validation_check_every_batches = 5
        batch_index = 0
        attempted_points = 0
        inserted_points = 0
        duplicate_points = 0
        warning_batches = 0
        value_mapping_ids = [m.column_mapping_id for m in value_mappings]

        # Initialize points_by_key ONCE, outside batch loop, so records with same
        # (column_mapping_id, ts_utc) are aggregated across ALL batches.
        # Track which keys are NEW each batch to avoid re-writing already-written points.
        points_by_key: dict[tuple[int, object], dict[str, object]] = {}
        previously_written_keys: set[tuple[int, object]] = set()

        for batch in iter_in_batches(raw_rows, batch_size=batch_size):
            batch_index += 1
            batch_start_row = batch[0].row_number
            batch_end_row = batch[-1].row_number

            parsed_timestamps = [parse_timestamp((rr.row_payload_json or {}).get(ts_field, "")) for rr in batch]
            ts_quality = summarize_timestamp_quality(parsed_timestamps)
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

            batch_attempted_points = 0
            batch_newly_added_keys: set[tuple[int, object]] = set()

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
                    is_new_key = bucket_key not in points_by_key
                    
                    bucket = points_by_key.setdefault(
                        bucket_key,
                        {
                            "column_mapping": value_m,
                            "ts_utc": ts_utc,
                            "value_json": {},
                        },
                    )

                    if is_new_key:
                        batch_newly_added_keys.add(bucket_key)

                    value_json = bucket["value_json"]
                    if dim_key in value_json:
                        raise ValueError(
                            f"row {rr.row_number}: duplicate value for dim_key '{dim_key}' at ts={ts_utc} "
                            f"column_mapping_id={value_m.column_mapping_id}"
                        )

                    value_json[dim_key] = float(value)
                    batch_attempted_points += 1

            # Only create points from NEWLY ADDED keys in this batch, not all of points_by_key
            points_to_create = [
                timeseries_point(
                    source_file=src,
                    column_mapping=points_by_key[key]["column_mapping"],
                    ts_utc=points_by_key[key]["ts_utc"],
                    value_json=points_by_key[key]["value_json"],
                )
                for key in batch_newly_added_keys
            ]

            with transaction.atomic():
                timeseries_point.objects.bulk_create(
                    points_to_create,
                    update_conflicts=True,
                    update_fields=["source_file", "value_json", "updated_at"],
                    unique_fields=["column_mapping", "ts_utc"],
                )
                previously_written_keys.update(batch_newly_added_keys)

            batch_written_points = len(points_to_create)
            batch_duplicates = max(0, batch_attempted_points - batch_written_points)

            should_run_post_validation = (batch_index % validation_check_every_batches) == 0
            written_count_check = batch_written_points
            post_validation_ok = True
            if should_run_post_validation:
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
                    "post_write_count_check_skipped": not should_run_post_validation,
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
