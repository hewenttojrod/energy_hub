import hashlib
import json
from decimal import Decimal, InvalidOperation

from celery import shared_task
from django.db import transaction
from django.utils import timezone

from ...ingestion_helpers import extract_csv_dicts, iter_in_batches, parse_timestamp, summarize_timestamp_quality



def normalize_nyiso_source_file(source_file_id: int) -> str:
    """Map raw_record rows to timeseries_point by querying column_mapping directly."""
    from core.models import (
        source_file,
        raw_record,
        timeseries_point,
        timeseries_batch_audit,
        parse_run,
        parse_error,
        column_mapping,
    )
    from ...nyiso_api.nyiso_models import nyiso_report_file
    from django.db.models import F, Func, JSONField, TextField
    from django.db.models.functions import Cast
    from django.db.models import Aggregate

    class JSONBAgg(Aggregate):
        function = "jsonb_agg"
        name = "JSONBAgg"
        output_field = JSONField()

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


    mapping = {

    
        "timestamp":[m for m in all_mappings if m.get_role() == "timestamp"],
        "value": [m for m in all_mappings if m.get_role() == "value"],
        "dimension": [m for m in all_mappings if m.get_role() == "dimension"],
    }

    if len(mapping["timestamp"]) != 1:
        return f"failed: expected exactly 1 timestamp mapping for {report.code}, found {len(mapping["timestamp"])}"
    ts_mapping = mapping["timestamp"][0]

    #fail if multiple dimensions, but leave as an array so future enhancements can happen
    if not mapping["value"]:
        return f"skipped: no value column_mappings for {report.code}"
    if len(mapping["dimension"]) != 1:
        return f"failed: expected exactly 1 dimension mapping for {report.code}, found {len(mapping["dimension"])}"

    run = parse_run.objects.create(
        source_system="nyiso",
        runner_name="normalize_nyiso_source_file",
        context_json={"source_file_id": source_file_id, "report_code": report.code},
    )

    ts_field = ts_mapping.raw_column

    grouped_raw_rows = (
        raw_record.objects.all()
        .values(timestamp_key=F('row_payload_json__Time Stamp'))
        .annotate(aggr_data=JSONBAgg('row_payload_json'))
        .order_by('timestamp_key')
        .iterator(chunk_size=2000)
    )
    try:
        timeseries_batch: list[timeseries_point] = []
        i = 0
        for record in grouped_raw_rows:
            #print(record)
            i += 1
            #build timeseries point
            point = timeseries_point(
                source_file=src,
                
            )
        print(i)
        # https://stackoverflow.com/questions/27047630/django-batching-bulk-update-or-create bulk-create update_conflict=true
    except Exception as exc:
        print(f"oops {exc}")
    
    return "x"
