from ninja import ModelSchema, Schema

from .nyiso_models import nyiso_report


class NyisoReportSchema(ModelSchema):
    class Meta:
        model = nyiso_report
        fields = "__all__"


class NyisoTaskStartSchema(Schema):
    queued_report_ids: list[int]
    active_report_ids: list[int]
    queued_count: int
    active_count: int
    cursor: str


class NyisoTaskPollSchema(Schema):
    active_report_ids: list[int]
    finished_report_ids: list[int]
    active_count: int
    finished_count: int
    cursor: str


class NyisoReportRefreshActionSchema(Schema):
    report_id: int
    task_id: str
    queued: bool
    message: str


class NyisoRefreshAllReportsSchema(Schema):
    total_reports: int
    queued_count: int
    already_active_count: int
    queued_report_ids: list[int]
    cache_bypassed: bool
    async_mode: bool
    completed_count: int | None = None
    failed_count: int | None = None
    message: str


class NyisoScheduleCreateRequestSchema(Schema):
    name: str
    mode: str = "METADATA_REFRESH"
    report_id: int | None = None
    is_active: bool = True
    interval_minutes: int = 1440
    use_cache: bool = True
    run_async: bool = True
    start_date: str | None = None
    end_date: str | None = None
    rolling_window_days: int | None = None
    template_values_json: dict[str, str] | None = None


class NyisoScheduleSchema(Schema):
    nyiso_report_schedule_id: int
    name: str
    mode: str
    report_id: int | None = None
    report_code: str | None = None
    report_name: str | None = None
    is_active: bool
    interval_minutes: int
    use_cache: bool
    run_async: bool
    start_date: str | None = None
    end_date: str | None = None
    rolling_window_days: int | None = None
    template_values_json: dict[str, str]
    next_run_at: str | None = None
    last_run_at: str | None = None
    last_state: str
    last_message: str


class NyisoScheduleActionResponseSchema(Schema):
    schedule_id: int
    ok: bool
    message: str
    task_id: str | None = None


class NyisoScheduleTestCallSchema(Schema):
    task_name: str
    report_id: int | None = None
    report_code: str | None = None
    run_date: str | None = None
    force_refresh: bool | None = None
    run_async: bool | None = None
    matched_href: str | None = None
    resolved_url: str | None = None
    found_on_page: bool | None = None
    note: str = ""


class NyisoScheduleTestResponseSchema(Schema):
    valid: bool
    message: str
    resolved_mode: str
    report_count: int
    estimated_call_count: int
    calls: list[NyisoScheduleTestCallSchema]


class NyisoScheduleRunSchema(Schema):
    schedule_run_id: int
    state_value: str
    triggered_by: str
    celery_task_id: str
    started_at: str | None = None
    finished_at: str | None = None
    records_targeted: int
    files_downloaded: int
    completed_count: int
    failed_count: int
    message: str
    created_at: str | None = None


class NyisoReportFileResolveRequestSchema(Schema):
    report_id: int
    template_values: dict[str, str] | None = None
    download: bool = False


class NyisoReportFileResolveResponseSchema(Schema):
    report_id: int
    built_file_name: str | None = None
    exists_on_report_page: bool
    requested_file_name_candidates: list[str]
    matched_href: str | None = None
    resolved_url: str | None = None
    downloaded: bool = False
    local_path: str | None = None
    source_file_id: int | None = None
    checksum: str | None = None
    file_type: str | None = None
    size_bytes: int | None = None
    created: bool = False
