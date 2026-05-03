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
