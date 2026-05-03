from datetime import datetime, timedelta, timezone as dt_timezone

from core._models.base_model import BaseModel
from django.apps import apps
from django.db import models
from django.utils import timezone
from django.contrib.postgres.fields import ArrayField

class nyiso_report(BaseModel):
    '''
        stores meta information about reports from NYISO
        fields:    
            nyiso_id: pk
            code: code for report (ie P-30 for Day-Ahead Market Daily Energy Report)
            name: Name of the report
            frequency: Array field for handling what frequencies the report supports
            file_type: array field of possible file types that can be downloaded
            is_deprecated: true if no new data is being created for report
    '''

    class freq_type(models.TextChoices):
        #         code      desc    
        REAL =    "REAL",   "Real Time"
        HOUR =    "HOUR",   "Hourly"
        DAY =     "DAY",    "Daily"
        YEAR =    "YEAR",   "Yearly"
        SINGLE =  "SINGLE", "Singular"

    class download_type(models.TextChoices):
        #       code    desc    
        HTML =  "HTML", "html"
        CSV =   "CSV", "CSV"
        PDF =   "PDF",  "PDF"
        ZIP =   "ZIP",  "ZIP"

    class report_content_type(models.TextChoices):
        FILE_LIST = "FILE_LIST", "File List"
        INLINE_FEED = "INLINE_FEED", "Inline Feed"

    class parse_state(models.TextChoices):
        OK = "OK", "Parsed"
        PARTIAL = "PARTIAL", "Partial"
        FAILED = "FAILED", "Failed"

    class task_state(models.TextChoices):
        IDLE = "IDLE", "Idle"
        QUEUED = "QUEUED", "Queued"
        RUNNING = "RUNNING", "Running"
        COMPLETED = "COMPLETED", "Completed"
        FAILED = "FAILED", "Failed"

    TASK_RESULT_TO_TASK_STATE = {
        "PENDING": task_state.QUEUED,
        "RECEIVED": task_state.QUEUED,
        "RETRY": task_state.QUEUED,
        "STARTED": task_state.RUNNING,
        "SUCCESS": task_state.COMPLETED,
        "FAILURE": task_state.FAILED,
        "REVOKED": task_state.FAILED,
    }

    code = models.CharField(max_length=25)
    name = models.CharField(max_length=500)
    frequency = ArrayField(
        base_field=models.CharField(
            max_length=20,
            choices=freq_type.choices,
        ),
        default=list,
        blank=True,
    )
    file_type = ArrayField(
        base_field=models.CharField(
            max_length=20,
            choices=download_type.choices,
        ),
        default=list,
        blank=True,
    )
    content_type = models.CharField(
        max_length=20,
        choices=report_content_type.choices,
        default=report_content_type.FILE_LIST,
    )
    source_page = models.CharField(max_length=255, blank=True, default="")
    latest_report_stamp = models.DateTimeField(null=True, blank=True)
    earliest_report_stamp = models.DateTimeField(null=True, blank=True)
    file_name_format = models.CharField(max_length=255, blank=True, default="")
    parse_status = models.CharField(
        max_length=20,
        choices=parse_state.choices,
        default=parse_state.PARTIAL,
    )
    task_status = models.CharField(
        max_length=20,
        choices=task_state.choices,
        default=task_state.IDLE,
    )
    active_task_id = models.CharField(max_length=64, blank=True, default="")
    task_updated_at = models.DateTimeField(null=True, blank=True)
    last_scanned_at = models.DateTimeField(null=True, blank=True)
    is_deprecated = models.BooleanField(default=False)


    def __str__(self) -> str:
        return f'{self.code} - {self.name}'

    def get_page_cache_max_age(self) -> timedelta | None:
        """Return cache freshness window for report page pulls; None means never refresh."""
        if self.is_deprecated:
            return None

        if self.content_type == type(self).report_content_type.INLINE_FEED:
            return timedelta(hours=1)

        primary_frequency = self.frequency[0] if self.frequency else None
        if primary_frequency == self.freq_type.SINGLE:
            return None
        if primary_frequency in {self.freq_type.REAL, self.freq_type.HOUR}:
            return timedelta(hours=1)
        if primary_frequency == self.freq_type.DAY:
            return timedelta(days=1)

        return timedelta(days=1)

    @classmethod
    def from_db(cls, db, field_names, values):
        """Revalidate queue-backed task status whenever a row is materialized from DB."""
        instance = super().from_db(db, field_names, values)
        instance.revalidate_task_status_on_pull()
        return instance

    def revalidate_task_status_on_pull(self) -> None:
        """Sync QUEUED/RUNNING statuses with celery task state to prevent stale active flags."""
        if self.task_status not in {self.task_state.QUEUED, self.task_state.RUNNING}:
            return

        update_fields: list[str] = []
        now = timezone.now()
        task_id = (self.active_task_id or "").strip()

        if not task_id:
            if self.task_status != self.task_state.FAILED:
                self.task_status = self.task_state.FAILED
                update_fields.append("task_status")
            self.task_updated_at = now
            update_fields.append("task_updated_at")
            self.save(update_fields=[*update_fields, "updated_at"])
            return

        task_result_model = apps.get_model("django_celery_results", "TaskResult")
        task_result = task_result_model.objects.filter(task_id=task_id).values_list("status", flat=True).first()

        if task_result is None:
            # Celery result rows may not exist immediately after queueing.
            # Avoid false FAILED transitions for fresh tasks.
            reference_time = self.task_updated_at or self.updated_at or now
            if now - reference_time <= timedelta(minutes=15):
                return

            if self.task_status != self.task_state.FAILED:
                self.task_status = self.task_state.FAILED
                update_fields.append("task_status")
            if self.active_task_id:
                self.active_task_id = ""
                update_fields.append("active_task_id")
            self.task_updated_at = now
            update_fields.append("task_updated_at")
            self.save(update_fields=[*update_fields, "updated_at"])
            return

        mapped_state = self.TASK_RESULT_TO_TASK_STATE.get(str(task_result), self.task_status)
        if mapped_state != self.task_status:
            self.task_status = mapped_state
            update_fields.append("task_status")

        if mapped_state in {self.task_state.COMPLETED, self.task_state.FAILED} and self.active_task_id:
            self.active_task_id = ""
            update_fields.append("active_task_id")

        if update_fields:
            self.task_updated_at = now
            update_fields.append("task_updated_at")
            self.save(update_fields=[*update_fields, "updated_at"])

    def set_deprecation_from_latest(self, latest_stamp: datetime) -> None:
        """Set is_deprecated from latest report timestamp and this instance's frequency."""
        now_utc = timezone.now()
        latest_utc = latest_stamp.replace(tzinfo=dt_timezone.utc)
        age = now_utc - latest_utc
        primary_frequency = self.frequency[0] if self.frequency else None

        if primary_frequency == self.freq_type.REAL:
            self.is_deprecated = age > timedelta(days=2)
            
        elif primary_frequency == self.freq_type.HOUR:
            self.is_deprecated = age > timedelta(days=7)
            
        elif primary_frequency == self.freq_type.DAY:
            self.is_deprecated = age > timedelta(days=45)
            
        elif primary_frequency == self.freq_type.YEAR:
            self.is_deprecated = age > timedelta(days=730)
        
        else:
            self.is_deprecated = age > timedelta(days=180)

    def save(self, *args, **kwargs):
        """Persist record and, when provided, compute deprecation from latest stamp."""
        latest_stamp = kwargs.pop("latest_stamp", None)
        if latest_stamp is not None:
            self.set_deprecation_from_latest(latest_stamp)
            self.latest_report_stamp = latest_stamp
        elif self.latest_report_stamp is not None:
            self.set_deprecation_from_latest(self.latest_report_stamp)
        return super().save(*args, **kwargs)