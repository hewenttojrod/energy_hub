from datetime import datetime, timedelta, timezone as dt_timezone

from core._models.base_model import BaseModel
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
        #       code    desc    
        REAL =  "REAL", "Real Time"
        HOUR =  "HOUR", "Hourly"
        DAY =   "DAY",  "Daily"
        YEAR =  "YEAR", "Yearly"

    class download_type(models.TextChoices):
        #       code    desc    
        HTML =  "HTML", "html"
        CSV =   "CSV", "CSV"
        PDF =   "PDF",  "PDF"

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
    is_deprecated = models.BooleanField(default=False)


    def __str__(self) -> str:
        return f'{self.code} - {self.name}'

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
        return super().save(*args, **kwargs)