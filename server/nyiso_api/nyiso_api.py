from ninja import NinjaAPI

from .nyiso_api_schema import NyisoReportSchema
from .nyiso_handler import nyiso_handler
from .nyiso_models import nyiso_report

nyiso_app = NinjaAPI(urls_namespace="energy_hub_api", docs_url="/docs")


@nyiso_app.get("report_list/", response=list[NyisoReportSchema])
def get_report_list(request, force_reinsert: bool = False):
    """Return reports and optionally force NYISO table refresh from source data."""
    nyiso_handler(force_reinsert=force_reinsert)
    return nyiso_report.objects.all().order_by("code", "name")
