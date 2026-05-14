"""
NYISO API - assembles report and schedule routers under a single NinjaAPI instance.
Route implementations live in nyiso_report_router.py and nyiso_schedule_router.py.
"""
from ninja import NinjaAPI

from .nyiso_report_router import report_router
from .nyiso_schedule_router import schedule_router

nyiso_app = NinjaAPI(urls_namespace="energy_hub_api", docs_url="/docs")
nyiso_app.add_router("", report_router)
nyiso_app.add_router("", schedule_router)
