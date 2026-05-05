"""Module registration metadata for bookstore."""

from src.registry.module_registration import ModuleRegistration


MODULE_REGISTRATION = ModuleRegistration(
    name="energy_hub",
    app_config="server.apps.EnergyHubConfig",
    urls="server.urls",
    api_router="server.api_urls",
    schedule_task="server.tasks.process_nyiso_report_schedules",
    schedule_interval_seconds=60.0,
)
