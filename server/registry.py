"""Module registration metadata for bookstore."""

from src.registry.module_registration import ModuleRegistration


MODULE_REGISTRATION = ModuleRegistration(
    name="energy_hub",
    app_config="server.apps.EnergyHubConfig",
    urls="server.urls",
    api_router="server.api_urls",
)
