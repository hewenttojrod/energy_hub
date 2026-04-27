"""Module registration metadata for bookstore."""

from src.registry.module_registration import ModuleRegistration


MODULE_REGISTRATION = ModuleRegistration(
    name="module_template",
    app_config="server.apps.ModuleTemplateConfig",
    urls="server.urls",
    api_router="server.api_urls",
)
