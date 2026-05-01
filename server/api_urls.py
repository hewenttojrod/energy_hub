from django.urls import path

from .nyiso_api.nyiso_api import nyiso_app


urlpatterns = [
    path("", nyiso_app.urls),
]
