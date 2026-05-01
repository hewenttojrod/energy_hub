from ninja import ModelSchema

from .nyiso_models import nyiso_report


class NyisoReportSchema(ModelSchema):
    class Meta:
        model = nyiso_report
        fields = "__all__"
