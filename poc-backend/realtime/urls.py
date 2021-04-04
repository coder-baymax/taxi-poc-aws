from django.conf.urls import url

from realtime.views import DriverCountView, LocationsView, OperatorCountView, DriverCountAntView, OperatorCountAntView

urlpatterns = [
    url(r"^driver_view/counter$", DriverCountView.as_view()),
    url(r"^driver_view/counter/ant$", DriverCountAntView.as_view()),
    url(r"^operator_view/locations$", LocationsView.as_view()),
    url(r"^operator_view/counter$", OperatorCountView.as_view()),
    url(r"^operator_view/counter/ant$", OperatorCountAntView.as_view()),
]
