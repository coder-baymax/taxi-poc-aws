from django.conf.urls import url

from dashboard.views_recent import RecentAggView, RecentView
from dashboard.views_history import HistoryAggView, HistoryView

urlpatterns = [
    url(r"^recent/agg$", RecentAggView.as_view()),
    url(r"^recent/view$", RecentView.as_view()),
    url(r"^history/agg$", HistoryAggView.as_view()),
    url(r"^history/view$", HistoryView.as_view()),
]
