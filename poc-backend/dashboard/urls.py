from django.conf.urls import url

from dashboard.views import RecentAggView, RecentView, HistoryAggView, HistoryView

urlpatterns = [
    url(r"^recent/agg$", RecentAggView.as_view()),
    url(r"^recent/view$", RecentView.as_view()),
    url(r"^history/agg$", HistoryAggView.as_view()),
    url(r"^history/view$", HistoryView.as_view()),
]
