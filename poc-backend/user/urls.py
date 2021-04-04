from django.conf.urls import url

from .views import UserLogin, UserLogout, UserInfo

urlpatterns = [
    url(r"^login$", UserLogin.as_view()),
    url(r"^logout$", UserLogout.as_view()),
    url(r"^info$", UserInfo.as_view()),
]
