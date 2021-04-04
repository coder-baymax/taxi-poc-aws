from django.db import transaction
from django.http import JsonResponse
from django.views import View

from user.models import User
from utils.message_exception import MessageException
from utils.parsed_view import parsed_view
from utils.timestamp_encoder import TimestampEncoder


class UserLogin(View):
    def login_session_save(self, session, user):
        session["userid"] = user.id
        session.save()
        user.session_id = session.session_key
        user.save()

    @transaction.atomic
    def post(self, request):
        session = request.session
        username = request.data.get("username")
        password = request.data.get("password")

        user = User.objects.auth(username, password)
        if user is None:
            raise MessageException("Username or password is wrong!")

        self.login_session_save(session, user)
        return JsonResponse(user.json, encoder=TimestampEncoder)


class UserLogout(View):
    def post(self, request):
        request.session.clear()
        response = JsonResponse({"success": True})
        for k in request.COOKIES:
            response.delete_cookie(k)
        return response


class UserInfo(View):

    @parsed_view
    def post(self, user_id):
        user = User.objects.filter(id=user_id).first()
        return JsonResponse(user.json, encoder=TimestampEncoder)
