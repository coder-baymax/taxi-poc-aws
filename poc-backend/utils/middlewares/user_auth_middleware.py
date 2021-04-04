from django.http import HttpResponse


class UserAuthMiddleWare(object):
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        if not request.path_info.startswith("/api/v1") or request.path_info == "/api/v1/auth/login":
            return self.get_response(request)

        session = request.session
        if session.get("userid") is None:
            return HttpResponse("not login", status=401)
        request.user_id = int(session.get("userid"))

        return self.get_response(request)
