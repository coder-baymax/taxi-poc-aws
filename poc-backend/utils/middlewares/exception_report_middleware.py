import logging
import traceback

from django.http import JsonResponse

logger = logging.getLogger("middleware")


class ExceptionReportMiddleware(object):
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        response = self.get_response(request)
        return response

    def process_exception(self, request, exception):
        msg = "服务器发生了一个未捕获的异常，喊后端来查bug"
        if hasattr(exception, "message"):
            msg = exception.message
        stack_info = traceback.format_exc()
        logger.error(
            f"""
            Exception is thrown for request: {request.path}
            Stacktrace: {stack_info},
            Exception: {exception}
            """
        )
        return JsonResponse({"msg": msg}, status=200)
