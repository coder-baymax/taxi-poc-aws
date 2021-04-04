import functools
import inspect

from django.db.models import QuerySet
from django.http import JsonResponse
from utils.timestamp_encoder import TimestampEncoder

PAGING_ARGS = ["page_size", "page_num", "need_count", "order"]


class PagingEncoder:
    def __init__(self, args_dict, paging_dict):
        self.args_dict = args_dict
        self.paging_dict = paging_dict

    def json_item(self, item):
        return item.json

    def json_result(self, result: dict, query_set: QuerySet):
        result["items"] = [self.json_item(x) for x in result["items"]]
        return result

    def paging_result(self, query_set: QuerySet):
        page_size = self.paging_dict.get("page_size") or 100
        page_num = self.paging_dict.get("page_num") or 1
        page_start = (page_num - 1) * page_size
        page_end = page_num * page_size
        result = {"page_size": page_size, "page_num": page_num, "items": []}
        if self.paging_dict.get("need_count"):
            result["total_count"] = (
                len(query_set) if isinstance(query_set, list) else query_set.count()
            )
        if self.paging_dict.get("order") and isinstance(query_set, QuerySet):
            query_set = query_set.order_by(*self.paging_dict["order"])
        result["items"] = list(query_set[page_start:page_end])
        return JsonResponse(self.json_result(result, query_set), encoder=TimestampEncoder)


def parse_from_request(func, request):
    signature = inspect.signature(func)
    parameters = set(x for x in signature.parameters if x != "self")
    kwargs_param = next(
        filter(
            lambda x: signature.parameters[x].kind == inspect._VAR_KEYWORD,
            signature.parameters,
        ),
        None,
    )

    params_dict = {k: v for k, v in request.data.items()}
    args_dict = {x: params_dict.pop(x, None) for x in parameters if x != kwargs_param}
    paging_dict = {x: params_dict.pop(x, None) for x in PAGING_ARGS}
    if "user_id" in parameters:
        args_dict["user_id"] = request.user_id
    args_dict.update(params_dict if kwargs_param is not None else {})
    return args_dict, paging_dict


def parsed_view(func):
    @functools.wraps(func)
    def wrapper(self, request):
        args_dict, _ = parse_from_request(func, request)
        return func(self, **args_dict)

    return wrapper


def parsed_paging_view(paging_class=PagingEncoder):
    def decorate(func):
        @functools.wraps(func)
        def wrapper(self, request):
            args_dict, paging_dict = parse_from_request(func, request)
            json_obj = paging_class(args_dict, paging_dict)
            func_result = func(self, **args_dict)
            return json_obj.paging_result(func_result)

        return wrapper

    return decorate
