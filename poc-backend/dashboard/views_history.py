from collections import defaultdict
from itertools import chain

from django.http import JsonResponse
from django.views import View

from dashboard.es_models import TripRecord, DayCounter
from utils.es_utils import EsAggBuilder
from elasticsearch_dsl import Q

from utils.locations import LOCATION_DICT
from utils.message_exception import MessageException
from utils.parsed_view import parsed_view
from utils.timestamp_encoder import TimestampEncoder


class BaseHistory(View):
    DATE_FIELDS = [
        "timestamp",
    ]
    TERMS_FIELDS = [
        "vendor_type",
        "location_borough",
        "location_id",
        "amount_level",
        "distance_level",
    ]
    FIELD_NAME_DICT = {
        "timestamp": "上车时间",
        "vendor_type": "服务类型",
        "location_borough": "上车区",
        "location_id": "上车地点",
        "distance_level": "车程",
        "amount_level": "总费用",
    }
    FORMAT_DICT = {
        "day": "yyyy-MM-dd",
        "week": "yyyy-ww周",
        "month": "yyyy-MM",
        "quarter": "yyyy-QQQ",
        "year": "yyyy",
    }

    def gen_query(self, **kwargs):
        terms = {}
        ranges = defaultdict(lambda: {})
        for key, value in kwargs.items():
            split = key.split("__")
            if len(split) == 1:
                terms[key] = value
            elif len(split) == 2:
                key, tail = split
                tail = "lte" if tail == "max" else "gte"
                ranges[key][tail] = value

        query = None
        for key, value in terms.items():
            single = Q("term", **{key: value})
            query = single if not query else query & single
        for key, value in ranges.items():
            single = Q("range", **{key: value})
            query = single if not query else query & single

        return query

    def gen_agg(self, **kwargs):
        query = self.gen_query(**kwargs)
        return EsAggBuilder(DayCounter.search().filter(query) if query else DayCounter.search())

    def add_term_names(self, field, value_list):
        if field == "vendor_type":
            for value in value_list:
                value["name"] = "出租车" if value["key"] == 0 else "网约车"
        elif field == "passenger_count":
            for value in value_list:
                value["name"] = f"{value['key']} 位乘客"
        elif field.endswith("location_id"):
            for value in value_list:
                location = LOCATION_DICT.get(value["key"])
                value["name"] = f"{location.borough} {location.zone}" if location else "NA"
        else:
            for value in value_list:
                value["name"] = value["key"]

    def sort_by_key(self, value_list):
        na_item = []
        min_item = []
        other_item = []
        for item in value_list:
            if item["key"] == "NA":
                na_item.append(item)
            elif isinstance(item["key"], str) and item["key"].startswith("<"):
                min_item.append(item)
            else:
                other_item.append(item)
        return [*na_item, *min_item, *sorted(other_item, key=lambda x: x["key"])]


class HistoryAggView(BaseHistory):
    def gen_min_max(self, agg, field):
        return agg.metric_max(field, f"{field}__max").metric_min(field, f"{field}__min")

    def gen_terms(self, agg, field):
        return agg.bucket_terms(field, f"{field}__terms").metric_sum("record_count")

    def get_agg_result(self, **kwargs):
        agg_result = {k: {"name": v} for k, v in self.FIELD_NAME_DICT.items()}
        for key, value in agg_result.items():
            value["file_type"] = "date" if key in self.DATE_FIELDS else "terms"

        agg = self.gen_agg(**kwargs)
        for field in self.DATE_FIELDS:
            self.gen_min_max(agg, field)
        for field in self.TERMS_FIELDS:
            self.gen_terms(agg, field)

        for key, value in agg.extract_result(True).items():
            key, tail = key.rsplit("__", 1)
            if isinstance(value, list):
                value = self.sort_by_key(value)
                self.add_term_names(key, value)
                for item in value:
                    item["doc_count"] = int(item.pop("sum"))
            agg_result[key][tail] = value
        agg_result["trip_count"] = sum(x["doc_count"] for x in agg_result["vendor_type"]["terms"])
        return agg_result

    @parsed_view
    def post(self, **kwargs):
        agg_result = self.get_agg_result(**kwargs)
        return JsonResponse(agg_result, encoder=TimestampEncoder)


class HistoryView(BaseHistory):
    TITLE_DICT = {
        "doc_count": "打车次数",
        "trip_distance__avg": "平均车程",
        "total_amount__avg": "平均费用",
        "duration__avg": "平均时间(秒)",
    }

    def add_range_names(self, interval, value_list):
        for value in value_list:
            value["name"] = f"{value['key']:.2f} ~ {value['key'] + interval:.2f}"

    def add_column_names(self, columns):
        for column in columns:
            key = column["key"]
            if key in self.FIELD_NAME_DICT:
                column["title"] = self.FIELD_NAME_DICT[key]
            elif key in self.TITLE_DICT:
                column["title"] = self.TITLE_DICT[key]
            elif "name" in column:
                column["title"] = column.pop("name")
            else:
                column["title"] = key

    def replace_type_names(self, data):
        for item in data:
            item["type"] = self.FIELD_NAME_DICT[item["type"].split("__")[0]]

    def bucket(self, agg, field_info):
        if not field_info:
            return None, None, agg
        field = field_info["field"]
        interval = field_info.get("interval")
        if field in self.DATE_FIELDS:
            if interval not in self.FORMAT_DICT:
                raise MessageException(f"选择的时间周期必须是：{self.FORMAT_DICT.keys()}")
            interval_value = field_info.get("interval_value")
            if interval_value:
                if interval not in ("minute", "hour", "day"):
                    raise MessageException(f"只有在分钟、小时、天的单位下能指定具体聚合时间")
                return field, "date", agg.bucket_fixed_histogram(f"{interval_value}{interval[0]}", field, "terms",
                                                                 format=self.FORMAT_DICT[interval])
            else:
                return field, "date", agg.bucket_calendar_histogram(interval, field, "terms",
                                                                    format=self.FORMAT_DICT[interval])
        elif field in self.TERMS_FIELDS:
            return field, "terms", agg.bucket_terms(field)
        else:
            raise MessageException("传递了一个系统不支持的聚合字段，请检查")

    def get_table_info(self, field_1, field_2, calculate_fields, agg_result):
        if field_2:
            calculate = calculate_fields[0]
            keys = set()
            data = []
            for row in agg_result:
                single = {'key': row['key'], field_1: row['name']}
                for term in row["terms"]:
                    keys.add(term["key"])
                    single[f"key__{term['key']}"] = term[calculate]
                data.append(single)
            columns = self.sort_by_key([{"dataIndex": x, "key": x} for x in keys])
            self.add_term_names(field_2, columns)
            columns = [{"dataIndex": field_1, "key": field_1}, *columns]
            self.add_column_names(columns)
            for column in columns:
                key = column["key"] if column["key"] == field_1 else f"key__{column['key']}"
                column["dataIndex"] = key
                column["key"] = key
        else:
            columns = [{"dataIndex": x, "key": x} for x in [field_1, *calculate_fields]]
            self.add_column_names(columns)
            data = []
            for row in agg_result:
                row[field_1] = row['key']
                data.append(row)
        return columns, self.sort_by_key(data)

    def get_timeline_info(self, field_1, field_2, calculate_fields, agg_result):
        if field_2:
            calculate = calculate_fields[0]
            data = []
            for row in agg_result:
                for term in row["terms"]:
                    data.append({
                        "date": row["key_as_string"], "time": row["key"],
                        "key": term["key"], "value": term[calculate]
                    })
            self.add_term_names(field_2, data)
            for item in data:
                item["type"] = item.pop("name")
                item.pop("key")
        else:
            data = []
            for row in agg_result:
                for calculate in calculate_fields:
                    data.append({
                        "date": row["key_as_string"], "time": row["key"],
                        "type": calculate, "value": row[calculate]
                    })
            self.replace_type_names(data)
        return data

    def format_term(self, term):
        if "sum" in term:
            term["doc_count"] = term.pop("sum")
        if "distance_count__sum" in term:
            if term["distance_count__sum"] == 0:
                term["trip_distance__avg"] = None
            else:
                term["trip_distance__avg"] = (term.get("distance_total__sum") or 0) / term["distance_count__sum"]
        if "amount_count__sum" in term:
            if term["amount_count__sum"] == 0:
                term["total_amount__avg"] = None
            else:
                term["total_amount__avg"] = (term.get("amount_total__sum") or 0) / term["amount_count__sum"]

    def get_view_result(self, agg_field_1, agg_field_2, calculate_fields, **kwargs):
        agg = self.gen_agg(**kwargs)

        if not agg_field_1 or not agg_field_1.get("field"):
            raise MessageException("必须选择一个一级聚合字段")
        field_1, agg_type, agg = self.bucket(agg, agg_field_1)
        field_2, _, agg = self.bucket(agg, agg_field_2)

        if not calculate_fields or not isinstance(calculate_fields, list):
            raise MessageException("必须提供统计字段")
        elif field_2 and len(calculate_fields) > 1:
            raise MessageException("进行两个维度聚合时，只能选择一个统计字段")

        for calculate in calculate_fields:
            if calculate == "doc_count":
                agg.metric_sum("record_count")
            elif calculate == "trip_distance":
                agg.metric_sum("distance_total", "distance_total__sum")
                agg.metric_sum("distance_count", "distance_count__sum")
            elif calculate == "total_amount":
                agg.metric_sum("amount_total", "amount_total__sum")
                agg.metric_sum("amount_count", "amount_count__sum")
            else:
                raise MessageException("统计字段必须是可计算的字段")
        calculate_fields = [f"{x}__avg" if x != "doc_count" else x for x in calculate_fields]

        view_result = {}
        agg_result = agg.extract_result(True)
        for row in agg_result:
            if row.get("terms"):
                for single in row["terms"]:
                    self.format_term(single)
            else:
                self.format_term(row)

        if agg_type == "terms":
            self.add_term_names(field_1, agg_result)
            columns, data = self.get_table_info(field_1, field_2, calculate_fields, agg_result)
            view_result = {"columns": columns, "data": data}
        elif agg_type == "range":
            self.add_range_names(agg_field_1["interval"], agg_result)
            columns, data = self.get_table_info(field_1, field_2, calculate_fields, agg_result)
            view_result = {"columns": columns, "data": data}
        elif agg_type == "date":
            data = self.get_timeline_info(field_1, field_2, calculate_fields, agg_result)
            view_result = {"data": data}
        return view_result

    @parsed_view
    def post(self, agg_field_1, agg_field_2, calculate_fields, **kwargs):
        view_result = self.get_view_result(agg_field_1, agg_field_2, calculate_fields, **kwargs)
        return JsonResponse(view_result, encoder=TimestampEncoder)
