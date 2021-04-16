import json
from collections import defaultdict

import redis
from django.conf import settings
from django.http import JsonResponse
from django.views import View

from dashboard.es_models import TripRecord
from realtime.locations import Locations
from utils.es_utils import EsAggBuilder
from utils.geo_hash import GeoHash
from elasticsearch_dsl import Q

from utils.message_exception import MessageException
from utils.parsed_view import parsed_view
from utils.timestamp_encoder import TimestampEncoder


class AggBaseView(View):
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

    def gen_min_max(self, agg, field):
        return agg.metric_max(field, f"{field}_max").metric_min(field, f"{field}_min")

    def gen_terms(self, agg, field):
        return agg.metric_terms(field, f"{field}_terms")

    def gen_agg(self, **kwargs):
        query = self.gen_query(**kwargs)
        return EsAggBuilder(TripRecord.search().filter(query) if query else TripRecord.search())

    @parsed_view
    def post(self, **kwargs):
        agg_result = {}

        agg = self.gen_agg(**kwargs)
        for field in self.MAX_MIN_FIELDS:
            self.gen_min_max(agg, field)
        for field in self.TERMS_FIELDS:
            self.gen_terms(agg, field)

        for key, value in agg.extract_result(True).items():
            key, tail = key.rsplit("_", 1)
            if key not in agg_result:
                agg_result[key] = {}
            if isinstance(value, list):
                value.sort(key=lambda x: x["key"])
            agg_result[key][tail] = value
        agg_result["trip_count"] = sum(x["doc_count"] for x in agg_result["vendor_type"]["terms"])

        return JsonResponse(agg_result, encoder=TimestampEncoder)


class RecentAggView(AggBaseView):
    MAX_MIN_FIELDS = [
        "pick_up_time",
        "drop_off_time",
        "trip_distance",
        "total_amount",
    ]
    TERMS_FIELDS = [
        "vendor_type",
        "passenger_count",
        "pick_up_location_borough",
        "drop_off_location_borough",
        "pick_up_location_id",
        "drop_off_location_id",
    ]


class RecentView(RecentAggView):

    def bucket(self, agg, field_info):
        if not field_info:
            return None, agg
        field = field_info["field"]
        interval = field_info.get("interval")
        if interval:
            agg.bucket_date_histogram(interval, field)
        else:
            agg.bucket_terms(field)
        return field

    @parsed_view
    def post(self, agg_field_1, agg_field_2, calculate, **kwargs):
        agg = self.gen_agg(**kwargs)
        agg_result = {}

        if not agg_field_1 or not agg_field_1.get("field"):
            raise MessageException("you must choose first aggregation field")
        field_1 = self.bucket(agg, agg_field_1)
        field_2 = self.bucket(agg, agg_field_2)

        print(field_1, field_2)
        if calculate:
            agg.metric_avg(calculate)
        print(agg.extract_result(True))

        return JsonResponse({}, encoder=TimestampEncoder)


class HistoryAggView(View):
    pass


class HistoryView(View):
    pass
