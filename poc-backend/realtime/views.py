import redis
from django.conf import settings
from django.http import JsonResponse
from django.views import View

from utils.locations import LOCATIONS
from utils.geo_hash import GeoHash
from utils.parsed_view import parsed_view
from utils.timestamp_encoder import TimestampEncoder


def gen_ant(features):
    def gen_feature(lat, lng, timestamp, count, **kwargs):
        return {
            "type": "Feature",
            "properties": {
                "mag": count,
                "time": timestamp,
            },
            "geometry": {
                "type": "Point",
                "coordinates": [
                    lng,
                    lat
                ]
            }
        }

    ant_result = {
        "type": "FeatureCollection",
        "crs": {
            "type": "name",
            "properties": {
                "name": "urn:ogc:def:crs:OGC:1.3:CRS84"
            }
        }, "features": [gen_feature(**x) for x in features]
    }
    return ant_result


class DriverCountView(View):
    HEADER_MAPPING = {
        "current": "DCC",
        "last": "DLC",
        "predict": "DPC",
    }

    def get_counter(self, lat, lng):
        geo_bits = GeoHash.from_lat_lng(lat, lng, 32).bits >> 32
        geo_range = geo_bits - (1 << 11), geo_bits + (1 << 11)
        r = redis.Redis(connection_pool=settings.REDIS_POOL)

        geo_hash_list = r.zrangebyscore("driver_locations", *geo_range)
        geo_list = [GeoHash.from_bits(int(x, 16) << 32, 32) for x in geo_hash_list]

        counter_dict = {}
        now_max_time = 0
        for data_type, header in self.HEADER_MAPPING.items():
            counter_dict[data_type] = []
            keys = [f"{header}&&{x}" for x in geo_hash_list]
            for i, item in (x for x in enumerate(r.mget(keys)) if x[-1]):
                point = geo_list[i].point
                count, timestamp = item.split('&&')
                counter_dict[data_type].append({
                    "count": float(count), "timestamp": int(timestamp), "lat": point.lat, "lng": point.lng
                })
            now_max_time = max(now_max_time, max(x["timestamp"] for x in counter_dict[data_type]))
            counter_dict[data_type] = [x for x in counter_dict[data_type] if x["timestamp"] > now_max_time - 60_000]

        newest_time = int(r.get(settings.REDIS_TIME_KEY) or 0)
        counter_dict["timestamp_info"] = {"newest": newest_time, "now_max": now_max_time}
        return counter_dict

    @parsed_view
    def post(self, lat, lng):
        return JsonResponse(self.get_counter(lat, lng), encoder=TimestampEncoder)


class DriverCountAntView(DriverCountView):

    @parsed_view
    def post(self, lat, lng):
        counter = self.get_counter(lat, lng)
        for data_type, _ in self.HEADER_MAPPING.items():
            counter[data_type] = gen_ant(counter[data_type])
        return JsonResponse(counter, encoder=TimestampEncoder)


class LocationsView(View):
    @parsed_view
    def post(self):
        return JsonResponse({"locations": [x.json for x in LOCATIONS]}, encoder=TimestampEncoder)


class OperatorCountView(View):
    HEADER_MAPPING = {
        "current": "OCC",
        "last": "OLC",
        "predict": "OPC",
    }

    def get_counter(self):
        r = redis.Redis(connection_pool=settings.REDIS_POOL)

        counter_dict = {}
        now_max_time = 0
        for data_type, header in self.HEADER_MAPPING.items():
            counter_dict[data_type] = []
            keys = [f"{header}&&{x.location_id}" for x in LOCATIONS]
            for i, item in (x for x in enumerate(r.mget(keys)) if x[-1]):
                location = LOCATIONS[i]
                count, timestamp = item.split('&&')
                counter_dict[data_type].append({
                    "count": float(count), "timestamp": int(timestamp), **location.json
                })
            now_max_time = max(now_max_time, max(x["timestamp"] for x in counter_dict[data_type]))
            counter_dict[data_type] = [x for x in counter_dict[data_type] if x["timestamp"] > now_max_time - 60_000]

        newest_time = int(r.get(settings.REDIS_TIME_KEY) or 0)
        counter_dict["timestamp_info"] = {"newest": newest_time, "now_max": now_max_time}
        return counter_dict

    @parsed_view
    def post(self):
        return JsonResponse(self.get_counter(), encoder=TimestampEncoder)


class OperatorCountAntView(OperatorCountView):

    @parsed_view
    def post(self):
        counter = self.get_counter()
        for data_type, _ in self.HEADER_MAPPING.items():
            counter[data_type] = gen_ant(counter[data_type])
        return JsonResponse(counter, encoder=TimestampEncoder)
