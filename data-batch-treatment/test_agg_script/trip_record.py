import re
from collections import OrderedDict
from datetime import datetime
from locations import Locations


class TripRecord:
    fields = OrderedDict((
        ("vendor_type", int),
        ("passenger_count", int),
        ("total_amount", float),
        ("trip_distance", float),
        ("pickup_datetime", datetime),
        ("pickup_latitude", float),
        ("pickup_longitude", float),
        ("dropoff_datetime", datetime),
        ("dropoff_latitude", float),
        ("dropoff_longitude", float),))

    def __init__(self, header, line):
        value = {k: line[i].strip('" ') for i, k in enumerate(header)}
        self.vendor_type = self.parse_int(value.get("vendor_type"))
        self.passenger_count = self.parse_int(value.get("passenger_count"))
        self.total_amount = self.parse_float(value.get("total_amount"))
        self.trip_distance = self.parse_float(value.get("trip_distance"))
        self.pickup_datetime = self.parse_datetime(value.get("pickup_datetime"))
        self.pickup_latitude = self.parse_float(value.get("pickup_latitude"))
        self.pickup_longitude = self.parse_float(value.get("pickup_longitude"))
        # self.dropoff_datetime = self.parse_datetime(value.get("dropoff_datetime"))
        # self.dropoff_latitude = self.parse_float(value.get("dropoff_latitude"))
        # self.dropoff_longitude = self.parse_float(value.get("dropoff_longitude"))

    @classmethod
    def parse_int(cls, num):
        try:
            return int(num)
        except (ValueError, TypeError):
            return None

    @classmethod
    def parse_float(cls, num):
        try:
            return float(num)
        except (ValueError, TypeError):
            return None

    @classmethod
    def parse_datetime(cls, num):
        time_split = re.findall(r"(\d{4})-(\d{1,2})-(\d{1,2}) \d{1,2}:\d{1,2}:\d{1,2}", num)
        return tuple(int(x) for x in time_split[0]) if time_split else None

    @property
    def json(self):
        return {
            "vendor_type": self.vendor_type,
            "passenger_count": self.passenger_count,
            "total_amount": self.total_amount,
            "trip_distance": self.trip_distance,
            "pickup_latitude": self.pickup_latitude,
            "pickup_longitude": self.pickup_longitude,
        }


class Pretty:
    def __init__(self, record: TripRecord):
        self.vendor_type = record.vendor_type
        self.timestamp = int(datetime(*record.pickup_datetime).timestamp() * 1000) if record.pickup_datetime else None

        if record.pickup_latitude and record.pickup_longitude:
            location = self.get_closest(record.pickup_latitude, record.pickup_longitude)
            self.location_id = location.location_id
            self.location_borough = location.borough
        else:
            self.location_id = -1
            self.location_borough = "NA"

        if record.total_amount:
            self.amount_level = "<20" if record.total_amount < 20 else "20-40" if record.total_amount <= 40 else ">40"
            self.amount_total = record.total_amount
            self.amount_count = 1
        else:
            self.amount_level = "NA"
            self.amount_total = 0
            self.amount_count = 0

        if record.trip_distance:
            self.distance_level = "<10" if record.trip_distance < 10 else "10-20" if record.trip_distance <= 20 else ">20"
            self.distance_total = record.trip_distance
            self.distance_count = 1
        else:
            self.distance_level = "NA"
            self.distance_total = 0
            self.distance_count = 0

    @property
    def key_value(self):
        return (self.timestamp, self.vendor_type, self.location_id, self.location_borough, self.amount_level,
                self.distance_level), \
               [self.amount_total, self.amount_count, self.distance_total, self.distance_count, 1]

    @classmethod
    def get_closest(cls, lat, lng):
        min_loc, min_value = None, 0xffffffff
        for location in Locations:
            value = (lat - location.lat) ** 2 + (lng - location.lng) ** 2
            if value < min_value:
                min_loc = location
                min_value = value
        return min_loc
