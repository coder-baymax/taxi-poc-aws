from elasticsearch_dsl import Document, Date, GeoPoint, Keyword, Long, Integer, Double


class TripRecord(Document):
    vendor_type = Integer()
    passenger_count = Integer()
    trip_distance = Double()
    total_amount = Double()
    pick_up_time = Date()
    drop_off_time = Date()
    duration = Long()
    speed = Double()
    pick_up_location_id = Integer()
    pick_up_location_borough = Keyword()
    pick_up_location = GeoPoint()
    drop_off_location_id = Integer()
    drop_off_location_borough = Keyword()
    drop_off_location = GeoPoint()

    class Index:
        name = "trip_record"
        using = "default"


class DayCounter(Document):
    timestamp = Date()
    vendor_type = Integer()
    location_id = Integer()
    location_borough = Keyword()
    amount_level = Keyword()
    distance_level = Keyword()
    amount_total = Double()
    amount_count = Integer()
    distance_total = Double()
    distance_count = Integer()
    record_count = Integer()

    class Index:
        name = "day_counter"
        using = "default"
