from elasticsearch_dsl import Document, Date, GeoPoint, Keyword, Long, Integer, Double


class TripRecord(Document):
    vendor_type = Integer()
    passenger_count = Integer()
    trip_distance = Double()
    total_amount = Double()
    pick_up_time = Date()
    drop_off_time = Date()
    duration = Long()
    pick_up_location_id = Integer()
    pick_up_location_borough = Keyword()
    pick_up_location = GeoPoint()
    drop_off_location_id = Integer()
    drop_off_location_borough = Keyword()
    drop_off_location = GeoPoint()

    class Index:
        name = "trip_record"
        using = "default"
