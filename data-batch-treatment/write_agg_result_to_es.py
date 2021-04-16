import json
import os
import time
from datetime import datetime

from elasticsearch import helpers
from elasticsearch_dsl import Document, Date, Keyword, Integer, Double
from elasticsearch_dsl.connections import connections

# for local test
# input_path = "/home/yunfei/aws/agg_result"
# es_host = "http://localhost:19200/"


# for s3 pre-treatment
input_path = "/home/ubuntu/agg_result"
es_host = "https://search-aws-taxi-poc-cntkqynq3uyadf3kmmrl5dnnje.us-east-1.es.amazonaws.com/"


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


def write_agg_result_to_es():
    connections.configure(default={"hosts": [es_host], "timeout": 20})
    DayCounter.init()

    for year in range(2009, 2021):
        for month in range(1, 13):
            month_str = "%d-%02d" % (year, month)
            print(f"START {month_str}")
            bulk_list = []
            for day in range(1, 32):
                date_str = "%s-%02d" % (month_str, day)
                filename = f"{input_path}/{month_str}/{date_str}.json"
                if not os.path.isfile(filename):
                    continue
                with open(filename, "r") as f:
                    for line in f.readlines():
                        bulk_list.append(DayCounter(**json.loads(line)).to_dict(include_meta=True))
            helpers.bulk(connections.get_connection(), bulk_list, chunk_size=5000, request_timeout=60)
            time.sleep(1)
            print(datetime.now())


if __name__ == '__main__':
    write_agg_result_to_es()
