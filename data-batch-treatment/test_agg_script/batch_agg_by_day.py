import json
import os
import re
from collections import defaultdict
from concurrent.futures.process import ProcessPoolExecutor
from datetime import datetime

import boto3

from trip_record import TripRecord, Pretty

bucket = "taxi-poc-formatted"
output_path = "/home/ubuntu/agg_result"


def insert_record_dict(record_dict, key, value):
    if key in record_dict:
        for i, num in enumerate(record_dict[key]):
            record_dict[key][i] += value[i]
    else:
        record_dict[key] = value


def get_records(file_name):
    s3 = boto3.client("s3")
    header = None
    record_dict = {}
    for line in s3.get_object(Bucket=bucket, Key=file_name)["Body"].iter_lines():
        if not header:
            header = line.decode("utf8").split(",")
        else:
            pretty = Pretty(TripRecord(header, line.decode("utf8").split(",")))
            if pretty.timestamp:
                key, value = pretty.key_value
                insert_record_dict(record_dict, key, value)

    return record_dict


def agg_by_day(month_str, date_str, file_names):
    record_dict = {}
    print(file_names)
    with ProcessPoolExecutor(max_workers=32) as executor:
        for single in executor.map(get_records, file_names):
            for key, value in single.items():
                insert_record_dict(record_dict, key, value)
    print(datetime.now())
    with open(f"{output_path}/{month_str}/{date_str}.json", "w") as f:
        for key, value in sorted(record_dict.items()):
            json_info = {
                "timestamp": key[0],
                "vendor_type": key[1],
                "location_id": key[2],
                "location_borough": key[3],
                "amount_level": key[4],
                "distance_level": key[5],
                "amount_total": value[0],
                "amount_count": value[1],
                "distance_total": value[2],
                "distance_count": value[3],
                "record_count": value[4],
            }
            f.write(f"{json.dumps(json_info)}\n")


def batch_agg_by_day():
    s3 = boto3.client("s3")
    for year in range(2009, 2021):
        for month in range(1, 13):
            month_str = "%d-%02d" % (year, month)
            print(month_str, "START")
            date_file_dict = defaultdict(lambda: [])
            for file_info in s3.list_objects(Bucket=bucket, Prefix=month_str)["Contents"]:
                date = re.findall(r"date=(\d{4}-\d{2}-\d{2})", file_info["Key"])
                if not date:
                    continue
                date_file_dict[date[0]].append(file_info["Key"])

            print("KEYS ", date_file_dict.keys())
            if not os.path.exists(f"{output_path}/{month_str}"):
                os.makedirs(f"{output_path}/{month_str}")
            for day in range(1, 32):
                date_str = "%s-%02d" % (month_str, day)
                if os.path.isfile(f"{output_path}/{month_str}/{date_str}.json"):
                    continue
                if date_str in date_file_dict:
                    agg_by_day(month_str, date_str, date_file_dict[date_str])


if __name__ == '__main__':
    batch_agg_by_day()
