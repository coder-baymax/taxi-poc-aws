import json
import re
from collections import defaultdict

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import DoubleType, StringType

# for local test
# input_path = "/mnt/d/aws-taxi/{}"
# output_path = "/home/yunfei/aws/taxi-formatted/{}"


# for s3 pre-treatment
input_path = "s3://nyc-tlc/trip data/{}"
output_path = "s3://taxi-poc-formatted/{}"


def format_and_split_csv():
    location_lines = [json.loads(x) for x in zones.split('\n') if x]
    location_dict = {x["LocationID"]: x for x in location_lines}

    file_lines = [x.split(',') for x in file_list.split('\n') if x]
    file_dict = defaultdict(lambda: [])
    for month, file_name, file_type in file_lines:
        file_dict[month].append(file_name.strip())

    lng_udf = udf(lambda x: location_dict.get(x, {}).get("lng"), DoubleType())
    lat_udf = udf(lambda x: location_dict.get(x, {}).get("lat"), DoubleType())
    udf_list = [
        ("pickup_longitude", "pulocationid", lng_udf),
        ("pickup_latitude", "pulocationid", lat_udf),
        ("dropoff_longitude", "dolocationid", lng_udf),
        ("dropoff_latitude", "dolocationid", lat_udf),
        ("pickup_longitude", "locationid", lng_udf),
        ("pickup_latitude", "locationid", lat_udf),
    ]
    date_udf = udf(lambda x: re.findall('\d{4}-\d{2}-\d{2}', x)[-1], StringType())

    spark = SparkSession.builder.getOrCreate()

    for date, file_names in file_dict.items():
        single_job = None
        for file_name in file_names:
            df = spark.read.option("header", True).csv(input_path.format(file_name))

            # trans all columns to lowercase & remove "_"
            for column_name in df.schema.names:
                df = df.withColumnRenamed(column_name, column_name.replace("_", "").lower())

            # trans columns to target_name
            renames = []
            for target_name, key_names in file_mapping.items():
                for exists_name in df.schema.names:
                    if any(x in exists_name for x in key_names):
                        renames.append((exists_name, target_name))
                        break
            for exists_name, target_name in renames:
                df = df.withColumnRenamed(exists_name, target_name)
            new_names = [x[-1] for x in renames]
            old_names = set(x for x in df.schema.names)

            # get lat & lng from locationID
            for col_new, col_old, udf_func in udf_list:
                if col_new not in new_names and col_old in old_names:
                    df = df.withColumn(col_new, udf_func(col(col_old)))
                    new_names.append(col_new)
            for col_name in set(file_mapping.keys()) - set(new_names):
                df = df.withColumn(col_name, lit(""))

            # file vendor-type 0 for taxi 1 for others
            file_first = file_name.split("_")[0]
            df = df.withColumn("vendor_type", lit(0) if file_first in ("green", "yellow") else lit(1))

            df = df.select(*field_list).withColumn("date", date_udf(df.pickup_datetime)).filter(
                col("date").startswith(date))
            single_job = df if single_job is None else df.union(single_job)

        single_job.orderBy("pickup_datetime").write.partitionBy("date").mode("overwrite").option(
            "header", "true").format("com.databricks.spark.csv").save(output_path.format(date))

    spark.stop()


field_list = [
    "vendor_type",
    "passenger_count",
    "total_amount",
    "trip_distance",
    "pickup_datetime",
    "pickup_latitude",
    "pickup_longitude",
    "dropoff_datetime",
    "dropoff_latitude",
    "dropoff_longitude",
]

file_mapping = {
    "vendor_id": ["vendor", "dispatchingbasenumber", "dispatchingbasenum"],
    "pickup_datetime": ["pickupdate"],
    "dropoff_datetime": ["dropoffdate"],
    "passenger_count": ["passenger"],
    "trip_distance": ["tripdistance"],
    "pickup_longitude": ["pickuplongitude", "startlon"],
    "pickup_latitude": ["pickuplatitude", "startlat"],
    "dropoff_longitude": ["dropofflongitude", "endlon"],
    "dropoff_latitude": ["dropofflatitude", "endlat"],
    "total_amount": ["totalam"],
}

file_headers = """
dispatchingbasenum,pickupdate,locationid
dispatchingbasenum,pickupdatetime,dropoffdatetime,pulocationid,dolocationid
dispatchingbasenum,pickupdatetime,dropoffdatetime,pulocationid,dolocationid,srflag
hvfhslicensenum,dispatchingbasenum,pickupdatetime,dropoffdatetime,pulocationid,dolocationid,srflag
pickupdatetime,dropoffdatetime,pulocationid,dolocationid,srflag,dispatchingbasenumber,dispatchingbasenum
vendorid,lpeppickupdatetime,lpepdropoffdatetime,storeandfwdflag,ratecodeid,pickuplongitude,pickuplatitude,dropofflongitude,dropofflatitude,passengercount,tripdistance,fareamount,extra,mtatax,tipamount,tollsamount,ehailfee,improvementsurcharge,totalamount,paymenttype,triptype
vendorid,lpeppickupdatetime,lpepdropoffdatetime,storeandfwdflag,ratecodeid,pickuplongitude,pickuplatitude,dropofflongitude,dropofflatitude,passengercount,tripdistance,fareamount,extra,mtatax,tipamount,tollsamount,ehailfee,totalamount,paymenttype,triptype
vendorid,lpeppickupdatetime,lpepdropoffdatetime,storeandfwdflag,ratecodeid,pulocationid,dolocationid,passengercount,tripdistance,fareamount,extra,mtatax,tipamount,tollsamount,ehailfee,improvementsurcharge,totalamount,paymenttype,triptype
vendorid,lpeppickupdatetime,lpepdropoffdatetime,storeandfwdflag,ratecodeid,pulocationid,dolocationid,passengercount,tripdistance,fareamount,extra,mtatax,tipamount,tollsamount,ehailfee,improvementsurcharge,totalamount,paymenttype,triptype,congestionsurcharge
vendorid,pickupdatetime,dropoffdatetime,passengercount,tripdistance,pickuplongitude,pickuplatitude,ratecode,storeandfwdflag,dropofflongitude,dropofflatitude,paymenttype,fareamount,surcharge,mtatax,tipamount,tollsamount,totalamount
vendorid,tpeppickupdatetime,tpepdropoffdatetime,passengercount,tripdistance,pickuplongitude,pickuplatitude,ratecodeid,storeandfwdflag,dropofflongitude,dropofflatitude,paymenttype,fareamount,extra,mtatax,tipamount,tollsamount,improvementsurcharge,totalamount
vendorid,tpeppickupdatetime,tpepdropoffdatetime,passengercount,tripdistance,ratecodeid,storeandfwdflag,pulocationid,dolocationid,paymenttype,fareamount,extra,mtatax,tipamount,tollsamount,improvementsurcharge,totalamount
vendorid,tpeppickupdatetime,tpepdropoffdatetime,passengercount,tripdistance,ratecodeid,storeandfwdflag,pulocationid,dolocationid,paymenttype,fareamount,extra,mtatax,tipamount,tollsamount,improvementsurcharge,totalamount,congestionsurcharge
vendorname,trippickupdatetime,tripdropoffdatetime,passengercount,tripdistance,startlon,startlat,ratecode,storeandforward,endlon,endlat,paymenttype,fareamt,surcharge,mtatax,tipamt,tollsamt,totalamt
"""

file_list = """
2009-01,yellow_tripdata_2009-01.csv,13
2009-02,yellow_tripdata_2009-02.csv,13
2009-03,yellow_tripdata_2009-03.csv,13
2009-04,yellow_tripdata_2009-04.csv,13
2009-05,yellow_tripdata_2009-05.csv,13
2009-06,yellow_tripdata_2009-06.csv,13
2009-07,yellow_tripdata_2009-07.csv,13
2009-08,yellow_tripdata_2009-08.csv,13
2009-09,yellow_tripdata_2009-09.csv,13
2009-10,yellow_tripdata_2009-10.csv,13
2009-11,yellow_tripdata_2009-11.csv,13
2009-12,yellow_tripdata_2009-12.csv,13
2010-01,yellow_tripdata_2010-01.csv,9
2010-02,yellow_tripdata_2010-02.csv,9
2010-03,yellow_tripdata_2010-03.csv,9
2010-04,yellow_tripdata_2010-04.csv,9
2010-05,yellow_tripdata_2010-05.csv,9
2010-06,yellow_tripdata_2010-06.csv,9
2010-07,yellow_tripdata_2010-07.csv,9
2010-08,yellow_tripdata_2010-08.csv,9
2010-09,yellow_tripdata_2010-09.csv,9
2010-10,yellow_tripdata_2010-10.csv,9
2010-11,yellow_tripdata_2010-11.csv,9
2010-12,yellow_tripdata_2010-12.csv,9
2011-01,yellow_tripdata_2011-01.csv,9
2011-02,yellow_tripdata_2011-02.csv,9
2011-03,yellow_tripdata_2011-03.csv,9
2011-04,yellow_tripdata_2011-04.csv,9
2011-05,yellow_tripdata_2011-05.csv,9
2011-06,yellow_tripdata_2011-06.csv,9
2011-07,yellow_tripdata_2011-07.csv,9
2011-08,yellow_tripdata_2011-08.csv,9
2011-09,yellow_tripdata_2011-09.csv,9
2011-10,yellow_tripdata_2011-10.csv,9
2011-11,yellow_tripdata_2011-11.csv,9
2011-12,yellow_tripdata_2011-12.csv,9
2012-01,yellow_tripdata_2012-01.csv,9
2012-02,yellow_tripdata_2012-02.csv,9
2012-03,yellow_tripdata_2012-03.csv,9
2012-04,yellow_tripdata_2012-04.csv,9
2012-05,yellow_tripdata_2012-05.csv,9
2012-06,yellow_tripdata_2012-06.csv,9
2012-07,yellow_tripdata_2012-07.csv,9
2012-08,yellow_tripdata_2012-08.csv,9
2012-09,yellow_tripdata_2012-09.csv,9
2012-10,yellow_tripdata_2012-10.csv,9
2012-11,yellow_tripdata_2012-11.csv,9
2012-12,yellow_tripdata_2012-12.csv,9
2013-01,yellow_tripdata_2013-01.csv,9
2013-02,yellow_tripdata_2013-02.csv,9
2013-03,yellow_tripdata_2013-03.csv,9
2013-04,yellow_tripdata_2013-04.csv,9
2013-05,yellow_tripdata_2013-05.csv,9
2013-06,yellow_tripdata_2013-06.csv,9
2013-07,yellow_tripdata_2013-07.csv,9
2013-08,green_tripdata_2013-08.csv,6
2013-08,yellow_tripdata_2013-08.csv,9
2013-09,green_tripdata_2013-09.csv,6
2013-09,yellow_tripdata_2013-09.csv,9
2013-10,green_tripdata_2013-10.csv,6
2013-10,yellow_tripdata_2013-10.csv,9
2013-11,green_tripdata_2013-11.csv,6
2013-11,yellow_tripdata_2013-11.csv,9
2013-12,green_tripdata_2013-12.csv,6
2013-12,yellow_tripdata_2013-12.csv,9
2014-01,green_tripdata_2014-01.csv,6
2014-01,yellow_tripdata_2014-01.csv,9
2014-02,green_tripdata_2014-02.csv,6
2014-02,yellow_tripdata_2014-02.csv,9
2014-03,green_tripdata_2014-03.csv,6
2014-03,yellow_tripdata_2014-03.csv,9
2014-04,green_tripdata_2014-04.csv,6
2014-04,yellow_tripdata_2014-04.csv,9
2014-05,green_tripdata_2014-05.csv,6
2014-05,yellow_tripdata_2014-05.csv,9
2014-06,green_tripdata_2014-06.csv,6
2014-06,yellow_tripdata_2014-06.csv,9
2014-07,green_tripdata_2014-07.csv,6
2014-07,yellow_tripdata_2014-07.csv,9
2014-08,green_tripdata_2014-08.csv,6
2014-08,yellow_tripdata_2014-08.csv,9
2014-09,green_tripdata_2014-09.csv,6
2014-09,yellow_tripdata_2014-09.csv,9
2014-10,green_tripdata_2014-10.csv,6
2014-10,yellow_tripdata_2014-10.csv,9
2014-11,green_tripdata_2014-11.csv,6
2014-11,yellow_tripdata_2014-11.csv,9
2014-12,green_tripdata_2014-12.csv,6
2014-12,yellow_tripdata_2014-12.csv,9
2015-01,fhv_tripdata_2015-01.csv,0
2015-01,green_tripdata_2015-01.csv,5
2015-01,yellow_tripdata_2015-01.csv,10
2015-02,fhv_tripdata_2015-02.csv,0
2015-02,green_tripdata_2015-02.csv,5
2015-02,yellow_tripdata_2015-02.csv,10
2015-03,fhv_tripdata_2015-03.csv,0
2015-03,green_tripdata_2015-03.csv,5
2015-03,yellow_tripdata_2015-03.csv,10
2015-04,fhv_tripdata_2015-04.csv,0
2015-04,green_tripdata_2015-04.csv,5
2015-04,yellow_tripdata_2015-04.csv,10
2015-05,fhv_tripdata_2015-05.csv,0
2015-05,green_tripdata_2015-05.csv,5
2015-05,yellow_tripdata_2015-05.csv,10
2015-06,fhv_tripdata_2015-06.csv,0
2015-06,green_tripdata_2015-06.csv,5
2015-06,yellow_tripdata_2015-06.csv,10
2015-07,fhv_tripdata_2015-07.csv,0
2015-07,green_tripdata_2015-07.csv,5
2015-07,yellow_tripdata_2015-07.csv,10
2015-08,fhv_tripdata_2015-08.csv,0
2015-08,green_tripdata_2015-08.csv,5
2015-08,yellow_tripdata_2015-08.csv,10
2015-09,fhv_tripdata_2015-09.csv,0
2015-09,green_tripdata_2015-09.csv,5
2015-09,yellow_tripdata_2015-09.csv,10
2015-10,fhv_tripdata_2015-10.csv,0
2015-10,green_tripdata_2015-10.csv,5
2015-10,yellow_tripdata_2015-10.csv,10
2015-11,fhv_tripdata_2015-11.csv,0
2015-11,green_tripdata_2015-11.csv,5
2015-11,yellow_tripdata_2015-11.csv,10
2015-12,fhv_tripdata_2015-12.csv,0
2015-12,green_tripdata_2015-12.csv,5
2015-12,yellow_tripdata_2015-12.csv,10
2016-01,fhv_tripdata_2016-01.csv,0
2016-01,green_tripdata_2016-01.csv,5
2016-01,yellow_tripdata_2016-01.csv,10
2016-02,fhv_tripdata_2016-02.csv,0
2016-02,green_tripdata_2016-02.csv,5
2016-02,yellow_tripdata_2016-02.csv,10
2016-03,fhv_tripdata_2016-03.csv,0
2016-03,green_tripdata_2016-03.csv,5
2016-03,yellow_tripdata_2016-03.csv,10
2016-04,fhv_tripdata_2016-04.csv,0
2016-04,green_tripdata_2016-04.csv,5
2016-04,yellow_tripdata_2016-04.csv,10
2016-05,fhv_tripdata_2016-05.csv,0
2016-05,green_tripdata_2016-05.csv,5
2016-05,yellow_tripdata_2016-05.csv,10
2016-06,fhv_tripdata_2016-06.csv,0
2016-06,green_tripdata_2016-06.csv,5
2016-06,yellow_tripdata_2016-06.csv,10
2016-07,fhv_tripdata_2016-07.csv,0
2016-07,green_tripdata_2016-07.csv,7
2016-07,yellow_tripdata_2016-07.csv,11
2016-08,fhv_tripdata_2016-08.csv,0
2016-08,green_tripdata_2016-08.csv,7
2016-08,yellow_tripdata_2016-08.csv,11
2016-09,fhv_tripdata_2016-09.csv,0
2016-09,green_tripdata_2016-09.csv,7
2016-09,yellow_tripdata_2016-09.csv,11
2016-10,fhv_tripdata_2016-10.csv,0
2016-10,green_tripdata_2016-10.csv,7
2016-10,yellow_tripdata_2016-10.csv,11
2016-11,fhv_tripdata_2016-11.csv,0
2016-11,green_tripdata_2016-11.csv,7
2016-11,yellow_tripdata_2016-11.csv,11
2016-12,fhv_tripdata_2016-12.csv,0
2016-12,green_tripdata_2016-12.csv,7
2016-12,yellow_tripdata_2016-12.csv,11
2017-01,fhv_tripdata_2017-01.csv,1
2017-01,green_tripdata_2017-01.csv,7
2017-01,yellow_tripdata_2017-01.csv,11
2017-02,fhv_tripdata_2017-02.csv,1
2017-02,green_tripdata_2017-02.csv,7
2017-02,yellow_tripdata_2017-02.csv,11
2017-03,fhv_tripdata_2017-03.csv,1
2017-03,green_tripdata_2017-03.csv,7
2017-03,yellow_tripdata_2017-03.csv,11
2017-04,fhv_tripdata_2017-04.csv,1
2017-04,green_tripdata_2017-04.csv,7
2017-04,yellow_tripdata_2017-04.csv,11
2017-05,fhv_tripdata_2017-05.csv,1
2017-05,green_tripdata_2017-05.csv,7
2017-05,yellow_tripdata_2017-05.csv,11
2017-06,fhv_tripdata_2017-06.csv,1
2017-06,green_tripdata_2017-06.csv,7
2017-06,yellow_tripdata_2017-06.csv,11
2017-07,fhv_tripdata_2017-07.csv,2
2017-07,green_tripdata_2017-07.csv,7
2017-07,yellow_tripdata_2017-07.csv,11
2017-08,fhv_tripdata_2017-08.csv,2
2017-08,green_tripdata_2017-08.csv,7
2017-08,yellow_tripdata_2017-08.csv,11
2017-09,fhv_tripdata_2017-09.csv,2
2017-09,green_tripdata_2017-09.csv,7
2017-09,yellow_tripdata_2017-09.csv,11
2017-10,fhv_tripdata_2017-10.csv,2
2017-10,green_tripdata_2017-10.csv,7
2017-10,yellow_tripdata_2017-10.csv,11
2017-11,fhv_tripdata_2017-11.csv,2
2017-11,green_tripdata_2017-11.csv,7
2017-11,yellow_tripdata_2017-11.csv,11
2017-12,fhv_tripdata_2017-12.csv,2
2017-12,green_tripdata_2017-12.csv,7
2017-12,yellow_tripdata_2017-12.csv,11
2018-01,fhv_tripdata_2018-01.csv,4
2018-01,green_tripdata_2018-01.csv,7
2018-01,yellow_tripdata_2018-01.csv,11
2018-02,fhv_tripdata_2018-02.csv,4
2018-02,green_tripdata_2018-02.csv,7
2018-02,yellow_tripdata_2018-02.csv,11
2018-03,fhv_tripdata_2018-03.csv,4
2018-03,green_tripdata_2018-03.csv,7
2018-03,yellow_tripdata_2018-03.csv,11
2018-04,fhv_tripdata_2018-04.csv,4
2018-04,green_tripdata_2018-04.csv,7
2018-04,yellow_tripdata_2018-04.csv,11
2018-05,fhv_tripdata_2018-05.csv,4
2018-05,green_tripdata_2018-05.csv,7
2018-05,yellow_tripdata_2018-05.csv,11
2018-06,fhv_tripdata_2018-06.csv,4
2018-06,green_tripdata_2018-06.csv,7
2018-06,yellow_tripdata_2018-06.csv,11
2018-07,fhv_tripdata_2018-07.csv,4
2018-07,green_tripdata_2018-07.csv,7
2018-07,yellow_tripdata_2018-07.csv,11
2018-08,fhv_tripdata_2018-08.csv,4
2018-08,green_tripdata_2018-08.csv,7
2018-08,yellow_tripdata_2018-08.csv,11
2018-09,fhv_tripdata_2018-09.csv,4
2018-09,green_tripdata_2018-09.csv,7
2018-09,yellow_tripdata_2018-09.csv,11
2018-10,fhv_tripdata_2018-10.csv,4
2018-10,green_tripdata_2018-10.csv,7
2018-10,yellow_tripdata_2018-10.csv,11
2018-11,fhv_tripdata_2018-11.csv,4
2018-11,green_tripdata_2018-11.csv,7
2018-11,yellow_tripdata_2018-11.csv,11
2018-12,fhv_tripdata_2018-12.csv,4
2018-12,green_tripdata_2018-12.csv,7
2018-12,yellow_tripdata_2018-12.csv,11
2019-01,fhv_tripdata_2019-01.csv,2
2019-01,green_tripdata_2019-01.csv,8
2019-01,yellow_tripdata_2019-01.csv,12
2019-02,fhv_tripdata_2019-02.csv,2
2019-02,fhvhv_tripdata_2019-02.csv,3
2019-02,green_tripdata_2019-02.csv,8
2019-02,yellow_tripdata_2019-02.csv,12
2019-03,fhv_tripdata_2019-03.csv,2
2019-03,fhvhv_tripdata_2019-03.csv,3
2019-03,green_tripdata_2019-03.csv,8
2019-03,yellow_tripdata_2019-03.csv,12
2019-04,fhv_tripdata_2019-04.csv,2
2019-04,fhvhv_tripdata_2019-04.csv,3
2019-04,green_tripdata_2019-04.csv,8
2019-04,yellow_tripdata_2019-04.csv,12
2019-05,fhv_tripdata_2019-05.csv,2
2019-05,fhvhv_tripdata_2019-05.csv,3
2019-05,green_tripdata_2019-05.csv,8
2019-05,yellow_tripdata_2019-05.csv,12
2019-06,fhv_tripdata_2019-06.csv,2
2019-06,fhvhv_tripdata_2019-06.csv,3
2019-06,green_tripdata_2019-06.csv,8
2019-06,yellow_tripdata_2019-06.csv,12
2019-07,fhv_tripdata_2019-07.csv,2
2019-07,fhvhv_tripdata_2019-07.csv,3
2019-07,green_tripdata_2019-07.csv,8
2019-07,yellow_tripdata_2019-07.csv,12
2019-08,fhv_tripdata_2019-08.csv,2
2019-08,fhvhv_tripdata_2019-08.csv,3
2019-08,green_tripdata_2019-08.csv,8
2019-08,yellow_tripdata_2019-08.csv,12
2019-09,fhv_tripdata_2019-09.csv,2
2019-09,fhvhv_tripdata_2019-09.csv,3
2019-09,green_tripdata_2019-09.csv,8
2019-09,yellow_tripdata_2019-09.csv,12
2019-10,fhv_tripdata_2019-10.csv,2
2019-10,fhvhv_tripdata_2019-10.csv,3
2019-10,green_tripdata_2019-10.csv,8
2019-10,yellow_tripdata_2019-10.csv,12
2019-11,fhv_tripdata_2019-11.csv,2
2019-11,fhvhv_tripdata_2019-11.csv,3
2019-11,green_tripdata_2019-11.csv,8
2019-11,yellow_tripdata_2019-11.csv,12
2019-12,fhv_tripdata_2019-12.csv,2
2019-12,fhvhv_tripdata_2019-12.csv,3
2019-12,green_tripdata_2019-12.csv,8
2019-12,yellow_tripdata_2019-12.csv,12
2020-01,fhv_tripdata_2020-01.csv,2
2020-01,fhvhv_tripdata_2020-01.csv,3
2020-01,green_tripdata_2020-01.csv,8
2020-01,yellow_tripdata_2020-01.csv,12
2020-02,fhv_tripdata_2020-02.csv,2
2020-02,fhvhv_tripdata_2020-02.csv,3
2020-02,green_tripdata_2020-02.csv,8
2020-02,yellow_tripdata_2020-02.csv,12
2020-03,fhv_tripdata_2020-03.csv,2
2020-03,fhvhv_tripdata_2020-03.csv,3
2020-03,green_tripdata_2020-03.csv,8
2020-03,yellow_tripdata_2020-03.csv,12
2020-04,fhv_tripdata_2020-04.csv,2
2020-04,fhvhv_tripdata_2020-04.csv,3
2020-04,green_tripdata_2020-04.csv,8
2020-04,yellow_tripdata_2020-04.csv,12
2020-05,fhv_tripdata_2020-05.csv,2
2020-05,fhvhv_tripdata_2020-05.csv,3
2020-05,green_tripdata_2020-05.csv,8
2020-05,yellow_tripdata_2020-05.csv,12
2020-06,fhv_tripdata_2020-06.csv,2
2020-06,fhvhv_tripdata_2020-06.csv,3
2020-06,green_tripdata_2020-06.csv,8
2020-06,yellow_tripdata_2020-06.csv,12
2020-07,fhv_tripdata_2020-07.csv,2
2020-07,fhvhv_tripdata_2020-07.csv,3
2020-07,green_tripdata_2020-07.csv,8
2020-07,yellow_tripdata_2020-07.csv,12
2020-08,fhv_tripdata_2020-08.csv,2
2020-08,fhvhv_tripdata_2020-08.csv,3
2020-08,green_tripdata_2020-08.csv,8
2020-08,yellow_tripdata_2020-08.csv,12
2020-09,fhv_tripdata_2020-09.csv,2
2020-09,fhvhv_tripdata_2020-09.csv,3
2020-09,green_tripdata_2020-09.csv,8
2020-09,yellow_tripdata_2020-09.csv,12
2020-10,fhv_tripdata_2020-10.csv,2
2020-10,fhvhv_tripdata_2020-10.csv,3
2020-10,green_tripdata_2020-10.csv,8
2020-10,yellow_tripdata_2020-10.csv,12
2020-11,fhv_tripdata_2020-11.csv,2
2020-11,fhvhv_tripdata_2020-11.csv,3
2020-11,green_tripdata_2020-11.csv,8
2020-11,yellow_tripdata_2020-11.csv,12
2020-12,fhv_tripdata_2020-12.csv,2
2020-12,fhvhv_tripdata_2020-12.csv,3
2020-12,green_tripdata_2020-12.csv,8
2020-12,yellow_tripdata_2020-12.csv,12
"""

zones = """
{"LocationID": "1", "Borough": "EWR", "Zone": "Newark Airport", "service_zone": "EWR", "lat": 40.6895314, "lng": -74.1744624}
{"LocationID": "2", "Borough": "Queens", "Zone": "Jamaica Bay", "service_zone": "Boro Zone", "lat": 40.6056632, "lng": -73.8713099}
{"LocationID": "3", "Borough": "Bronx", "Zone": "Allerton/Pelham Gardens", "service_zone": "Boro Zone", "lat": 40.8627726, "lng": -73.84343919999999}
{"LocationID": "4", "Borough": "Manhattan", "Zone": "Alphabet City", "service_zone": "Yellow Zone", "lat": 40.7258428, "lng": -73.9774916}
{"LocationID": "5", "Borough": "Staten Island", "Zone": "Arden Heights", "service_zone": "Boro Zone", "lat": 40.556413, "lng": -74.1735044}
{"LocationID": "6", "Borough": "Staten Island", "Zone": "Arrochar/Fort Wadsworth", "service_zone": "Boro Zone", "lat": 40.6012117, "lng": -74.0579185}
{"LocationID": "7", "Borough": "Queens", "Zone": "Astoria", "service_zone": "Boro Zone", "lat": 40.7643574, "lng": -73.92346189999999}
{"LocationID": "8", "Borough": "Queens", "Zone": "Astoria Park", "service_zone": "Boro Zone", "lat": 40.7785364, "lng": -73.92283359999999}
{"LocationID": "9", "Borough": "Queens", "Zone": "Auburndale", "service_zone": "Boro Zone", "lat": 40.7577672, "lng": -73.78339609999999}
{"LocationID": "10", "Borough": "Queens", "Zone": "Baisley Park", "service_zone": "Boro Zone", "lat": 40.6737751, "lng": -73.786025}
{"LocationID": "11", "Borough": "Brooklyn", "Zone": "Bath Beach", "service_zone": "Boro Zone", "lat": 40.6038852, "lng": -74.0062078}
{"LocationID": "12", "Borough": "Manhattan", "Zone": "Battery Park", "service_zone": "Yellow Zone", "lat": 40.703141, "lng": -74.0159996}
{"LocationID": "13", "Borough": "Manhattan", "Zone": "Battery Park City", "service_zone": "Yellow Zone", "lat": 40.7115786, "lng": -74.0158441}
{"LocationID": "14", "Borough": "Brooklyn", "Zone": "Bay Ridge", "service_zone": "Boro Zone", "lat": 40.6263732, "lng": -74.0298767}
{"LocationID": "15", "Borough": "Queens", "Zone": "Bay Terrace/Fort Totten", "service_zone": "Boro Zone", "lat": 40.7920899, "lng": -73.7760996}
{"LocationID": "16", "Borough": "Queens", "Zone": "Bayside", "service_zone": "Boro Zone", "lat": 40.7585569, "lng": -73.7654367}
{"LocationID": "17", "Borough": "Brooklyn", "Zone": "Bedford", "service_zone": "Boro Zone", "lat": 40.6872176, "lng": -73.9417735}
{"LocationID": "18", "Borough": "Bronx", "Zone": "Bedford Park", "service_zone": "Boro Zone", "lat": 40.8700999, "lng": -73.8856912}
{"LocationID": "19", "Borough": "Queens", "Zone": "Bellerose", "service_zone": "Boro Zone", "lat": 40.7361769, "lng": -73.7137365}
{"LocationID": "20", "Borough": "Bronx", "Zone": "Belmont", "service_zone": "Boro Zone", "lat": 40.8534507, "lng": -73.88936819999999}
{"LocationID": "21", "Borough": "Brooklyn", "Zone": "Bensonhurst East", "service_zone": "Boro Zone", "lat": 40.6139307, "lng": -73.9921833}
{"LocationID": "22", "Borough": "Brooklyn", "Zone": "Bensonhurst West", "service_zone": "Boro Zone", "lat": 40.6139307, "lng": -73.9921833}
{"LocationID": "23", "Borough": "Staten Island", "Zone": "Bloomfield/Emerson Hill", "service_zone": "Boro Zone", "lat": 40.6074525, "lng": -74.0963115}
{"LocationID": "24", "Borough": "Manhattan", "Zone": "Bloomingdale", "service_zone": "Yellow Zone", "lat": 40.7988958, "lng": -73.9697795}
{"LocationID": "25", "Borough": "Brooklyn", "Zone": "Boerum Hill", "service_zone": "Boro Zone", "lat": 40.6848689, "lng": -73.9844722}
{"LocationID": "26", "Borough": "Brooklyn", "Zone": "Borough Park", "service_zone": "Boro Zone", "lat": 40.6350319, "lng": -73.9921028}
{"LocationID": "27", "Borough": "Queens", "Zone": "Breezy Point/Fort Tilden/Riis Beach", "service_zone": "Boro Zone", "lat": 40.5597687, "lng": -73.88761509999999}
{"LocationID": "28", "Borough": "Queens", "Zone": "Briarwood/Jamaica Hills", "service_zone": "Boro Zone", "lat": 40.7109315, "lng": -73.81356099999999}
{"LocationID": "29", "Borough": "Brooklyn", "Zone": "Brighton Beach", "service_zone": "Boro Zone", "lat": 40.5780706, "lng": -73.9596565}
{"LocationID": "30", "Borough": "Queens", "Zone": "Broad Channel", "service_zone": "Boro Zone", "lat": 40.6158335, "lng": -73.8213213}
{"LocationID": "31", "Borough": "Bronx", "Zone": "Bronx Park", "service_zone": "Boro Zone", "lat": 40.8608544, "lng": -73.8706278}
{"LocationID": "32", "Borough": "Bronx", "Zone": "Bronxdale", "service_zone": "Boro Zone", "lat": 40.8474697, "lng": -73.8599132}
{"LocationID": "33", "Borough": "Brooklyn", "Zone": "Brooklyn Heights", "service_zone": "Boro Zone", "lat": 40.6959294, "lng": -73.9955523}
{"LocationID": "34", "Borough": "Brooklyn", "Zone": "Brooklyn Navy Yard", "service_zone": "Boro Zone", "lat": 40.7025634, "lng": -73.9697795}
{"LocationID": "35", "Borough": "Brooklyn", "Zone": "Brownsville", "service_zone": "Boro Zone", "lat": 40.665214, "lng": -73.9125304}
{"LocationID": "36", "Borough": "Brooklyn", "Zone": "Bushwick North", "service_zone": "Boro Zone", "lat": 40.6957755, "lng": -73.9170604}
{"LocationID": "37", "Borough": "Brooklyn", "Zone": "Bushwick South", "service_zone": "Boro Zone", "lat": 40.7043655, "lng": -73.9383476}
{"LocationID": "38", "Borough": "Queens", "Zone": "Cambria Heights", "service_zone": "Boro Zone", "lat": 40.692158, "lng": -73.7330753}
{"LocationID": "39", "Borough": "Brooklyn", "Zone": "Canarsie", "service_zone": "Boro Zone", "lat": 40.6402325, "lng": -73.9060579}
{"LocationID": "40", "Borough": "Brooklyn", "Zone": "Carroll Gardens", "service_zone": "Boro Zone", "lat": 40.6795331, "lng": -73.9991637}
{"LocationID": "41", "Borough": "Manhattan", "Zone": "Central Harlem", "service_zone": "Boro Zone", "lat": 40.8089419, "lng": -73.9482305}
{"LocationID": "42", "Borough": "Manhattan", "Zone": "Central Harlem North", "service_zone": "Boro Zone", "lat": 40.8142585, "lng": -73.9426617}
{"LocationID": "43", "Borough": "Manhattan", "Zone": "Central Park", "service_zone": "Yellow Zone", "lat": 40.7812199, "lng": -73.9665138}
{"LocationID": "44", "Borough": "Staten Island", "Zone": "Charleston/Tottenville", "service_zone": "Boro Zone", "lat": 40.5083408, "lng": -74.23554039999999}
{"LocationID": "45", "Borough": "Manhattan", "Zone": "Chinatown", "service_zone": "Yellow Zone", "lat": 40.7157509, "lng": -73.9970307}
{"LocationID": "46", "Borough": "Bronx", "Zone": "City Island", "service_zone": "Boro Zone", "lat": 40.8468202, "lng": -73.7874983}
{"LocationID": "47", "Borough": "Bronx", "Zone": "Claremont/Bathgate", "service_zone": "Boro Zone", "lat": 40.84128339999999, "lng": -73.9001573}
{"LocationID": "48", "Borough": "Manhattan", "Zone": "Clinton East", "service_zone": "Yellow Zone", "lat": 40.7637581, "lng": -73.9918181}
{"LocationID": "49", "Borough": "Brooklyn", "Zone": "Clinton Hill", "service_zone": "Boro Zone", "lat": 40.6896834, "lng": -73.9661144}
{"LocationID": "50", "Borough": "Manhattan", "Zone": "Clinton West", "service_zone": "Yellow Zone", "lat": 40.7628785, "lng": -73.9940134}
{"LocationID": "51", "Borough": "Bronx", "Zone": "Co-Op City", "service_zone": "Boro Zone", "lat": 40.8738889, "lng": -73.82944440000001}
{"LocationID": "52", "Borough": "Brooklyn", "Zone": "Cobble Hill", "service_zone": "Boro Zone", "lat": 40.686536, "lng": -73.9962255}
{"LocationID": "53", "Borough": "Queens", "Zone": "College Point", "service_zone": "Boro Zone", "lat": 40.786395, "lng": -73.8389657}
{"LocationID": "54", "Borough": "Brooklyn", "Zone": "Columbia Street", "service_zone": "Boro Zone", "lat": 40.6775239, "lng": -74.00634409999999}
{"LocationID": "55", "Borough": "Brooklyn", "Zone": "Coney Island", "service_zone": "Boro Zone", "lat": 40.5755438, "lng": -73.9707016}
{"LocationID": "56", "Borough": "Queens", "Zone": "Corona", "service_zone": "Boro Zone", "lat": 40.7449859, "lng": -73.8642613}
{"LocationID": "57", "Borough": "Queens", "Zone": "Corona", "service_zone": "Boro Zone", "lat": 40.7449859, "lng": -73.8642613}
{"LocationID": "58", "Borough": "Bronx", "Zone": "Country Club", "service_zone": "Boro Zone", "lat": 40.8391667, "lng": -73.8197222}
{"LocationID": "59", "Borough": "Bronx", "Zone": "Crotona Park", "service_zone": "Boro Zone", "lat": 40.8400367, "lng": -73.8953489}
{"LocationID": "60", "Borough": "Bronx", "Zone": "Crotona Park East", "service_zone": "Boro Zone", "lat": 40.8365344, "lng": -73.8933509}
{"LocationID": "61", "Borough": "Brooklyn", "Zone": "Crown Heights North", "service_zone": "Boro Zone", "lat": 40.6694022, "lng": -73.9422324}
{"LocationID": "62", "Borough": "Brooklyn", "Zone": "Crown Heights South", "service_zone": "Boro Zone", "lat": 40.6694022, "lng": -73.9422324}
{"LocationID": "63", "Borough": "Brooklyn", "Zone": "Cypress Hills", "service_zone": "Boro Zone", "lat": 40.6836873, "lng": -73.87963309999999}
{"LocationID": "64", "Borough": "Queens", "Zone": "Douglaston", "service_zone": "Boro Zone", "lat": 40.76401509999999, "lng": -73.7433727}
{"LocationID": "65", "Borough": "Brooklyn", "Zone": "Downtown Brooklyn/MetroTech", "service_zone": "Boro Zone", "lat": 40.6930987, "lng": -73.98566339999999}
{"LocationID": "66", "Borough": "Brooklyn", "Zone": "DUMBO/Vinegar Hill", "service_zone": "Boro Zone", "lat": 40.70371859999999, "lng": -73.98226830000002}
{"LocationID": "67", "Borough": "Brooklyn", "Zone": "Dyker Heights", "service_zone": "Boro Zone", "lat": 40.6214932, "lng": -74.00958399999999}
{"LocationID": "68", "Borough": "Manhattan", "Zone": "East Chelsea", "service_zone": "Yellow Zone", "lat": 40.7465004, "lng": -74.00137370000002}
{"LocationID": "69", "Borough": "Bronx", "Zone": "East Concourse/Concourse Village", "service_zone": "Boro Zone", "lat": 40.8255863, "lng": -73.9184388}
{"LocationID": "70", "Borough": "Queens", "Zone": "East Elmhurst", "service_zone": "Boro Zone", "lat": 40.7737505, "lng": -73.8713099}
{"LocationID": "71", "Borough": "Brooklyn", "Zone": "East Flatbush/Farragut", "service_zone": "Boro Zone", "lat": 40.63751329999999, "lng": -73.9280797}
{"LocationID": "72", "Borough": "Brooklyn", "Zone": "East Flatbush/Remsen Village", "service_zone": "Boro Zone", "lat": 40.6511399, "lng": -73.9181602}
{"LocationID": "73", "Borough": "Queens", "Zone": "East Flushing", "service_zone": "Boro Zone", "lat": 40.7540534, "lng": -73.8086418}
{"LocationID": "74", "Borough": "Manhattan", "Zone": "East Harlem North", "service_zone": "Boro Zone", "lat": 40.7957399, "lng": -73.93892129999999}
{"LocationID": "75", "Borough": "Manhattan", "Zone": "East Harlem South", "service_zone": "Boro Zone", "lat": 40.7957399, "lng": -73.93892129999999}
{"LocationID": "76", "Borough": "Brooklyn", "Zone": "East New York", "service_zone": "Boro Zone", "lat": 40.6590529, "lng": -73.8759245}
{"LocationID": "77", "Borough": "Brooklyn", "Zone": "East New York/Pennsylvania Avenue", "service_zone": "Boro Zone", "lat": 40.65845729999999, "lng": -73.8904498}
{"LocationID": "78", "Borough": "Bronx", "Zone": "East Tremont", "service_zone": "Boro Zone", "lat": 40.8453781, "lng": -73.8909693}
{"LocationID": "79", "Borough": "Manhattan", "Zone": "East Village", "service_zone": "Yellow Zone", "lat": 40.7264773, "lng": -73.98153370000001}
{"LocationID": "80", "Borough": "Brooklyn", "Zone": "East Williamsburg", "service_zone": "Boro Zone", "lat": 40.7141953, "lng": -73.9316461}
{"LocationID": "81", "Borough": "Bronx", "Zone": "Eastchester", "service_zone": "Boro Zone", "lat": 40.8859837, "lng": -73.82794710000002}
{"LocationID": "82", "Borough": "Queens", "Zone": "Elmhurst", "service_zone": "Boro Zone", "lat": 40.737975, "lng": -73.8801301}
{"LocationID": "83", "Borough": "Queens", "Zone": "Elmhurst/Maspeth", "service_zone": "Boro Zone", "lat": 40.7294018, "lng": -73.9065883}
{"LocationID": "84", "Borough": "Staten Island", "Zone": "Eltingville/Annadale/Prince's Bay", "service_zone": "Boro Zone", "lat": 40.52899439999999, "lng": -74.197644}
{"LocationID": "85", "Borough": "Brooklyn", "Zone": "Erasmus", "service_zone": "Boro Zone", "lat": 40.649649, "lng": -73.95287379999999}
{"LocationID": "86", "Borough": "Queens", "Zone": "Far Rockaway", "service_zone": "Boro Zone", "lat": 40.5998931, "lng": -73.74484369999999}
{"LocationID": "87", "Borough": "Manhattan", "Zone": "Financial District North", "service_zone": "Yellow Zone", "lat": 40.7077143, "lng": -74.00827869999999}
{"LocationID": "88", "Borough": "Manhattan", "Zone": "Financial District South", "service_zone": "Yellow Zone", "lat": 40.705123, "lng": -74.0049259}
{"LocationID": "89", "Borough": "Brooklyn", "Zone": "Flatbush/Ditmas Park", "service_zone": "Boro Zone", "lat": 40.6414876, "lng": -73.9593998}
{"LocationID": "90", "Borough": "Manhattan", "Zone": "Flatiron", "service_zone": "Yellow Zone", "lat": 40.740083, "lng": -73.9903489}
{"LocationID": "91", "Borough": "Brooklyn", "Zone": "Flatlands", "service_zone": "Boro Zone", "lat": 40.6232714, "lng": -73.9321664}
{"LocationID": "92", "Borough": "Queens", "Zone": "Flushing", "service_zone": "Boro Zone", "lat": 40.7674987, "lng": -73.833079}
{"LocationID": "93", "Borough": "Queens", "Zone": "Flushing Meadows-Corona Park", "service_zone": "Boro Zone", "lat": 40.7400275, "lng": -73.8406953}
{"LocationID": "94", "Borough": "Bronx", "Zone": "Fordham South", "service_zone": "Boro Zone", "lat": 40.8592667, "lng": -73.8984694}
{"LocationID": "95", "Borough": "Queens", "Zone": "Forest Hills", "service_zone": "Boro Zone", "lat": 40.718106, "lng": -73.8448469}
{"LocationID": "96", "Borough": "Queens", "Zone": "Forest Park/Highland Park", "service_zone": "Boro Zone", "lat": 40.6960418, "lng": -73.8663024}
{"LocationID": "97", "Borough": "Brooklyn", "Zone": "Fort Greene", "service_zone": "Boro Zone", "lat": 40.6920638, "lng": -73.97418739999999}
{"LocationID": "98", "Borough": "Queens", "Zone": "Fresh Meadows", "service_zone": "Boro Zone", "lat": 40.7335179, "lng": -73.7801447}
{"LocationID": "99", "Borough": "Staten Island", "Zone": "Freshkills Park", "service_zone": "Boro Zone", "lat": 40.5772365, "lng": -74.1858183}
{"LocationID": "100", "Borough": "Manhattan", "Zone": "Garment District", "service_zone": "Yellow Zone", "lat": 40.7547072, "lng": -73.9916342}
{"LocationID": "101", "Borough": "Queens", "Zone": "Glen Oaks", "service_zone": "Boro Zone", "lat": 40.7471504, "lng": -73.7118223}
{"LocationID": "102", "Borough": "Queens", "Zone": "Glendale", "service_zone": "Boro Zone", "lat": 40.7016662, "lng": -73.8842219}
{"LocationID": "103", "Borough": "Manhattan", "Zone": "Governor's Island/Ellis Island/Liberty Island", "service_zone": "Yellow Zone", "lat": 40.6892494, "lng": -74.04450039999999}
{"LocationID": "104", "Borough": "Manhattan", "Zone": "Governor's Island/Ellis Island/Liberty Island", "service_zone": "Yellow Zone", "lat": 40.6892494, "lng": -74.04450039999999}
{"LocationID": "105", "Borough": "Manhattan", "Zone": "Governor's Island/Ellis Island/Liberty Island", "service_zone": "Yellow Zone", "lat": 40.6892494, "lng": -74.04450039999999}
{"LocationID": "106", "Borough": "Brooklyn", "Zone": "Gowanus", "service_zone": "Boro Zone", "lat": 40.6751161, "lng": -73.9879753}
{"LocationID": "107", "Borough": "Manhattan", "Zone": "Gramercy", "service_zone": "Yellow Zone", "lat": 40.7367783, "lng": -73.9844722}
{"LocationID": "108", "Borough": "Brooklyn", "Zone": "Gravesend", "service_zone": "Boro Zone", "lat": 40.5918636, "lng": -73.9768653}
{"LocationID": "109", "Borough": "Staten Island", "Zone": "Great Kills", "service_zone": "Boro Zone", "lat": 40.5543273, "lng": -74.156292}
{"LocationID": "110", "Borough": "Staten Island", "Zone": "Great Kills Park", "service_zone": "Boro Zone", "lat": 40.5492367, "lng": -74.1238486}
{"LocationID": "111", "Borough": "Brooklyn", "Zone": "Green-Wood Cemetery", "service_zone": "Boro Zone", "lat": 40.6579777, "lng": -73.9940634}
{"LocationID": "112", "Borough": "Brooklyn", "Zone": "Greenpoint", "service_zone": "Boro Zone", "lat": 40.7304701, "lng": -73.95150319999999}
{"LocationID": "113", "Borough": "Manhattan", "Zone": "Greenwich Village North", "service_zone": "Yellow Zone", "lat": 40.7335719, "lng": -74.0027418}
{"LocationID": "114", "Borough": "Manhattan", "Zone": "Greenwich Village South", "service_zone": "Yellow Zone", "lat": 40.7335719, "lng": -74.0027418}
{"LocationID": "115", "Borough": "Staten Island", "Zone": "Grymes Hill/Clifton", "service_zone": "Boro Zone", "lat": 40.6189726, "lng": -74.0784785}
{"LocationID": "116", "Borough": "Manhattan", "Zone": "Hamilton Heights", "service_zone": "Boro Zone", "lat": 40.8252793, "lng": -73.94761390000001}
{"LocationID": "117", "Borough": "Queens", "Zone": "Hammels/Arverne", "service_zone": "Boro Zone", "lat": 40.5880813, "lng": -73.81199289999999}
{"LocationID": "118", "Borough": "Staten Island", "Zone": "Heartland Village/Todt Hill", "service_zone": "Boro Zone", "lat": 40.5975007, "lng": -74.10189749999999}
{"LocationID": "119", "Borough": "Bronx", "Zone": "Highbridge", "service_zone": "Boro Zone", "lat": 40.836916, "lng": -73.9271294}
{"LocationID": "120", "Borough": "Manhattan", "Zone": "Highbridge Park", "service_zone": "Boro Zone", "lat": 40.8537599, "lng": -73.9257492}
{"LocationID": "121", "Borough": "Queens", "Zone": "Hillcrest/Pomonok", "service_zone": "Boro Zone", "lat": 40.732341, "lng": -73.81077239999999}
{"LocationID": "122", "Borough": "Queens", "Zone": "Hollis", "service_zone": "Boro Zone", "lat": 40.7112203, "lng": -73.762495}
{"LocationID": "123", "Borough": "Brooklyn", "Zone": "Homecrest", "service_zone": "Boro Zone", "lat": 40.6004787, "lng": -73.9565551}
{"LocationID": "124", "Borough": "Queens", "Zone": "Howard Beach", "service_zone": "Boro Zone", "lat": 40.6571222, "lng": -73.8429989}
{"LocationID": "125", "Borough": "Manhattan", "Zone": "Hudson Sq", "service_zone": "Yellow Zone", "lat": 40.7265834, "lng": -74.0074731}
{"LocationID": "126", "Borough": "Bronx", "Zone": "Hunts Point", "service_zone": "Boro Zone", "lat": 40.8094385, "lng": -73.8803315}
{"LocationID": "127", "Borough": "Manhattan", "Zone": "Inwood", "service_zone": "Boro Zone", "lat": 40.8677145, "lng": -73.9212019}
{"LocationID": "128", "Borough": "Manhattan", "Zone": "Inwood Hill Park", "service_zone": "Boro Zone", "lat": 40.8722007, "lng": -73.9255549}
{"LocationID": "129", "Borough": "Queens", "Zone": "Jackson Heights", "service_zone": "Boro Zone", "lat": 40.7556818, "lng": -73.8830701}
{"LocationID": "130", "Borough": "Queens", "Zone": "Jamaica", "service_zone": "Boro Zone", "lat": 40.702677, "lng": -73.7889689}
{"LocationID": "131", "Borough": "Queens", "Zone": "Jamaica Estates", "service_zone": "Boro Zone", "lat": 40.7179512, "lng": -73.783822}
{"LocationID": "132", "Borough": "Queens", "Zone": "JFK Airport", "service_zone": "Airports", "lat": 40.6413111, "lng": -73.77813909999999}
{"LocationID": "133", "Borough": "Brooklyn", "Zone": "Kensington", "service_zone": "Boro Zone", "lat": 40.63852019999999, "lng": -73.97318729999999}
{"LocationID": "134", "Borough": "Queens", "Zone": "Kew Gardens", "service_zone": "Boro Zone", "lat": 40.705695, "lng": -73.8272029}
{"LocationID": "135", "Borough": "Queens", "Zone": "Kew Gardens Hills", "service_zone": "Boro Zone", "lat": 40.724707, "lng": -73.8207618}
{"LocationID": "136", "Borough": "Bronx", "Zone": "Kingsbridge Heights", "service_zone": "Boro Zone", "lat": 40.8711235, "lng": -73.8976328}
{"LocationID": "137", "Borough": "Manhattan", "Zone": "Kips Bay", "service_zone": "Yellow Zone", "lat": 40.74232920000001, "lng": -73.9800645}
{"LocationID": "138", "Borough": "Queens", "Zone": "LaGuardia Airport", "service_zone": "Airports", "lat": 40.7769271, "lng": -73.8739659}
{"LocationID": "139", "Borough": "Queens", "Zone": "Laurelton", "service_zone": "Boro Zone", "lat": 40.67764, "lng": -73.7447853}
{"LocationID": "140", "Borough": "Manhattan", "Zone": "Lenox Hill East", "service_zone": "Yellow Zone", "lat": 40.7662315, "lng": -73.9602312}
{"LocationID": "141", "Borough": "Manhattan", "Zone": "Lenox Hill West", "service_zone": "Yellow Zone", "lat": 40.7662315, "lng": -73.9602312}
{"LocationID": "142", "Borough": "Manhattan", "Zone": "Lincoln Square East", "service_zone": "Yellow Zone", "lat": 40.7741769, "lng": -73.98491179999999}
{"LocationID": "143", "Borough": "Manhattan", "Zone": "Lincoln Square West", "service_zone": "Yellow Zone", "lat": 40.7741769, "lng": -73.98491179999999}
{"LocationID": "144", "Borough": "Manhattan", "Zone": "Little Italy/NoLiTa", "service_zone": "Yellow Zone", "lat": 40.7230413, "lng": -73.99486069999999}
{"LocationID": "145", "Borough": "Queens", "Zone": "Long Island City/Hunters Point", "service_zone": "Boro Zone", "lat": 40.7485587, "lng": -73.94964639999999}
{"LocationID": "146", "Borough": "Queens", "Zone": "Long Island City/Queens Plaza", "service_zone": "Boro Zone", "lat": 40.7509846, "lng": -73.9402762}
{"LocationID": "147", "Borough": "Bronx", "Zone": "Longwood", "service_zone": "Boro Zone", "lat": 40.8248438, "lng": -73.8915875}
{"LocationID": "148", "Borough": "Manhattan", "Zone": "Lower East Side", "service_zone": "Yellow Zone", "lat": 40.715033, "lng": -73.9842724}
{"LocationID": "149", "Borough": "Brooklyn", "Zone": "Madison", "service_zone": "Boro Zone", "lat": 40.60688529999999, "lng": -73.947958}
{"LocationID": "150", "Borough": "Brooklyn", "Zone": "Manhattan Beach", "service_zone": "Boro Zone", "lat": 40.57815799999999, "lng": -73.93892129999999}
{"LocationID": "151", "Borough": "Manhattan", "Zone": "Manhattan Valley", "service_zone": "Yellow Zone", "lat": 40.7966989, "lng": -73.9684247}
{"LocationID": "152", "Borough": "Manhattan", "Zone": "Manhattanville", "service_zone": "Boro Zone", "lat": 40.8169443, "lng": -73.9558333}
{"LocationID": "153", "Borough": "Manhattan", "Zone": "Marble Hill", "service_zone": "Boro Zone", "lat": 40.8761173, "lng": -73.9102628}
{"LocationID": "154", "Borough": "Brooklyn", "Zone": "Marine Park/Floyd Bennett Field", "service_zone": "Boro Zone", "lat": 40.58816030000001, "lng": -73.8969745}
{"LocationID": "155", "Borough": "Brooklyn", "Zone": "Marine Park/Mill Basin", "service_zone": "Boro Zone", "lat": 40.6055157, "lng": -73.9348698}
{"LocationID": "156", "Borough": "Staten Island", "Zone": "Mariners Harbor", "service_zone": "Boro Zone", "lat": 40.63677010000001, "lng": -74.1587547}
{"LocationID": "157", "Borough": "Queens", "Zone": "Maspeth", "service_zone": "Boro Zone", "lat": 40.7294018, "lng": -73.9065883}
{"LocationID": "158", "Borough": "Manhattan", "Zone": "Meatpacking/West Village West", "service_zone": "Yellow Zone", "lat": 40.7342331, "lng": -74.0100622}
{"LocationID": "159", "Borough": "Bronx", "Zone": "Melrose South", "service_zone": "Boro Zone", "lat": 40.824545, "lng": -73.9104143}
{"LocationID": "160", "Borough": "Queens", "Zone": "Middle Village", "service_zone": "Boro Zone", "lat": 40.717372, "lng": -73.87425}
{"LocationID": "161", "Borough": "Manhattan", "Zone": "Midtown Center", "service_zone": "Yellow Zone", "lat": 40.7314658, "lng": -73.9970956}
{"LocationID": "162", "Borough": "Manhattan", "Zone": "Midtown East", "service_zone": "Yellow Zone", "lat": 40.7571432, "lng": -73.9718815}
{"LocationID": "163", "Borough": "Manhattan", "Zone": "Midtown North", "service_zone": "Yellow Zone", "lat": 40.7649516, "lng": -73.9851039}
{"LocationID": "164", "Borough": "Manhattan", "Zone": "Midtown South", "service_zone": "Yellow Zone", "lat": 40.7521795, "lng": -73.9875438}
{"LocationID": "165", "Borough": "Brooklyn", "Zone": "Midwood", "service_zone": "Boro Zone", "lat": 40.6204388, "lng": -73.95997779999999}
{"LocationID": "166", "Borough": "Manhattan", "Zone": "Morningside Heights", "service_zone": "Boro Zone", "lat": 40.8105443, "lng": -73.9620581}
{"LocationID": "167", "Borough": "Bronx", "Zone": "Morrisania/Melrose", "service_zone": "Boro Zone", "lat": 40.824545, "lng": -73.9104143}
{"LocationID": "168", "Borough": "Bronx", "Zone": "Mott Haven/Port Morris", "service_zone": "Boro Zone", "lat": 40.8022025, "lng": -73.9166051}
{"LocationID": "169", "Borough": "Bronx", "Zone": "Mount Hope", "service_zone": "Boro Zone", "lat": 40.8488863, "lng": -73.9051185}
{"LocationID": "170", "Borough": "Manhattan", "Zone": "Murray Hill", "service_zone": "Yellow Zone", "lat": 40.7478792, "lng": -73.9756567}
{"LocationID": "171", "Borough": "Queens", "Zone": "Murray Hill-Queens", "service_zone": "Boro Zone", "lat": 40.7634996, "lng": -73.8073261}
{"LocationID": "172", "Borough": "Staten Island", "Zone": "New Dorp/Midland Beach", "service_zone": "Boro Zone", "lat": 40.5739937, "lng": -74.1159755}
{"LocationID": "173", "Borough": "Queens", "Zone": "North Corona", "service_zone": "Boro Zone", "lat": 40.7543725, "lng": -73.8669188}
{"LocationID": "174", "Borough": "Bronx", "Zone": "Norwood", "service_zone": "Boro Zone", "lat": 40.8810341, "lng": -73.878486}
{"LocationID": "175", "Borough": "Queens", "Zone": "Oakland Gardens", "service_zone": "Boro Zone", "lat": 40.7408584, "lng": -73.758241}
{"LocationID": "176", "Borough": "Staten Island", "Zone": "Oakwood", "service_zone": "Boro Zone", "lat": 40.563994, "lng": -74.1159754}
{"LocationID": "177", "Borough": "Brooklyn", "Zone": "Ocean Hill", "service_zone": "Boro Zone", "lat": 40.6782737, "lng": -73.9108212}
{"LocationID": "178", "Borough": "Brooklyn", "Zone": "Ocean Parkway South", "service_zone": "Boro Zone", "lat": 40.61287799999999, "lng": -73.96838620000001}
{"LocationID": "179", "Borough": "Queens", "Zone": "Old Astoria", "service_zone": "Boro Zone", "lat": 40.7643574, "lng": -73.92346189999999}
{"LocationID": "180", "Borough": "Queens", "Zone": "Ozone Park", "service_zone": "Boro Zone", "lat": 40.6794072, "lng": -73.8507279}
{"LocationID": "181", "Borough": "Brooklyn", "Zone": "Park Slope", "service_zone": "Boro Zone", "lat": 40.6710672, "lng": -73.98142279999999}
{"LocationID": "182", "Borough": "Bronx", "Zone": "Parkchester", "service_zone": "Boro Zone", "lat": 40.8382522, "lng": -73.8566087}
{"LocationID": "183", "Borough": "Bronx", "Zone": "Pelham Bay", "service_zone": "Boro Zone", "lat": 40.8505556, "lng": -73.83333329999999}
{"LocationID": "184", "Borough": "Bronx", "Zone": "Pelham Bay Park", "service_zone": "Boro Zone", "lat": 40.8670144, "lng": -73.81006339999999}
{"LocationID": "185", "Borough": "Bronx", "Zone": "Pelham Parkway", "service_zone": "Boro Zone", "lat": 40.8553279, "lng": -73.8639594}
{"LocationID": "186", "Borough": "Manhattan", "Zone": "Penn Station/Madison Sq West", "service_zone": "Yellow Zone", "lat": 40.7505045, "lng": -73.9934387}
{"LocationID": "187", "Borough": "Staten Island", "Zone": "Port Richmond", "service_zone": "Boro Zone", "lat": 40.63549140000001, "lng": -74.1254641}
{"LocationID": "188", "Borough": "Brooklyn", "Zone": "Prospect-Lefferts Gardens", "service_zone": "Boro Zone", "lat": 40.6592355, "lng": -73.9533895}
{"LocationID": "189", "Borough": "Brooklyn", "Zone": "Prospect Heights", "service_zone": "Boro Zone", "lat": 40.6774196, "lng": -73.9668408}
{"LocationID": "190", "Borough": "Brooklyn", "Zone": "Prospect Park", "service_zone": "Boro Zone", "lat": 40.6602037, "lng": -73.9689558}
{"LocationID": "191", "Borough": "Queens", "Zone": "Queens Village", "service_zone": "Boro Zone", "lat": 40.7156628, "lng": -73.7419017}
{"LocationID": "192", "Borough": "Queens", "Zone": "Queensboro Hill", "service_zone": "Boro Zone", "lat": 40.7429383, "lng": -73.8251741}
{"LocationID": "193", "Borough": "Queens", "Zone": "Queensbridge/Ravenswood", "service_zone": "Boro Zone", "lat": 40.7556711, "lng": -73.9456723}
{"LocationID": "194", "Borough": "Manhattan", "Zone": "Randalls Island", "service_zone": "Yellow Zone", "lat": 40.7932271, "lng": -73.92128579999999}
{"LocationID": "195", "Borough": "Brooklyn", "Zone": "Red Hook", "service_zone": "Boro Zone", "lat": 40.6733676, "lng": -74.00831889999999}
{"LocationID": "196", "Borough": "Queens", "Zone": "Rego Park", "service_zone": "Boro Zone", "lat": 40.72557219999999, "lng": -73.8624893}
{"LocationID": "197", "Borough": "Queens", "Zone": "Richmond Hill", "service_zone": "Boro Zone", "lat": 40.6958108, "lng": -73.8272029}
{"LocationID": "198", "Borough": "Queens", "Zone": "Ridgewood", "service_zone": "Boro Zone", "lat": 40.7043986, "lng": -73.9018292}
{"LocationID": "199", "Borough": "Bronx", "Zone": "Rikers Island", "service_zone": "Boro Zone", "lat": 40.79312770000001, "lng": -73.88601}
{"LocationID": "200", "Borough": "Bronx", "Zone": "Riverdale/North Riverdale/Fieldston", "service_zone": "Boro Zone", "lat": 40.89961830000001, "lng": -73.9088276}
{"LocationID": "201", "Borough": "Queens", "Zone": "Rockaway Park", "service_zone": "Boro Zone", "lat": 40.57978629999999, "lng": -73.8372237}
{"LocationID": "202", "Borough": "Manhattan", "Zone": "Roosevelt Island", "service_zone": "Boro Zone", "lat": 40.76050310000001, "lng": -73.9509934}
{"LocationID": "203", "Borough": "Queens", "Zone": "Rosedale", "service_zone": "Boro Zone", "lat": 40.6584068, "lng": -73.7389596}
{"LocationID": "204", "Borough": "Staten Island", "Zone": "Rossville/Woodrow", "service_zone": "Boro Zone", "lat": 40.5434385, "lng": -74.19764409999999}
{"LocationID": "205", "Borough": "Queens", "Zone": "Saint Albans", "service_zone": "Boro Zone", "lat": 40.6895283, "lng": -73.76436880000001}
{"LocationID": "206", "Borough": "Staten Island", "Zone": "Saint George/New Brighton", "service_zone": "Boro Zone", "lat": 40.6404369, "lng": -74.090226}
{"LocationID": "207", "Borough": "Queens", "Zone": "Saint Michaels Cemetery/Woodside", "service_zone": "Boro Zone", "lat": 40.7646761, "lng": -73.89850419999999}
{"LocationID": "208", "Borough": "Bronx", "Zone": "Schuylerville/Edgewater Park", "service_zone": "Boro Zone", "lat": 40.8235967, "lng": -73.81029269999999}
{"LocationID": "209", "Borough": "Manhattan", "Zone": "Seaport", "service_zone": "Yellow Zone", "lat": 40.70722629999999, "lng": -74.0027431}
{"LocationID": "210", "Borough": "Brooklyn", "Zone": "Sheepshead Bay", "service_zone": "Boro Zone", "lat": 40.5953955, "lng": -73.94575379999999}
{"LocationID": "211", "Borough": "Manhattan", "Zone": "SoHo", "service_zone": "Yellow Zone", "lat": 40.723301, "lng": -74.0029883}
{"LocationID": "212", "Borough": "Bronx", "Zone": "Soundview/Bruckner", "service_zone": "Boro Zone", "lat": 40.8247566, "lng": -73.8710929}
{"LocationID": "213", "Borough": "Bronx", "Zone": "Soundview/Castle Hill", "service_zone": "Boro Zone", "lat": 40.8176831, "lng": -73.8507279}
{"LocationID": "214", "Borough": "Staten Island", "Zone": "South Beach/Dongan Hills", "service_zone": "Boro Zone", "lat": 40.5903824, "lng": -74.06680759999999}
{"LocationID": "215", "Borough": "Queens", "Zone": "South Jamaica", "service_zone": "Boro Zone", "lat": 40.6808594, "lng": -73.7919103}
{"LocationID": "216", "Borough": "Queens", "Zone": "South Ozone Park", "service_zone": "Boro Zone", "lat": 40.6764003, "lng": -73.8124984}
{"LocationID": "217", "Borough": "Brooklyn", "Zone": "South Williamsburg", "service_zone": "Boro Zone", "lat": 40.7043921, "lng": -73.9565551}
{"LocationID": "218", "Borough": "Queens", "Zone": "Springfield Gardens North", "service_zone": "Boro Zone", "lat": 40.6715916, "lng": -73.779798}
{"LocationID": "219", "Borough": "Queens", "Zone": "Springfield Gardens South", "service_zone": "Boro Zone", "lat": 40.6715916, "lng": -73.779798}
{"LocationID": "220", "Borough": "Bronx", "Zone": "Spuyten Duyvil/Kingsbridge", "service_zone": "Boro Zone", "lat": 40.8833912, "lng": -73.9051185}
{"LocationID": "221", "Borough": "Staten Island", "Zone": "Stapleton", "service_zone": "Boro Zone", "lat": 40.6264929, "lng": -74.07764139999999}
{"LocationID": "222", "Borough": "Brooklyn", "Zone": "Starrett City", "service_zone": "Boro Zone", "lat": 40.6484272, "lng": -73.88236119999999}
{"LocationID": "223", "Borough": "Queens", "Zone": "Steinway", "service_zone": "Boro Zone", "lat": 40.7745459, "lng": -73.9037477}
{"LocationID": "224", "Borough": "Manhattan", "Zone": "Stuy Town/Peter Cooper Village", "service_zone": "Yellow Zone", "lat": 40.7316903, "lng": -73.9778494}
{"LocationID": "225", "Borough": "Brooklyn", "Zone": "Stuyvesant Heights", "service_zone": "Boro Zone", "lat": 40.6824166, "lng": -73.9319933}
{"LocationID": "226", "Borough": "Queens", "Zone": "Sunnyside", "service_zone": "Boro Zone", "lat": 40.7432759, "lng": -73.9196324}
{"LocationID": "227", "Borough": "Brooklyn", "Zone": "Sunset Park East", "service_zone": "Boro Zone", "lat": 40.65272, "lng": -74.00933479999999}
{"LocationID": "228", "Borough": "Brooklyn", "Zone": "Sunset Park West", "service_zone": "Boro Zone", "lat": 40.65272, "lng": -74.00933479999999}
{"LocationID": "229", "Borough": "Manhattan", "Zone": "Sutton Place/Turtle Bay North", "service_zone": "Yellow Zone", "lat": 40.7576281, "lng": -73.961698}
{"LocationID": "230", "Borough": "Manhattan", "Zone": "Times Sq/Theatre District", "service_zone": "Yellow Zone", "lat": 40.759011, "lng": -73.9844722}
{"LocationID": "231", "Borough": "Manhattan", "Zone": "TriBeCa/Civic Center", "service_zone": "Yellow Zone", "lat": 40.71625299999999, "lng": -74.0122396}
{"LocationID": "232", "Borough": "Manhattan", "Zone": "Two Bridges/Seward Park", "service_zone": "Yellow Zone", "lat": 40.7149056, "lng": -73.98924699999999}
{"LocationID": "233", "Borough": "Manhattan", "Zone": "UN/Turtle Bay South", "service_zone": "Yellow Zone", "lat": 40.7571432, "lng": -73.9718815}
{"LocationID": "234", "Borough": "Manhattan", "Zone": "Union Sq", "service_zone": "Yellow Zone", "lat": 40.7358633, "lng": -73.9910835}
{"LocationID": "235", "Borough": "Bronx", "Zone": "University Heights/Morris Heights", "service_zone": "Boro Zone", "lat": 40.8540855, "lng": -73.9198498}
{"LocationID": "236", "Borough": "Manhattan", "Zone": "Upper East Side North", "service_zone": "Yellow Zone", "lat": 40.7600931, "lng": -73.9598414}
{"LocationID": "237", "Borough": "Manhattan", "Zone": "Upper East Side South", "service_zone": "Yellow Zone", "lat": 40.7735649, "lng": -73.9565551}
{"LocationID": "238", "Borough": "Manhattan", "Zone": "Upper West Side North", "service_zone": "Yellow Zone", "lat": 40.7870106, "lng": -73.9753676}
{"LocationID": "239", "Borough": "Manhattan", "Zone": "Upper West Side South", "service_zone": "Yellow Zone", "lat": 40.7870106, "lng": -73.9753676}
{"LocationID": "240", "Borough": "Bronx", "Zone": "Van Cortlandt Park", "service_zone": "Boro Zone", "lat": 40.8972233, "lng": -73.8860668}
{"LocationID": "241", "Borough": "Bronx", "Zone": "Van Cortlandt Village", "service_zone": "Boro Zone", "lat": 40.8837203, "lng": -73.89313899999999}
{"LocationID": "242", "Borough": "Bronx", "Zone": "Van Nest/Morris Park", "service_zone": "Boro Zone", "lat": 40.8459682, "lng": -73.8625946}
{"LocationID": "243", "Borough": "Manhattan", "Zone": "Washington Heights North", "service_zone": "Boro Zone", "lat": 40.852476, "lng": -73.9342996}
{"LocationID": "244", "Borough": "Manhattan", "Zone": "Washington Heights South", "service_zone": "Boro Zone", "lat": 40.8417082, "lng": -73.9393554}
{"LocationID": "245", "Borough": "Staten Island", "Zone": "West Brighton", "service_zone": "Boro Zone", "lat": 40.6270298, "lng": -74.10931409999999}
{"LocationID": "246", "Borough": "Manhattan", "Zone": "West Chelsea/Hudson Yards", "service_zone": "Yellow Zone", "lat": 40.7542535, "lng": -74.0023331}
{"LocationID": "247", "Borough": "Bronx", "Zone": "West Concourse", "service_zone": "Boro Zone", "lat": 40.8316761, "lng": -73.9227554}
{"LocationID": "248", "Borough": "Bronx", "Zone": "West Farms/Bronx River", "service_zone": "Boro Zone", "lat": 40.8430609, "lng": -73.8816001}
{"LocationID": "249", "Borough": "Manhattan", "Zone": "West Village", "service_zone": "Yellow Zone", "lat": 40.73468, "lng": -74.0047554}
{"LocationID": "250", "Borough": "Bronx", "Zone": "Westchester Village/Unionport", "service_zone": "Boro Zone", "lat": 40.8340447, "lng": -73.8531349}
{"LocationID": "251", "Borough": "Staten Island", "Zone": "Westerleigh", "service_zone": "Boro Zone", "lat": 40.616296, "lng": -74.1386767}
{"LocationID": "252", "Borough": "Queens", "Zone": "Whitestone", "service_zone": "Boro Zone", "lat": 40.7920449, "lng": -73.8095574}
{"LocationID": "253", "Borough": "Queens", "Zone": "Willets Point", "service_zone": "Boro Zone", "lat": 40.7606911, "lng": -73.840436}
{"LocationID": "254", "Borough": "Bronx", "Zone": "Williamsbridge/Olinville", "service_zone": "Boro Zone", "lat": 40.8787602, "lng": -73.85283559999999}
{"LocationID": "255", "Borough": "Brooklyn", "Zone": "Williamsburg (North Side)", "service_zone": "Boro Zone", "lat": 40.71492, "lng": -73.9528472}
{"LocationID": "256", "Borough": "Brooklyn", "Zone": "Williamsburg (South Side)", "service_zone": "Boro Zone", "lat": 40.70824229999999, "lng": -73.9571487}
{"LocationID": "257", "Borough": "Brooklyn", "Zone": "Windsor Terrace", "service_zone": "Boro Zone", "lat": 40.6539346, "lng": -73.9756567}
{"LocationID": "258", "Borough": "Queens", "Zone": "Woodhaven", "service_zone": "Boro Zone", "lat": 40.6901366, "lng": -73.8566087}
{"LocationID": "259", "Borough": "Bronx", "Zone": "Woodlawn/Wakefield", "service_zone": "Boro Zone", "lat": 40.8955885, "lng": -73.8627133}
{"LocationID": "260", "Borough": "Queens", "Zone": "Woodside", "service_zone": "Boro Zone", "lat": 40.7532952, "lng": -73.9068973}
{"LocationID": "261", "Borough": "Manhattan", "Zone": "World Trade Center", "service_zone": "Yellow Zone", "lat": 40.7118011, "lng": -74.0131196}
{"LocationID": "262", "Borough": "Manhattan", "Zone": "Yorkville East", "service_zone": "Yellow Zone", "lat": 40.7762231, "lng": -73.94920789999999}
{"LocationID": "263", "Borough": "Manhattan", "Zone": "Yorkville West", "service_zone": "Yellow Zone", "lat": 40.7762231, "lng": -73.94920789999999}
"""

if __name__ == '__main__':
    format_and_split_csv()
