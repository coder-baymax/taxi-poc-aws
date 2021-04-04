from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, first, sum, lit
from pyspark.sql.types import DoubleType, StructType, IntegerType, StructField, StringType, LongType

# for local test
input_path = "/home/yunfei/aws/taxi-agg-record/{}"


# for s3 pre-treatment
# input_path = "s3://taxi-poc-batch-record/"


def write_agg_result_to_es():
    spark = SparkSession.builder.getOrCreate()

    for year in range(2009, 2021):
        for month in range(1, 13):
            month_str = "%d-%02d" % (year, month)
            df = spark.read.json(input_path.format(month_str))
            df.show()

    spark.stop()


if __name__ == '__main__':
    write_agg_result_to_es()
