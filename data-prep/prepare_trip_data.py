import os
import shutil
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def load_data(data_dir):
    schema = T.StructType([
        T.StructField("Rental Id",          T.IntegerType(),    False),
        T.StructField("Duration",           T.IntegerType(),    False),
        T.StructField("Bike Id",            T.IntegerType(),    False),
        T.StructField("End Date",           T.StringType(),     False),
        T.StructField("EndStation Id",      T.LongType(),       False),
        T.StructField("EndStation Name",    T.StringType(),     False),
        T.StructField("Start Date",         T.StringType(),     False),
        T.StructField("StartStation Id",    T.LongType(),       False),
        T.StructField("StartStation Name",  T.StringType(),     False)
    ])

    df = spark.read.csv(
        path="{0}/*.csv".format(data_dir),
        header=True,
        schema=schema,
        mode="PERMISSIVE"
    )

    df = df\
        .withColumnRenamed("Rental Id", "rental_id")\
        .withColumnRenamed("Duration", "duration")\
        .withColumnRenamed("Bike Id", "bike_id")\
        .withColumnRenamed("End Date", "end_date")\
        .withColumnRenamed("EndStation Id", "end_station_id")\
        .withColumnRenamed("EndStation Name", "end_station_name")\
        .withColumnRenamed("Start Date", "start_date")\
        .withColumnRenamed("StartStation Id", "start_station_id")\
        .withColumnRenamed("StartStation Name", "start_station_name")

    df = explode_date_str(df, "start_date", "start_")
    df = explode_date_str(df, "end_date", "end_")

    return df


def explode_date_str(df, input_field, prefix):
    ts_field = prefix + 'ts'

    df = df.withColumn(
        ts_field,
        F.to_timestamp(input_field, 'dd/MM/yyyy HH:mm')
    )
    df = df.drop(input_field)
    df = df\
        .withColumn(prefix + 'year', F.year(ts_field)) \
        .withColumn(prefix + 'month', F.month(ts_field)) \
        .withColumn(prefix + 'day', F.dayofmonth(ts_field)) \
        .withColumn(prefix + 'day_of_week', F.dayofweek(ts_field)) \
        .withColumn(prefix + 'hour', F.hour(ts_field))
    return df


def filter_data(trip_data):
    return (trip_data
                .where(F.col("Duration") > 0)
                .dropDuplicates()
            )


def main():
    output_dir = "../data/parquet_trip"
    raw_trip_dir = "../data/raw_trip"

    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)

    trip_data = load_data(raw_trip_dir)
    trip_data = filter_data(trip_data)
    trip_data.write.parquet(output_dir)

    print("Rows: " + str(trip_data.count()))
    trip_data.limit(10).show()


if __name__ == "__main__":
    spark = (
        SparkSession.builder
            .master("local")
            .appName("TFL Data Prep")
            .config("spark.executor.memory", "4g")
            .getOrCreate()
    )

    start = time.time()
    main()
    end = time.time()
    print("Data prep took {0:06.2f}s".format(end - start))
