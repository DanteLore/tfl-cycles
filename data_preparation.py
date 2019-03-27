import json
import os
import shutil
import time

import pandas as pd
import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def download_bike_stations(url):
    response = requests.get(url)
    parsed = json.loads(response.content)
    data = []
    for p in parsed:
        bp_id = int(p["id"].replace("BikePoints_", ""))
        name = p["commonName"]
        latitude = p["lat"]
        longitude = p["lon"]
        num_docks = 0
        num_bikes = 0
        num_empty = 0

        for x in p["additionalProperties"]:
            if x["key"] == "NbDocks":
                num_docks = x["value"]
            if x["key"] == "NbBikes":
                num_bikes = x["value"]
            if x["key"] == "NbEmptyDocks":
                num_empty = x["value"]

        data.append([bp_id, name, latitude, longitude, num_docks, int(num_bikes), num_empty])

    cols = ['id', 'name', 'latitude', 'longitude', 'num_docks', 'num_bikes', 'num_empty']
    return pd.DataFrame(data, columns=cols)


def download_data_files(remote_files, cache_dir):
    for url in remote_files:
        filename = cache_dir + '/' + url.split('/')[-1].replace(' ', '_')

        if os.path.isfile(filename):
            print("File already downloaded: " + filename)
        else:
            print("Downloading file: " + filename)
            r = requests.get(url, allow_redirects=True)
            open(filename, 'wb').write(r.content)

        yield filename


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
        mode="DROPMALFORMED"
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


def get_remote_files_from_index(url):
    file_list_json = json.loads(requests.get(url).content)
    urls = [f["url"] for f in file_list_json["entries"]]
    return [f.replace("s3:", "https:") for f in urls if f.endswith("csv")]


def filter_data(trip_data):
    return trip_data.where(F.col("Duration") > 0)


def main():
    bike_points_url = "https://api.tfl.gov.uk/bikepoint"
    # NOTE:  Looks like this index file is not complete :(
    index_url = "https://cycling.data.tfl.gov.uk/usage-stats/cycling-load.json"
    output_dir = "data/parquet_trip"
    raw_trip_dir = "data/raw_trip"

    if not os.path.exists(raw_trip_dir):
        os.makedirs(raw_trip_dir)

    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)

    bike_stations = download_bike_stations(bike_points_url)
    print("Loaded bike stations:")
    print(bike_stations)

    remote_files = get_remote_files_from_index(index_url)
    download_data_files(remote_files, raw_trip_dir)

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
