import calendar
import json
import os
import time
import traceback
from datetime import datetime

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


def date_str_to_datetime(date_str):
    try:
        return datetime.strptime(date_str[:16], '%d/%m/%Y %H:%M')
    except (ValueError, TypeError):
        return None


def datetime_to_unix_ts(dt):
    return int(dt.strftime("%s"))


def datetime_to_weekday(dt):
    return calendar.day_name[dt.weekday()]


def explode_date(df, date_field, prefix):
    df[prefix + '_dt'] = df[date_field].apply(date_str_to_datetime)
    df = df.dropna()

    split_df = df[date_field].str.split("[/ :]", expand=True)
    df[prefix + '_day'] = split_df[0]
    df[prefix + '_month'] = split_df[1]
    df[prefix + '_year'] = split_df[2]
    df[prefix + '_hour'] = split_df[3]
    df[prefix + '_day_of_week'] = df[prefix + '_dt'].apply(datetime_to_weekday)
    df[prefix + '_ts'] = df[prefix + '_dt'].apply(datetime_to_unix_ts)
    df.drop([date_field, prefix + '_dt'], axis=1)
    return df


def load_data_old(data_files):
    files = [f for f in data_files]
    dfs = []

    for f in files:
        print("Loading: " + f)
        raw = pd.read_csv(f, index_col=None, header=0)

        try:
            print(raw.columns)
            df = raw[['Bike Id', 'Duration', 'StartStation Id', 'EndStation Id', 'Start Date', 'End Date']]
            df = df.dropna()
            df.columns = ['bike_id', 'duration', 'start_station_id', 'end_station_id', 'start_str', 'end_str']
            df = explode_date(df, 'start_str', 'start')
            df = explode_date(df, 'end_str', 'end')
            dfs.append(df)
        except Exception:
            print(traceback.format_exc())
            continue

    return pd.concat(dfs, axis=0, ignore_index=True)


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

    df = df.withColumnRenamed("Rental Id", "rental_id")\
        .withColumnRenamed("Duration", "duration")\
        .withColumnRenamed("Bike Id", "bike_id")\
        .withColumnRenamed("End Date", "end_date")\
        .withColumnRenamed("EndStation Id", "end_station_id")\
        .withColumnRenamed("EndStation Name", "end_station_name")\
        .withColumnRenamed("Start Date", "start_date")\
        .withColumnRenamed("StartStation Id", "start_station_id")\
        .withColumnRenamed("StartStation Name", "start_station_name")

    return df


def get_remote_files_from_index(url):
    file_list_json = json.loads(requests.get(url).content)
    urls = [f["url"] for f in file_list_json["entries"]]
    return [f.replace("s3:", "https:") for f in urls if f.endswith("csv")]


def filter_data(trip_data):
    return trip_data.where(F.col("Duration") > 0)


def main():
    bike_points_url = "https://api.tfl.gov.uk/bikepoint"
    index_url = "https://cycling.data.tfl.gov.uk/usage-stats/cycling-load.json"
    output_dir = "data/parquet"
    raw_trip_dir = "data/raw_trip"

    if not os.path.exists(raw_trip_dir):
        os.makedirs(raw_trip_dir)

    if os.path.exists(output_dir):
        os.removedirs(output_dir)

    bike_stations = download_bike_stations(bike_points_url)

    print("Loaded bike stations:")
    print(bike_stations)

    remote_files = get_remote_files_from_index(index_url)
    download_data_files(remote_files, raw_trip_dir)

    trip_data = load_data(raw_trip_dir)
    trip_data = filter_data(trip_data)
    # trip_data.to_csv(output_file)

    # 23464034
    # 23293614
    print("Rows: " + str(trip_data.count()))
    trip_data.limit(10).show()
    trip_data.write.parquet(output_dir)


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
