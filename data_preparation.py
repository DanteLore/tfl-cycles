import calendar
import json
import os
import traceback
from datetime import datetime

import pandas as pd
import requests


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


def load_data(data_files):
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


def get_remote_files_from_index(url):
    file_list_json = json.loads(requests.get(url).content)
    urls = [f["url"] for f in file_list_json["entries"]]
    return [f.replace("s3:", "https:") for f in urls if f.endswith("csv")]


def main():
    bike_points_url = "https://api.tfl.gov.uk/bikepoint"
    index_url = "https://cycling.data.tfl.gov.uk/usage-stats/cycling-load.json"
    pickle_file = "data/bikes.pkl"
    raw_trip_dir = "data/raw_trip"

    if not os.path.exists(raw_trip_dir):
        os.makedirs(raw_trip_dir)

    bike_stations = download_bike_stations(bike_points_url)

    print("Loaded bike stations:")
    print(bike_stations)

    remote_files = get_remote_files_from_index(index_url)[:1]

    if os.path.isfile(pickle_file):
        trip_data = pd.read_pickle(pickle_file)
    else:
        data_files = list(download_data_files(remote_files, raw_trip_dir))

        trip_data = load_data(data_files)
        trip_data.to_pickle(pickle_file)

    print(trip_data.head(10))


if __name__ == "__main__":
    main()
