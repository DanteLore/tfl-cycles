import json
import requests

import pandas as pd


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


if __name__ == "__main__":
    bike_points_url = "https://api.tfl.gov.uk/bikepoint"

    bike_stations = download_bike_stations(bike_points_url)
    print("Loaded bike stations:")
    print(bike_stations)
