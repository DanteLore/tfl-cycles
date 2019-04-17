import json
import requests
import pandas as pd
from pyproj import Proj, transform


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

        out_proj = Proj('+init=EPSG:27700')
        in_proj = Proj('+init=EPSG:4326')
        (osgb_x, osgb_y) = transform(in_proj, out_proj, longitude, latitude)

        for x in p["additionalProperties"]:
            if x["key"] == "NbDocks":
                num_docks = x["value"]
            if x["key"] == "NbBikes":
                num_bikes = x["value"]
            if x["key"] == "NbEmptyDocks":
                num_empty = x["value"]

        data.append([bp_id, name, latitude, longitude, osgb_x, osgb_y, num_docks, int(num_bikes), num_empty])

    cols = ['id', 'name', 'latitude', 'longitude', 'osgb_x', 'osgb_y', 'num_docks', 'num_bikes', 'num_empty']
    return pd.DataFrame(data, columns=cols)


if __name__ == "__main__":
    BIKE_POINTS_URL = "https://api.tfl.gov.uk/bikepoint"
    BIKE_POINTS_FILE = "../data/bike-points.csv"

    print('Downloading {0} to {1}'.format(BIKE_POINTS_URL, BIKE_POINTS_FILE))
    bike_stations = download_bike_stations(BIKE_POINTS_URL)
    bike_stations.to_csv(BIKE_POINTS_FILE)
    print('Done!')
