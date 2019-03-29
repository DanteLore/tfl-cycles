import os
import zipfile
import requests


def download_file(url, filename):
    if os.path.isfile(filename):
        print("File already downloaded: " + filename)
    else:
        print("Downloading file: " + filename)
        r = requests.get(url, allow_redirects=True)
        open(filename, 'wb').write(r.content)


if __name__ == "__main__":
    MAP_DIR = "../data/maps/"
    MAP_DATA_URL = "https://data.london.gov.uk/download/statistical-gis-boundary-files-london/9ba8c833-6370-4b11-abdc-314aa020d5e0/statistical-gis-boundaries-london.zip"

    if not os.path.exists(MAP_DIR):
        os.makedirs(MAP_DIR)

    zip_file = MAP_DIR + "london-wards.zip"
    download_file(MAP_DATA_URL, zip_file)

    with zipfile.ZipFile(zip_file, "r") as zip_ref:
        zip_ref.extractall(MAP_DIR)
