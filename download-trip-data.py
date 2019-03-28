import os
import re

import requests
import xmltodict


def download_file(url, filename):
    if os.path.isfile(filename):
        print("File already downloaded: " + filename)
    else:
        print("Downloading file: " + filename)
        r = requests.get(url, allow_redirects=True)
        open(filename, 'wb').write(r.content)


def read_index(index_file):
    with open(index_file) as fd:
        doc = xmltodict.parse(fd.read())

    pattern = re.compile("^usage-stats/.*csv$")

    entries = [item['Key'] for item in doc['ListBucketResult']['Contents']]
    return [f for f in entries if pattern.match(f)]


if __name__ == "__main__":
    BUCKET_URL = "https://s3-eu-west-1.amazonaws.com/cycling.data.tfl.gov.uk/"
    INDEX_FILE = "data/file-index.xml"
    RAW_TRIP_DIR = "data/raw_trip/"

    if not os.path.exists(RAW_TRIP_DIR):
        os.makedirs(RAW_TRIP_DIR)

    download_file(BUCKET_URL, INDEX_FILE)

    files = read_index(INDEX_FILE)

    for file in files:
        local_file = RAW_TRIP_DIR + file.split('/')[-1].replace(' ', '_')
        url = BUCKET_URL + file

        download_file(url, local_file)
