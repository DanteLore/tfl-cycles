import os
import requests
import re

HEATHROW_URL= "https://www.metoffice.gov.uk/pub/data/weather/uk/climate/stationdata/heathrowdata.txt"
WEATHER_DIR = "../data/weather"
HEATHROW_FILE = WEATHER_DIR + "/heathrow.csv"
HEADER ="year,month,maximum_temp,minimum_temp,days_of_air_frost,total_rainfall,total_sunshine\n"

if __name__ == "__main__":
    if not os.path.exists(WEATHER_DIR):
        os.makedirs(WEATHER_DIR)

    print("Downloading file: " + HEATHROW_URL)
    r = requests.get(HEATHROW_URL, allow_redirects=True, stream=True)

    decoded = [l.decode('utf-8') for l in r.iter_lines()]

    r = r"^[\s]+([0-9]+)[\s]+([0-9]+)[\s]+([0-9.-]+)[\s]+([0-9.-]+)[\s]+([0-9.-]+)[\s]+([0-9.-]+)[\s]+([0-9.-]+).*$"

    matches = [re.match(r, l) for l in decoded]
    fields = [",".join(m.groups()) for m in matches if m]
    cleaned = [f.replace('-', '') for f in fields]
    text = HEADER + "\n".join(cleaned)

    open(HEATHROW_FILE, 'w').write(text)
