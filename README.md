# tfl-cycles
Messing around with the TFL cycles data

### Bike Points Data

Bike points data can be downloaded from `https://api.tfl.gov.uk/bikepoint`.  The data is 'live' with real time counts
for number of available bikes, number of empty slots etc.  See `bike-points.py` for example code for downloading and
reading the data.

### Trip Data

Historical trip data for all TFL cycle journeys is available from 
`https://s3-eu-west-1.amazonaws.com/cycling.data.tfl.gov.uk/`.  To avoid using AWS credentials etc, in 
`download-trip-data.py`, I download the bucket listing as XML, extract the full list of filenames and select the ones
I want using a regular expression.

`prepare-trip-data.py` can be used to parse the downloaded CSV files.  It uses PySpark to read and clean the files
before storing to parquet format.  The schema of the files has changed several times over the years, so needs to be 
loaded with the `PERMISSIVE` flag set.  The `Duration` column was renamed `Duration_Seconds` at some point, and several
_additional_ columns were added on the right.

To help make aggregations easier, start and end timestamps are exploded out to multiple columns - a unix timestamp 
column and derived columns for `year`, `month`, `day` (of month), `day_of_week`.  This cuts down on the need for 
date time manipulation in notebooks.

Note that parquet data is deleted and recreated each time `prepare-trip-data.py` is run.

### Jupyter Notebooks

You can host these inside PyCharm, which is handy

https://www.jetbrains.com/help/pycharm/using-ipython-notebook-with-product.html
