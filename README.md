# NYC-Collisions

Welcome! This is a personal data project exploring & analyzing auto accidents in New York City from 2013-2023. The final results of this project have been published to my [Tableau Public](https://public.tableau.com/app/profile/jeremymenes/viz/NYCCollisions_17336129497660/Dashboard2?publish=yes)

The primary purpose of this repository is to showcase my skills with end-to-end data handling, cleaning, and analysis using SQL and Python. Secondly, this will also serve as documentation of my changes to the raw dataset and the methodologies I used to clean and manipulate the data.

  
## Project Overview

**Data Source:**  
- The primary dataset for this project was obtained from the [City of New York OpenData website](https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95/about_data)  
- Weather data for NYC was obtained from [NOAA Climate Data Online](https://www.ncdc.noaa.gov/cdo-web/datasets)

**Data Size/Scope:**  
- The NYC Motor Collisions dataset contains over 2 million rows, each representing individual auto accidents across the five boroughs of NYC over a ten-year period.  
- The NOAA dataset contains over 300,000 rows of daily weather readings, from 220 individual weather stations in the greater NYC area.

**SQL for Data Cleaning:**  
- I wrote a series of custom SQL queries (available in this repo) to clean, standardize, and correct data inconsistencies directly within the database environment. 

**Python for Integration and Analysis:**  
- To explore the impact of weather conditions on auto accidents, I used Python to combine the two datasets. This involved a complex Python script that matched each collision to the closest weather station with full weather readings for the date of the crash.

**Database Management:**  
- I began this project using a local Microsoft SQL Server instance to manipulate the NYC Collision data. As the project grew in complexity, I migrated to a Docker MySQL server for improved flexibility and portability. The MySQL server is now accessed and queried through PyCharm, with integrated Python scripts for analysis.

  
## Project Highlights

### Weather Data from NOAA
  Combining the two datasets was the most technical hurdle of the project. The problem is not as simple as finding out "What was the weather like in NYC on the date of the accident?"...which part of NYC? From its furthest points, NYC is 35 miles long and encompasses over 300 square miles. Another obstacle is that the NOAA data contains weather readings from 220 individual weather stations across the greater NYC area - and many of the smaller weather stations had massive holes in their historical data. To start, I wrote the following Python script to return a list of weather stations that had over 95% of non-Null daily weather data over the 10-year period:
  
<pre>import pandas as pd

with open(r"[NOAA_WeatherData.csv", encoding='utf-8-sig') as Weather_table:
    Stations = pd.read_csv(Weather_table)

# Set the completeness threshold as a %
completeness_threshold = 0.95

# The data I am primarily interested in: temperature, precipitation and snow
required_columns = ['PRCP', 'TMAX', 'TMIN', 'SNOW']

# Group by station
station_groups = Stations.groupby('NAME')

# List to collect stations that meet the threshold
qualified_stations = []

# Loop through each station
for station, group in station_groups:
    total_records = len(group)
    # Count rows that have all required columns non-null
    complete_records = group[required_columns].dropna().shape[0]
    completeness_ratio = complete_records / total_records if total_records > 0 else 0

    if completeness_ratio >= completeness_threshold:
        qualified_stations.append((station, completeness_ratio))

#print the results
result_df = pd.DataFrame(qualified_stations, columns=['NAME', 'Completeness'])
print(result_df.sort_values(by='Completeness', ascending=False))</pre>

This returned the following list of weather stations which unsurprisingly contained the 3 major NYC airports and the Central Park weather station, the largest weather stations in the greater NYC area. 

| NAME | Completeness |
| --- | --- |
| JFK INTERNATIONAL AIRPORT, NY US | 1.000000 |
| NEWARK LIBERTY INTERNATIONAL AIRPORT, NJ US | 1.000000 |
| LAGUARDIA AIRPORT, NY US | 1.000000 |
| NY CITY CENTRAL PARK, NY US | 1.000000 |
| LONG BRANCH OAKHURST, NJ US | 0.986291 |
| CENTERPORT, NY US | 0.982268 |
| BOONTON 1 SE, NJ US | 0.977084 |

I then took the NOAA weather data extract and filtered it for data from these stations only. This ensured I had nearly 100% complete data, and the rest I would eventually fill in with average data for the date from these stations.  

I decided to go with this method, recognizing the potential inaccuracies with extrapolating and filling in missing data with averages, as my goal with the project is to highlight *overall trends* in relation to auto accidents and weather conditions. In a different setting, my next Python script would be more complex and would involve extra logical checks for not just the closest weather station to the auto accident, but checking if that weather station had complete data, and if not, checking the next closest weather station, and so on. With 220 total weather stations and 1.75 million rows of collision data with latitude/longitude coordinates, this script would have taken a very long time to run and I decided to focus my time on the core goals of the project.

After filtering for completed weather data, the next step was to add weather data from the closest weather station to each accident in the NYC Collision dataset. I started by filtering the NYC Collisions dataset for accidents with non-Null and valid Latitude/Longitude coordinates (meaning coordinates that were actually *within* the city limits) which resulted in about 75%, or 1.75 million rows from the original dataset. I wrote the following Python script to accomplish this, which involved the use of the Haversine formula to find the distance between two sets of Latitude and Longitude coordinates (Both the NYC Collisions dataset and the NOAA Weather Station dataset contained coordinates for each accident and weather station)

<pre>import pandas as pd
import logging
from math import radians, cos, sin, asin, sqrt

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#logger.info('Starting script')

# Load data
#logger.debug('Loading and processing data')
with open(r"NYC_CollisionsData", encoding='utf-8-sig') as Coll_Table:
    NYCdb = pd.read_csv(Coll_Table)
with open(r"NOAA Weather Data 95perc Accuracy.csv", encoding='utf-8-sig') as Weather_table:
    Stations = pd.read_csv(Weather_table)

# Rename columns
NYCdb = NYCdb.rename(columns={'LATITUDE': 'NYCLat', 'LONGITUDE': 'NYCLong'})
Stations = Stations.rename(columns={'LATITUDE': 'StationLat', 'LONGITUDE': 'StationLong'})
#logger.debug('Columns renamed')

# Fix date formats
NYCdb['CRASH_DATE'] = pd.to_datetime(NYCdb['CRASH_DATE']).dt.date
Stations['DATE'] = pd.to_datetime(Stations['DATE']).dt.date
#logger.debug('Dates reformatted')

#Sort data by Date, allows caching by groups of same dates
NYCdb.sort_values(by='CRASH_DATE', inplace=True)
Stations.sort_values(by='DATE', inplace=True)
#logger.debug('Data sorted by date')

#create the date cache
date_cache = {}

# function to apply Haversine formula to find the distance between two sets of latitude/longitude coordinates:
class Coordinates:
    def __init__(self, lat, lon):
        self.lat = radians(lat)
        self.lon = radians(lon)

    def __repr__(self):
        return f"Coordinates(lat={self.lat}, lon={self.lon})"

    # Haversine formula
    def __sub__(self, other):
        diff_lon = self.lon - other.lon
        diff_lat = self.lat - other.lat
        a = sin(diff_lat / 2) ** 2 + cos(other.lat) * cos(self.lat) * sin(diff_lon / 2) ** 2
        c = 2 * asin(sqrt(a))
        return 6371 * c

# Main function, returns the closest weather station that has a weather reading
# for the date in the current row in NYCdb
def find_nearest_station_by_date(lat, lon, date):
    #logger.info(f'Finding nearest station for lat: {lat}, lon: {lon}, date: {date}')

    # logical checks are in the order of most likely -> least likely to occur:
    # First checks if Date is already in cache, which happens most often
    # Second, checks if the date is a date, but a different one (when the code encounters a new date in the sorted table)
    # Then if that new date is empty for some reason, due to the row not having a date or no stations were found for the date, return None
    if date in date_cache:
        stations_on_date = date_cache[date]

    else:
        stations_on_date = Stations[Stations['DATE'] == date]
        date_cache.clear()  # only cache one date at a time to conserve memory
        date_cache[date] = stations_on_date

        if stations_on_date.empty:
            #logger.warning(f'No stations found for date: {date}')
            return None

        elif not date:
            #logger.warning('Date is None, skipping row')
            return None

    # Find the distances of all stations that have weather readings for the current date
    src_coords = Coordinates(lat, lon)
    distances = stations_on_date.apply(
        lambda row: Coordinates(row['StationLat'], row['StationLong']) - src_coords, axis=1)

    # Then, return the station with the shortest distance
    closest_idx = distances.idxmin()
    station_name = stations_on_date.loc[closest_idx, 'NAME']
    #logger.info(f'Found nearest station: {station_name} for date: {date}')
    return stations_on_date.loc[closest_idx, 'NAME']

# Applies the main function to the NYCdb table, and adds the returned Station name
# to the current row in a new column 'Closest_Station'
NYCdb['Closest_Station'] = NYCdb.apply(
    lambda row: find_nearest_station_by_date(row['NYCLat'], row['NYCLong'], row['CRASH_DATE']), axis=1)

# Save to CSV
logger.info('Saving to CSV')
NYCdb.to_csv("output.csv", index=False, sep=',', encoding='utf-8')
logger.info('done')</pre>

With the NYC Collisions dataset now containing a new column containing the name of the closest weather station (with weather readings) on the date of the crash, I then added the rest of the weather data using a simple JOIN in SQL:

<pre>UPDATE TargetTable
SET 
TargetTable.[Precipitation] = WeatherTable.[Precipitation],
TargetTable.[Snowfall] = WeatherTable.[Snowfall],
TargetTable.[Snow Depth] = WeatherTable.[Snow_Depth],
TargetTable.[Fog] = WeatherTable.[Fog],
TargetTable.[Heavy Fog] = WeatherTable.[Heavy_Fog],
TargetTable.[Thunder] = WeatherTable.[Thunder],
TargetTable.[Sleet] = WeatherTable.[Sleet],
TargetTable.[Hail] = WeatherTable.[Hail],
TargetTable.[Glaze/Rime] = WeatherTable.[Glaze/Rime],
TargetTable.[Smoke/Haze] = WeatherTable.[Smoke/Haze],
TargetTable.[HighWinds] = WeatherTable.High_Winds],
TargetTable.[Avg Temp] = WeatherTable.[Avg_Temp],
TargetTable.[Max Temp] = WeatherTable.[Max_Temp],
TargetTable.[Min Temp] = WeatherTable.[Min_Temp]
FROM [Motor_Vehicle_Collisions_-_Crashes] TargetTable
JOIN [NOAA Weather Data 95perc Accuracy] WeatherTable
    ON TargetTable.Closest_Station = WeatherTable.NAME
    AND TargetTable.CRASH_DATE = WeatherTable.DATE</pre>
