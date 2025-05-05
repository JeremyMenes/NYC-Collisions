# NYC-Collisions

Welcome to my repository! This is a personal data project where I explored and analyzed auto accidents in New York City from 2013-2023. The final results of this project have been published to my [Tableau Public](https://public.tableau.com/app/profile/jeremymenes/viz/NYCCollisions_17336129497660/Dashboard2?publish=yes)

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
