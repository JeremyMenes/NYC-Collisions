--Update the Weather data table with 0 instead of NULL
UPDATE [Over 90 Accurate Weather Data] SET [High_Winds] = 0 WHERE [High_Winds] IS NULL;
UPDATE [Over 90 Accurate Weather Data] SET [Precipitation] = 0 WHERE [Precipitation] IS NULL;
UPDATE [Over 90 Accurate Weather Data] SET [Snowfall] = 0 WHERE [Snowfall] IS NULL;
UPDATE [Over 90 Accurate Weather Data] SET [Snow_Depth] = 0 WHERE [Snow_Depth] IS NULL;
UPDATE [Over 90 Accurate Weather Data] SET [Avg_Temp] = 0 WHERE [Avg_Temp] IS NULL;
UPDATE [Over 90 Accurate Weather Data] SET [Min_Temp] = 0 WHERE [Min_Temp] IS NULL;
UPDATE [Over 90 Accurate Weather Data] SET [Max_Temp] = 0 WHERE [Max_Temp] IS NULL;
UPDATE [Over 90 Accurate Weather Data] SET [Fog] = 0 WHERE [Fog] IS NULL;
UPDATE [Over 90 Accurate Weather Data] SET [Heavy_Fog] = 0 WHERE [Heavy_Fog] IS NULL;
UPDATE [Over 90 Accurate Weather Data] SET [Thunder] = 0 WHERE [Thunder] IS NULL;
UPDATE [Over 90 Accurate Weather Data] SET [Sleet] = 0 WHERE [Sleet] IS NULL;
UPDATE [Over 90 Accurate Weather Data] SET [Hail] = 0 WHERE [Hail] IS NULL;
UPDATE [Over 90 Accurate Weather Data] SET [Glaze/Rime] = 0 WHERE [Glaze/Rime] IS NULL;
UPDATE [Over 90 Accurate Weather Data] SET [Smoke/Haze] = 0 WHERE [Smoke/Haze] IS NULL;

--Finally, update the weather data table so that entries with null average temps and non-null min and max temps now
--have an average temp as an average of non-null min and max temp columns (this is technically inaccurate but will assist in showing overall trends
--in regards to collisions)
--All average temp entries of 0 were found to be non-valid temperatures, although min temperatures have reached below 0 in NYC, the average temperature
--has never reached 0 degress Farenheit from 2013-2023
UPDATE [Over 90 Accurate Weather Data] SET [Avg_Temp] = (([Max_Temp]+[Min_Temp])/2) WHERE [Avg_Temp] = 0;

--Add the closest weather stations from the csv returned from the "Find Closest Weather Station" Python script.
UPDATE TargetTable
SET TargetTable.Closest_Station = Stations.Closest_Station
FROM [Motor_Vehicle_Collisions_-_Crashes] TargetTable
JOIN [CollisionIDs with Closest Weather Station] Stations
	ON TargetTable.COLLISION_ID = Stations.COLLISION_ID

--With the closest weather station added for valid Collision IDs, we can now add weather data to the main Collisions table
--from the Weather Data table based on the weather station name and the date of the collision
UPDATE TargetTable
SET 
TargetTable.[Precipitation] = [Over 90 Accurate Weather Data].[Precipitation],
TargetTable.[Snowfall] = [Over 90 Accurate Weather Data].[Snowfall],
TargetTable.[Snow Depth] = [Over 90 Accurate Weather Data].[Snow_Depth],
TargetTable.[Fog] = [Over 90 Accurate Weather Data].[Fog],
TargetTable.[Heavy Fog] = [Over 90 Accurate Weather Data].[Heavy_Fog],
TargetTable.[Thunder] = [Over 90 Accurate Weather Data].[Thunder],
TargetTable.[Sleet] = [Over 90 Accurate Weather Data].[Sleet],
TargetTable.[Hail] = [Over 90 Accurate Weather Data].[Hail],
TargetTable.[Glaze/Rime] = [Over 90 Accurate Weather Data].[Glaze/Rime],
TargetTable.[Smoke/Haze] = [Over 90 Accurate Weather Data].[Smoke/Haze],
TargetTable.[HighWinds] = [Over 90 Accurate Weather Data].High_Winds,
TargetTable.[Avg Temp] = [Over 90 Accurate Weather Data].Avg_Temp,
TargetTable.[Max Temp] = [Over 90 Accurate Weather Data].Max_Temp,
TargetTable.[Min Temp] = [Over 90 Accurate Weather Data].Min_Temp
FROM [Motor_Vehicle_Collisions_-_Crashes] TargetTable
JOIN [Over 90 Accurate Weather Data]
    ON TargetTable.Closest_Station = [Over 90 Accurate Weather Data].NAME
    AND TargetTable.CRASH_DATE = [Over 90 Accurate Weather Data].DATE

--There are errors when trying to Join/overwrite data that is NULL. This will set any remaining NULL data to 0
--(Or -99 in the case of temperature readings, since 0 is a valid and possible temperature in NYC)
--Coordinates for collisions that were well outside of NYC will be filtered out of these update statements
--and will not have their weather data changed
UPDATE [Motor_Vehicle_Collisions_-_Crashes]
SET [Avg Temp] = -99
WHERE (
	[Avg Temp] IS NULL
	AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
	AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
	AND LATITUDE > 40
	AND LATITUDE < 41
	AND LONGITUDE > -74.5
	AND LONGITUDE < -73 )
OR (
	[Avg Temp] IS NULL
	AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
	AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
	AND LATITUDE IS NULL )
OR (
	[Avg Temp] IS NULL
	AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
	AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
	AND LATITUDE = 0 );

UPDATE [Motor_Vehicle_Collisions_-_Crashes]
SET [Min Temp] = -99
WHERE (
	[Min Temp] IS NULL
	AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
	AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
	AND LATITUDE > 40
	AND LATITUDE < 41
	AND LONGITUDE > -74.5
	AND LONGITUDE < -73 )
OR ( 
	[Min Temp] IS NULL
	AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
	AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
	AND LATITUDE IS NULL )
OR (
	[Min Temp] IS NULL
	AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
	AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
	AND LATITUDE = 0 );

UPDATE [Motor_Vehicle_Collisions_-_Crashes]
SET [Max Temp] = -99
WHERE (
	[Max Temp] IS NULL
	AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
	AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
	AND LATITUDE > 40
	AND LATITUDE < 41
	AND LONGITUDE > -74.5
	AND LONGITUDE < -73 )
OR (
	[Max Temp] IS NULL
	AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
	AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
	AND LATITUDE IS NULL )
OR (
	[Max Temp] IS NULL
	AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
	AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
	AND LATITUDE = 0 );

UPDATE [Motor_Vehicle_Collisions_-_Crashes]
SET [Fog] = 0
WHERE (
	[Fog] IS NULL
	AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
	AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
	AND LATITUDE > 40
	AND LATITUDE < 41
	AND LONGITUDE > -74.5
	AND LONGITUDE < -73 )
OR (
	[Fog] IS NULL
	AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
	AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
	AND LATITUDE IS NULL )
OR (
	[Fog] IS NULL
	AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
	AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
	AND LATITUDE = 0 );

UPDATE [Motor_Vehicle_Collisions_-_Crashes]
SET [Heavy Fog] = 0
WHERE (
	[Heavy Fog] IS NULL
	AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
	AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
	AND LATITUDE > 40
	AND LATITUDE < 41
	AND LONGITUDE > -74.5
	AND LONGITUDE < -73 )
OR (
	[Heavy Fog] IS NULL
	AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
	AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
	AND LATITUDE IS NULL )
OR (
	[Heavy Fog] IS NULL
	AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
	AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
	AND LATITUDE = 0 )

UPDATE [Motor_Vehicle_Collisions_-_Crashes]
SET [Precipitation] = 0
WHERE (
	[Precipitation] IS NULL
	AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
	AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
	AND LATITUDE > 40
	AND LATITUDE < 41
	AND LONGITUDE > -74.5
	AND LONGITUDE < -73 )
OR (
	[Precipitation] IS NULL
	AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
	AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
	AND LATITUDE IS NULL)
OR (
	[Precipitation] IS NULL
	AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
	AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
	AND LATITUDE = 0 );

UPDATE [Motor_Vehicle_Collisions_-_Crashes]
SET [Snowfall] = 0
WHERE ( 
	[Snowfall] IS NULL
	AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
	AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
	AND LATITUDE > 40
	AND LATITUDE < 41
	AND LONGITUDE > -74.5
	AND LONGITUDE < -73 )
OR ( 
	[Snowfall] IS NULL
	AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
	AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
	AND LATITUDE IS NULL )
OR (
	[Snowfall] IS NULL
	AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
	AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
	AND LATITUDE = 0 );

UPDATE [Motor_Vehicle_Collisions_-_Crashes]
SET [Snow Depth] = 0
WHERE ( 
	[Snow Depth] IS NULL
	AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
	AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
	AND LATITUDE > 40
	AND LATITUDE < 41
	AND LONGITUDE > -74.5
	AND LONGITUDE < -73 )
OR ( 
	[Snow Depth] IS NULL
	AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
	AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
	AND LATITUDE IS NULL )
OR (
	[Snow Depth] IS NULL
	AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
	AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
	AND LATITUDE = 0 );

UPDATE [Motor_Vehicle_Collisions_-_Crashes]
SET [Thunder] = 0
WHERE (
	[Thunder] IS NULL
	AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
	AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
	AND LATITUDE > 40
	AND LATITUDE < 41
	AND LONGITUDE > -74.5
	AND LONGITUDE < -73 )
OR ( 
	[Thunder] IS NULL
	AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
	AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
	AND LATITUDE IS NULL )
OR (
	[Thunder] IS NULL
	AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
	AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
	AND LATITUDE = 0 );

UPDATE [Motor_Vehicle_Collisions_-_Crashes]
SET [Sleet] = 0
WHERE (
	[Sleet] IS NULL
	AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
	AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
	AND LATITUDE > 40
	AND LATITUDE < 41
	AND LONGITUDE > -74.5
	AND LONGITUDE < -73 )
OR (
	[Sleet] IS NULL
	AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
	AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
	AND LATITUDE IS NULL)
OR (
	[Sleet] IS NULL
	AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
	AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
	AND LATITUDE = 0 );

UPDATE [Motor_Vehicle_Collisions_-_Crashes]
SET [Hail] = 0
WHERE (
	[Hail] IS NULL
	AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
	AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
	AND LATITUDE > 40
	AND LATITUDE < 41
	AND LONGITUDE > -74.5
	AND LONGITUDE < -73 )
OR (
	[Hail] IS NULL
	AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
	AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
	AND LATITUDE IS NULL)
OR (
	[Hail] IS NULL
	AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
	AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
	AND LATITUDE = 0 );

UPDATE [Motor_Vehicle_Collisions_-_Crashes]
SET [Glaze/Rime] = 0
WHERE (
	[Glaze/Rime] IS NULL
	AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
	AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
	AND LATITUDE > 40
	AND LATITUDE < 41
	AND LONGITUDE > -74.5
	AND LONGITUDE < -73 )
OR (
	[Glaze/Rime] IS NULL
	AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
	AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
	AND LATITUDE IS NULL)
OR (
	[Glaze/Rime] IS NULL
	AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
	AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
	AND LATITUDE = 0 );

UPDATE [Motor_Vehicle_Collisions_-_Crashes]
SET [Smoke/Haze] = 0
WHERE (
	[Smoke/Haze] IS NULL
	AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
	AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
	AND LATITUDE > 40
	AND LATITUDE < 41
	AND LONGITUDE > -74.5
	AND LONGITUDE < -73 )
OR (
	[Smoke/Haze] IS NULL
	AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
	AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
	AND LATITUDE IS NULL)
OR (
	[Smoke/Haze] IS NULL
	AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
	AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
	AND LATITUDE = 0 );

UPDATE [Motor_Vehicle_Collisions_-_Crashes]
SET [HighWinds] = 0
WHERE (
	[HighWinds] IS NULL
	AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
	AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
	AND LATITUDE > 40
	AND LATITUDE < 41
	AND LONGITUDE > -74.5
	AND LONGITUDE < -73 )
OR (
	[HighWinds] IS NULL
	AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
	AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
	AND LATITUDE IS NULL)
OR (
	[HighWinds] IS NULL
	AND [CRASH_DATE] < '2024-01-01 00:00:00.0000000'
	AND [CRASH_DATE] > '2012-12-31 00:00:00.0000000'
	AND LATITUDE = 0 );

--Create a CTE with average weather data grouped by date (this will be used to fill in collisions with missing locations)
With AVG_WeatherData_By_Date AS (
SELECT [DATE] as [Temp Date],
Avg([Precipitation]) as [Temp Precipitation],
Avg([Snowfall]) as [Temp Snowfall],
Avg(Snow_Depth) as [Temp Snow Depth],
Avg(Avg_Temp) as [Temp Avg Temp],
Avg([Max_Temp]) as [Temp Max Temp],
Avg([Min_Temp]) as [Temp Min Temp],
Avg([High_Winds]) as [Temp High Winds],
Avg([Fog]) as [Temp Fog],
Avg([Heavy_Fog]) as [Temp Heavy Fog],
Avg([Thunder]) as [Temp Thunder],
Avg([Sleet]) as [Temp Sleet],
Avg([Hail]) as [Temp Hail],
Avg([Glaze/Rime]) as [Temp Glaze/Rime],
Avg([Smoke/Haze]) as [Temp Smoke/Haze]
FROM [Over 90 Accurate Weather Data]
WHERE
DATE < '2024-01-01 00:00:00.0000000'
AND DATE > '2012-12-31 00:00:00.0000000'
GROUP BY [DATE]
)

--Add the average weather data to the main collisions table on Collision IDs with missing weather data
UPDATE TargetTable
Set
TargetTable.Precipitation = WeatherData.[Temp Precipitation],
TargetTable.Snowfall = WeatherData.[Temp Snowfall],
TargetTable.[Snow Depth]= WeatherData.[Temp Snow Depth],
TargetTable.[Avg Temp]= WeatherData.[Temp Avg Temp],
TargetTable.[Max Temp] = WeatherData.[Temp Max Temp],
TargetTable.[Min Temp] = WeatherData.[Temp Min Temp],
TargetTable.HighWinds = WeatherData.[Temp High Winds],
TargetTable.Fog = WeatherData.[Temp Fog],
TargetTable.[Heavy Fog] = WeatherData.[Temp Heavy Fog],
TargetTable.Thunder = WeatherData.[Temp Thunder],
TargetTable.Sleet = WeatherData.[Temp Sleet],
TargetTable.Hail = WeatherData.[Temp Hail],
TargetTable.[Glaze/Rime] = WeatherData.[Temp Glaze/Rime],
TargetTable.[Smoke/Haze] = WeatherData.[Temp Smoke/Haze]
FROM [Motor_Vehicle_Collisions_-_Crashes] TargetTable
JOIN AVG_WeatherData_By_Date WeatherData
ON TargetTable.CRASH_DATE = WeatherData.[Temp Date]
WHERE
	TargetTable.CRASH_DATE < '2024-01-01 00:00:00.0000000'
	AND TargetTable.CRASH_DATE > '2012-12-31 00:00:00.0000000'
AND (
	TargetTable.LATITUDE > 40
	AND TargetTable.LATITUDE < 41
	AND TargetTable.LONGITUDE > -74.5
	AND TargetTable.LONGITUDE < -73
	AND TargetTable.Closest_Station IS NULL
)
OR TargetTable.LATITUDE IS NULL;