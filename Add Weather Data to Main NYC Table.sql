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