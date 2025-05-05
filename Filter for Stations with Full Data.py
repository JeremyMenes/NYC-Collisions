import pandas as pd

with open(r"C:\Users\menes\Documents\NYC Traffic Collisions Project\Weather Data\Weather Data.csv", encoding='utf-8-sig') as Weather_table:
    Stations = pd.read_csv(Weather_table)

# Set the completeness threshold as a %
completeness_threshold = 0.9

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
print(result_df.sort_values(by='Completeness', ascending=False))