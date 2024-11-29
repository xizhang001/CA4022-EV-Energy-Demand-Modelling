# Import libraries
import pandas as pd
import numpy as np
import os

# Function to read input data
def file_load(file_name):
    global csv_file
    csv_file = pd.read_csv(file_name, names=['Station_Name', 'Date_Time', 'Charging_Duration', 'Energy', 'Station_Cluster_Name'])
    csv_file.Date_Time = pd.to_datetime(csv_file.Date_Time, dayfirst=True)

    # Set common start and end date range
    global start_point
    start_point = csv_file['Date_Time'].iloc[0].floor('60min')
    global end_point
    end_point = csv_file['Date_Time'].iloc[-1].floor('60min')

# Function to create a list of time periods the charging overlaped with
def intervals(split_time, start_point):
    end_point = start_point + pd.Timedelta(hours=int(split_time[0]), minutes=int(split_time[1]), seconds=int(split_time[2]))
    time_intervals = pd.period_range(start=start_point.floor('1H'), end=end_point.floor('1H'), freq='1H')
    time_intervals = time_intervals.to_timestamp()
    return time_intervals

# Function that iterates over input data rows, calculates the number of unoccupied stations during each period and outputs a file
def dictionary_add_station(cluster_name):
    time_intervals = pd.period_range(start=start_point, end=end_point, freq='1H')
    time_intervals = time_intervals.to_timestamp()
    # Filter table for appropriate station cluster
    station_table = csv_file[csv_file.Station_Cluster_Name==cluster_name]
    station_table.reset_index(drop=True, inplace=True)
    # Calculate max. no of available stations
    unique_station = np.unique(station_table.Station_Name)
    max_stations = len(unique_station)

    # Dictionary for each time period and unoccupied stations count
    time_intervals_dict = {}
    for i in time_intervals:
        time_intervals_dict[i] = []

    i = 0
    while i < len(station_table):
        split_time = station_table.Charging_Duration[i].split(':')
        row_intervals = intervals(split_time, station_table.Date_Time[i])
        # Append the occupied station's name in dictionary for each time period
        for k in row_intervals:
            time_intervals_dict[k].append(station_table.Station_Name[i])
        i = i + 1

    # Create output file
    data = []
    for k in time_intervals_dict.keys():
        data.append([k, max_stations - len(np.unique(time_intervals_dict[k]))])
    df_new = pd.DataFrame(data,columns=['Date_Time','Free_Station_Count'])
    new_file_name = 'clean_data/Dataset3/' + cluster_name + '.csv'
    df_new.to_csv(new_file_name, index=False)

# Load file, make new directory and create output files
file_load('clean_data/Processed_Dataset/part-r-00000')
try:
    os.mkdir('./clean_data/Dataset3')
except FileExistsError:
    pass
cluster_names = ['BRYANT', 'HIGH', 'HAMILTON']
for k in cluster_names:
    dictionary_add_station(k)

