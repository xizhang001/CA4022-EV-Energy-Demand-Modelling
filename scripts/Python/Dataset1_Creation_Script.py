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
    if end_point == end_point.floor('60min'):
        end_point = end_point - pd.Timedelta(hours=1)
    time_intervals = pd.period_range(start=start_point.floor('60min'), end=end_point.floor('60min'), freq='60T')
    time_intervals = time_intervals.to_timestamp()
    return time_intervals

# Function to create a list of minutes the charging span in each time period
def minute_intervals(split_time, start_point):
    end_point = start_point + pd.Timedelta(hours=int(split_time[0]), minutes=int(split_time[1]), seconds=int(split_time[2]))
    minute_intervals = []
    while start_point < end_point:
        ceiling = start_point.ceil('60min')
        # pd.Timestamp.ceil() and pd.Timestamp.floor() do not work on round hours as intended
        if start_point == ceiling:
            # Case when there is at least an hour left of charging
            if start_point + pd.Timedelta(hours=1) < end_point:
                minute_intervals.append(60)
                start_point = start_point + pd.Timedelta(minutes=60)
            # Case when there is less than an hour left of charging
            else:
                minutes_value = (end_point - start_point) / pd.Timedelta(minutes=1)
                minute_intervals.append(minutes_value)
                start_point = start_point + pd.Timedelta(minutes=minutes_value)

        else:            
            # Case when there is at least an hour left of charging
            if ceiling < end_point:
                minutes_value = (ceiling - start_point) / pd.Timedelta(minutes=1)
                minute_intervals.append(minutes_value)
                start_point = start_point + pd.Timedelta(minutes=minutes_value)
            # Case when there is less than an hour left of charging
            else:
                minutes_value = (end_point - start_point) / pd.Timedelta(minutes=1)
                minute_intervals.append(minutes_value)
                start_point = start_point + pd.Timedelta(minutes=minutes_value)

    return minute_intervals

# Function that iterates over input data rows, sums up respective split energy for periods and outputs a file
def dictionary_add_station(cluster_name):
    time_intervals = pd.period_range(start=start_point, end=end_point, freq='60T')
    time_intervals = time_intervals.to_timestamp()
    # Filter table for appropriate station cluster
    station_table = csv_file[csv_file.Station_Cluster_Name==cluster_name]
    station_table.reset_index(drop=True, inplace=True)

    # Dictionary for each time period and energy value
    time_intervals_dict = {}
    for i in time_intervals:
        time_intervals_dict[i] = 0

    i = 0
    while i < len(station_table):
        split_time = station_table.Charging_Duration[i].split(':')
        row_intervals = intervals(split_time, station_table.Date_Time[i])
        minute_split = minute_intervals(split_time, station_table.Date_Time[i])
        energy = station_table.Energy[i]
        j = 0
        # Update the energy value in dictionary for each time period
        while j < len(row_intervals):
            time_intervals_dict[row_intervals[j]] = time_intervals_dict[row_intervals[j]] + (minute_split[j] * energy / sum(minute_split))
            j = j + 1
        i = i + 1

    # Create output file
    data = []
    for k in time_intervals_dict.keys():
        data.append([time_intervals_dict[k], k])
    new_file_name = 'clean_data/Dataset1/' + cluster_name + '.txt'
    # Features are respectively: day of week, hour, last 3 values average, last value
    with open (new_file_name, 'w') as f:
        f.write("{} 1:{} 2:{} 3:{} 4:{}\n".format(data[0][0], data[0][1].day_of_week, data[0][1].hour, data[0][0], data[0][0]))
        i = 1
        while i < 3:
            f.write("{} 1:{} 2:{} 3:{} 4:{}\n".format(data[i][0], data[i][1].day_of_week, data[i][1].hour, data[i][0], data[i-1][0]))
            i = i + 1
        while i < len(data):
            f.write("{} 1:{} 2:{} 3:{} 4:{}\n".format(data[i][0], data[i][1].day_of_week, data[i][1].hour, np.mean([data[i-1][0], data[i-2][0], data[i-3][0]]), data[i-1][0]))
            i = i + 1

# Load file, make new directory and create output files
file_load('clean_data/Processed_Dataset/part-r-00000')
try:
    os.mkdir('./clean_data/Dataset1')
except FileExistsError:
    pass
cluster_names = ['BRYANT', 'HIGH', 'HAMILTON']
for k in cluster_names:
    dictionary_add_station(k)