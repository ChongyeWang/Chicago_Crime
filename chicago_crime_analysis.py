"""
This file transfers the csv data file of the
Chicago crime event and information of research
targets.
"""
import pandas
import os
import glob
import math
import geopy.distance
from os import listdir
from os.path import isfile, join
from os import walk
from datetime import datetime


def analyze(distance_range, time_range, target_info, data_file):
    satisfied_target = []
    for target in target_info:
        for csv_file in target_info[target]:
            for user_index in range(len(csv_file)):
                x_t = csv_file['lon'][user_index] #x
                y_t = csv_file['lat'][user_index] #y
                time_t = csv_file['time'][user_index] #time

                if len(time_t) <= 20: continue #Invalid data

                date_str_t = csv_file['time'][user_index][0:10]
                time_str_t = csv_file['time'][user_index][11:19]
                date_time_str_t = date_str_t + ' ' + time_str_t
                date_time_t = datetime.strptime(date_time_str_t, "%Y-%m-%d %H:%M:%S")
                target_location = (x_t, y_t)
                for data_index in range(len(data_file)):
                    x_d = data_file['X'][data_index] #x
                    y_d = data_file['Y'][data_index] #y
                    time_d = data_file['USER_Clrdate'][data_index] #time
                    type_d = data_file['USER_Event_Type'][data_index] #type

                    if isinstance(time_d, float) == True: continue #Invalid data

                    date_time_d = datetime.strptime(time_d, "%m/%d/%Y %H:%M:%S")
                    data_location = (x_d, y_d)

                    time_diff = abs(date_time_t - date_time_d).total_seconds()
                    loca_diff = geopy.distance.vincenty(target_location, data_location).m

                    if loca_diff < distance_range and time_diff < time_range:
                        info_list = [x_t, y_t, time_t, x_d, y_d, time_d, type_d]
                        satisfied_target.append(info_list)

    return satisfied_target

if __name__ == "__main__":
    #Columns that are included in data_file
    fields = ['X', 'Y', 'USER_Event_Type', 'USER_Clrdate']

    #Include the x, y, user_event_type, user_clrdate
    data_file = pandas.read_csv('/Users/apple/Desktop/Dataset/GPS/FA171521_ARCGIS_GPS_50.csv', skipinitialspace=True, usecols=fields)

    #Each target include time, x, y
    target_info = {}
    for target_name in os.listdir("/Users/apple/Desktop/Dataset/GPS"):
        if os.path.isdir(os.path.join("/Users/apple/Desktop/Dataset/GPS", target_name)):
            sub_dir = "/Users/apple/Desktop/Dataset/GPS/" + target_name #each target
            list_of_csv = glob.glob(sub_dir + "/*.csv")
            target_info[target_name] = []
            for csv_file in list_of_csv:
                field = ['time', 'lat', 'lon']
                target_csv_data_file = pandas.read_csv(csv_file, skipinitialspace=True, usecols=field)
                target_info[target_name].append(target_csv_data_file)

    list = analyze(100, 60, target_info, data_file)
    print(list)
