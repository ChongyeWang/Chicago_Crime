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
from datetime import timedelta
import logging
logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)
logger = logging.getLogger(__name__)
import numpy as np
import multiprocessing
from progressbar import ProgressBar, SimpleProgress
import tqdm

def parallelize_dataframe(distance_range, time_range, target_file, data_file, func):
    num_cores = multiprocessing.cpu_count()-1 #leave one free to not freeze machine
    num_partitions = num_cores #number of partitions to split dataframe
    df_split = np.array_split(target_file, num_partitions)
    print len(df_split)
    a = [(split, data_file, time_range, distance_range) for split in df_split]
    print len(a)
    pool = multiprocessing.Pool(num_cores)
    results = []
    for x in tqdm.tqdm(pool.imap_unordered(func, a), total=num_partitions):
        results.append(x)
    #df = pandas.concat(pool.map(func, a))
    pool.close()
    pool.join()
    df = pandas.concat(results)
    return df


def closest(target_file_row, data_file_row, time_range, distance_range):
    try:
        distance = geopy.distance.vincenty((target_file_row.lat, target_file_row.lon), (data_file_row.lat, data_file_row.lon)).m
        delta = abs(data_file_row.time - target_file_row.time)
        return delta <= timedelta(seconds=time_range) and distance <= distance_range
    except:
        return False

def analyze(args):
    target_file, data_file, time_range, distance_range = args
    satisfied_target = []
    for target_index, target_row in target_file.iterrows():
        #print('Checking target row {}'.format(target_index))
        #logger.info('Checking target row {}'.format(target_index))
        for data_index, data_row in data_file.iterrows():
            """if(data_index%10000==0):
                #logger.info('Checking data row {}'.format(data_index))
                print('Checking data row {}'.format(data_index))"""
        if(closest(target_row, data_row, time_range, distance_range)):
                #time_t, y_t, x_t, x_d, y_d, time_t, type_d
                info_list = target_row.tolist()
                info_list.extend(data_row.tolist())
                satisfied_target.append(info_list)
        #logger.info('Finished target row {}'.format(target_index))
    return satisfied_target


if __name__ == "__main__":
    #Columns that are included in data_file
    fields = ['X', 'Y', 'USER_Event_Type', 'USER_Entry_Date___Time'] #changed the date field to the one that looks correct need to ask
    folder = '/Users/ismini/Documents/Dataset/GPS/'
    filename = 'FA171521_ARCGIS_GPS_50.csv'
    #Include the x, y, user_event_type, user_clrdate
    logger.info('Started reading data file')
    data_file = pandas.read_csv(os.path.join(folder, filename),skipinitialspace=True, usecols=fields)
    data_file = data_file.rename(columns={'USER_Entry_Date___Time': 'time', 'X':'lon', 'Y':'lat'}) #lets keep all times times
    data_file = data_file[data_file.time.notnull()] # remove all NaN time values
    data_file['time'] = pandas.to_datetime(data_file.time)
    logger.info('Finished reading data file')

    #Each target include time, x, y
    for target_name in os.listdir(folder):
        if os.path.isdir(os.path.join(folder, target_name)):
            sub_dir = os.path.join(folder, target_name)
            list_of_csv = glob.glob(sub_dir + "/*.csv")
            target_info = []
            logger.info('Started reading target {}'.format(target_name))
            #concat all files in one
            for csv_file in list_of_csv:
                field = ['time', 'lat', 'lon']
                target_csv_data_file = pandas.read_csv(csv_file, skipinitialspace=True, usecols=field)
                target_info.append(target_csv_data_file)
            target_file = pandas.concat(target_info)
            target_file = target_file[target_file.time.notnull()] # remove all NaN time values
            target_file['time'] =pandas.to_datetime(target_file.time)
            assert target_file.time.dt.normalize().nunique() == len(list_of_csv)
            logger.info('Finished reading target {}'.format(target_name))
            #start analysis for one person
            logger.info('Started analysis for target {}'.format(target_name))
            #list = analyze(100, 60, target_file, data_file)
            list = parallelize_dataframe(100, 60, target_file, data_file, analyze)
            logger.info('Finished analysis for target {}'.format(target_name))
            exit()
