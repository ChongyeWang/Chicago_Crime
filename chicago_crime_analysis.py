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
import tqdm
import argparse

def parallelize_dataframe(distance_range, time_range, target_file, data_file, which, num_partitions, func):
    if(which==1):
        split_file =  target_file
    else:
        split_file = data_file
    num_cores = multiprocessing.cpu_count()-1 #leave one free to not freeze machine
    #num_partitions = 10#num_cores #number of partitions to split dataframe
    df_split = np.array_split(split_file, num_partitions)
    print len(df_split)
    a = [(split, data_file, time_range, distance_range) for split in df_split]
    print len(a)
    pool = multiprocessing.Pool(num_cores)
    results = []
    for x in tqdm.tqdm(pool.imap_unordered(func, a), total=num_partitions):
        results.extend(x)
    pool.close()
    pool.join()

    return results


def closest(target_file_row, data_file_row, time_range, distance_range):
    try:
        distance = geopy.distance.vincenty((target_file_row.lat, target_file_row.lon), (data_file_row.lat, data_file_row.lon)).m
        delta = abs(data_file_row.time - target_file_row.time)
        return delta <= timedelta(seconds=time_range) and distance <= distance_range
    except:
        return False

def closest_distance(target_file_row, data_file_row, distance_range):
    try:
        distance = geopy.distance.vincenty((target_file_row.lat, target_file_row.lon), (data_file_row.lat, data_file_row.lon)).m
        return distance <= distance_range
    except:
        return False

def analyze(args):#target_file, data_file, time_range, distance_range):
    target_file, data_file, time_range, distance_range = args
    satisfied_target = []
    curr_df_date = data_file['time'][13000] #current earliest date of data file
    for target_index, target_row in target_file.iterrows():
        #print('Checking target row {}'.format(target_index))
        #logger.info('Checking target row {}'.format(target_index))
        curr_target_date = target_row.time
        lower_bound_date = curr_target_date - timedelta(seconds=time_range)
        upper_bound_date = curr_target_date + timedelta(seconds=time_range)
        find_first = False
        for row_index, data_row in data_file[data_file['time'] >= curr_df_date].iterrows():
            curr_row_date = data_row.time
            if curr_df_date < lower_bound_date: continue
            if curr_df_date > upper_bound_date: break
            if find_first == False:
                curr_df_date = curr_row_date
                find_first = True
            if closest_distance(target_row, data_row, distance_range):
                info_list = target_row.tolist()
                info_list.extend(data_row.tolist())
                satisfied_target.append(info_list)
    print "satisfied_target",satisfied_target
    return satisfied_target


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="train.py")
    parser.add_argument("-folder", default='/Users/apple/Desktop/Dataset/GPS/', help="Path to the *-train.pt file from preprocess.py")
    parser.add_argument("-filename", default='FA171521_ARCGIS_GPS_50_V1_2.csv', help="Filename for police file")
    parser.add_argument("-parallelize", default=1, type=int, help="run in parallel:1, no:0")
    parser.add_argument("-which", default=1, type=int, help="which file to split for parallel. 1:fitbit file, otherwise fitbit file")
    parser.add_argument("-npartitions", default=10, type=int, help="number of partitions for parallel, needs to be smaller than number of file rows")
    args = parser.parse_args()

    #Columns that are included in data_file
    fields = ['X', 'Y', 'USER_Event_Type', 'USER_Entry_Date___Time'] #changed the date field to the one that looks correct need to ask
    folder = args.folder
    filename = args.filename
    #Include the x, y, user_event_type, user_clrdate
    logger.info('Started reading data file')
    data_file = pandas.read_csv(os.path.join(folder, filename),skipinitialspace=True, usecols=fields)
    data_file = data_file.rename(columns={'USER_Entry_Date___Time': 'time', 'X':'lon', 'Y':'lat'}) #lets keep all times times
    #data_file = data_file[data_file.time.notnull()] # remove all NaN time values
    data_file['time'] = pandas.to_datetime(data_file.time)
    logger.info('Finished reading data file')

    #sort data file
    data_file = data_file.sort_values(by=['time'], ascending=[True])

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
            target_file['time'] = pandas.to_datetime(target_file.time)
            #convert to chicago time
            target_file['time'] -= timedelta(minutes=300)

            #assert target_file.time.dt.normalize().nunique() == len(list_of_csv)
            logger.info('Finished reading target {}'.format(target_name))
            #start analysis for one person
            logger.info('Started analysis for target {}'.format(target_name))

            target_file = target_file.sort_values(by=['time'], ascending=[True])

            if(args.parallelize):
                list = parallelize_dataframe(100, 60, target_file, data_file, args.which, args.npartitions, analyze)
            else:
                arguments = target_file, data_file, 60, 100
                list = analyze(arguments)
            logger.info('Finished analysis for target {}'.format(target_name))

            df =  pandas.DataFrame(list)
            df.to_csv(str(target_name) + '.csv', sep=',', encoding='utf-8')

            exit()
