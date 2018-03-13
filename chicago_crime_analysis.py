"""
Find the matching target from the target
folders to the police data file.
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
import time

#ps -u | grep chicago_crime_analysis.py | awk '{print $2}' | xargs kill -KILL #kills all parallel processes

def parallelize_dataframe(distance_range, time_range, target_file, data_file, which, num_partitions, func):
    if(which==1):
        split_file =  target_file
    else:
        split_file = data_file
    num_cores = multiprocessing.cpu_count()-1 #leave one free to not freeze machine
    df_split = np.array_split(split_file, num_partitions)
    a = [(split, data_file, time_range, distance_range) for split in df_split]
    pool = multiprocessing.Pool(num_cores)
    results = []
    for x in tqdm.tqdm(pool.imap_unordered(func, a), total=num_partitions):
        results.extend(x)
    pool.close()
    pool.join()
    return results


def closest_distance(target_file_row, data_file_row, distance_range):
    """
    Check if the distance is within the range.
    """
    try:
        distance = geopy.distance.vincenty((target_file_row.lat, target_file_row.lon), (data_file_row.Y, data_file_row.X)).m
        #return distance <= distance_range
        if distance > distance_range:
            return -1
        return distance
    except:
        return -1

def analyze(args):#target_file, data_file, time_range, distance_range):
    """
    Check if target data matches the plice data.
    """
    target_file, data_file, time_range, distance_range = args
    satisfied_target = []
    nCrime = 0
    row_start = 0

    for target_index, target_row in target_file.iterrows():

        curr_target_date = target_row.time
        lower_bound_date = curr_target_date - timedelta(seconds=time_range)
        upper_bound_date = curr_target_date + timedelta(seconds=time_range)
        nCount = 0

        row_offset = 0

        for row_index in range(row_start, len(data_file)): #data_row in data_file[data_file['time'] >= curr_df_date].iterrows():
            data_row = data_file.iloc[row_index]
            curr_row_date = data_row.USER_Entry_Date___Time

            row_offset = row_offset + 1
            nCount = nCount + 1

            if curr_row_date < lower_bound_date: continue
            if curr_row_date > upper_bound_date: break

            distance = closest_distance(target_row, data_row, distance_range)
            if (distance != -1):
                info_list = target_row.tolist()
                info_list.extend(data_row.tolist())
                info_list.extend([distance])#add distance
                satisfied_target.append(info_list)
                nCrime = nCrime + 1

        #Subtract 1 to ensure next loop doesn't skip a row
        row_start = row_start + row_offset - 1

    print("Crime exposures: ", nCrime)
    print ("satisfied_target",satisfied_target)
    return satisfied_target


def run_one_folder(folder, target_name, fields, args):
    """
    Run for one target.
    """
    sub_dir = os.path.join(folder, target_name)
    list_of_csv = glob.glob(sub_dir + "/*.csv")
    print("Mobile files detected: ", len(list_of_csv))
    target_info = []
    logger.info('Started reading target {}'.format(target_name))
    #concat all files in one
    for csv_file in list_of_csv:
        target_csv_data_file = pandas.read_csv(csv_file, skipinitialspace=True, usecols=fields)
        target_info.append(target_csv_data_file)

        #REMOVE THIS AFTER DEBUGGING
        for index, row in target_csv_data_file.iterrows():
            print csv_file, index, pandas.to_datetime(row.time)

    target_file = pandas.concat(target_info)
    target_file = target_file[target_file.time.notnull()] # remove all NaN time values
    target_file['time'] = pandas.to_datetime(target_file.time)

    #convert to chicago time
    target_file['time'] -= timedelta(minutes=300)

    logger.info('Finished reading target {}'.format(target_name))
    print("Mobile GPS samples: ", len(target_file.index))
    logger.info('Started analysis for target {}'.format(target_name))

    target_file = target_file.sort_values(by=['time'], ascending=[True])

    if(args.parallelize):
        print("Run analysis in parralel")
        list = parallelize_dataframe(args.distance, args.time, target_file, data_file, args.which, args.npartitions, analyze)
    else:
        print("Run analysis sequentially")
        arguments = target_file, data_file, args.time, args.distance
        list = analyze(arguments)
    logger.info('Finished analysis for target {}'.format(target_name))

    return list

if __name__ == "__main__":

    analysis_type = '911'

    #crimedata = 'Incident Reports.csv'
    parser = argparse.ArgumentParser(description="train.py")
    parser.add_argument("-folder", default='GPS/', help="Path to the *-train.pt file from preprocess.py")
    parser.add_argument("-filename", default='FA171521_ARCGIS_GPS_50_V1_2.csv', help="Filename for police file")
    parser.add_argument("-analysis_type", default='911', help="either 911 or IR?")
    parser.add_argument("-tgt_folder", default=None, help="Target folder")
    parser.add_argument("-parallelize", default=0, type=int, help="run in parallel:1, no:0")
    parser.add_argument("-which", default=1, type=int, help="which file to split for parallel. 1:fitbit file, otherwise fitbit file")
    parser.add_argument("-npartitions", default=10, type=int, help="number of partitions for parallel, needs to be smaller than number of file rows")
    parser.add_argument("-distance", default=100, type=int, help="write this")
    parser.add_argument("-time", default=60, type=int, help="write this")
    args = parser.parse_args()

    starttime = time.time()

    police_fields = ['ObjectID', 'Loc_name', 'Status', 'Score', 'Match_type', 'Match_addr', \
    'Addr_type', 'AddNum', 'Side', 'StPreDir', 'StPreType', 'StName', 'StType', 'StDir', \
    'StAddr', 'City', 'Subregion', 'Region', 'Postal', 'Country', 'LangCode', 'Distance', \
    'X', 'Y', 'DisplayX', 'DisplayY', 'Xmin', 'Xmax', 'Ymin', 'Ymax', 'AddNumFrom', 'AddNumTo', \
    'RegionAbbr', 'Rank', 'IN_SingleLine', 'USER_Eventnumber', 'USER_Entry_Date___Time', \
    'USER_Clrdate', 'USER_Event_Type', 'USER_Address_by_Block', 'USER_District_of_Incident', \
                     'USER_Beat_of_Incident', 'USER_Findisposition'] #changed the date field to the one that looks correct need to ask

    folder = args.folder
    filename = args.filename

    target_fields = ['time', 'lat', 'lon', 'elevation', 'accuracy', 'bearing', 'speed', \
             'satellites', 'provider', 'hdop', 'vdop', 'pdop', 'geoidheight', 'ageofdgpsdata', \
             'dgpsid', 'activity', 'battery', 'annotation']

    #Include the x, y, user_event_type, user_clrdate
    logger.info('Started reading data file')
    data_file = pandas.read_csv(os.path.join(folder, filename), skipinitialspace=True, usecols=police_fields)
    #data_file = data_file[data_file.time.notnull()] # remove all NaN time values
    data_file['USER_Entry_Date___Time'] = pandas.to_datetime(data_file.USER_Entry_Date___Time)
    logger.info('Finished reading data file')
    print("Loading time: ", round(time.time()-starttime,2))

    #sort data file
    data_file = data_file.sort_values(by=['USER_Entry_Date___Time'], ascending=[True])

    print ("Crimes loaded:", len(data_file.index))

    if(args.tgt_folder):#run one folder
        #check subject data exists
        if os.path.isdir(os.path.join(folder, args.tgt_folder)):
            list = run_one_folder(folder, args.tgt_folder, target_fields, args)
            df =  pandas.DataFrame(list)
            #Put the headers here before making the new column
            column_names = target_fields + police_fields + ['Distance']
            df.columns = column_names
            df.to_csv(args.tgt_folder + '.csv', sep=',', encoding='utf-8')

    else:#run all folders
        list_of_df = []#Add each dataframe to the list
        #Each target include time, x, y
        for target_name in os.listdir(folder):
            if os.path.isdir(os.path.join(folder, target_name)):
                list = run_one_folder(folder, target_name, target_fields, args)
                df =  pandas.DataFrame(list)
                #Put the headers here before making the new column
                column_names = target_fields + police_fields + ['Distance']
                df.columns = column_names
                df['Target'] = target_name
                list_of_df.append(df)

        result_df = pandas.concat(list_of_df)
        result_df = result_df.drop_duplicates(subset=police_fields)#remove duplicates
        result_df.to_csv('result_' + analysis_type + '.csv', sep=',', encoding='utf-8')
