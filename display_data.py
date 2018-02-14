import datetime
from datetime import datetime
import csv
import pandas

#Columns that are included in data_file
fields = ['time_t', 'lat_t', 'lon_t', 'lon_d', 'lat_d', 'USER_Event_Type', 'time_d']

#Include the x, y, user_event_type, user_clrdate
data_file = pandas.read_csv('/Users/apple/Desktop/Dataset/GPS/abc.csv', skipinitialspace=True, usecols=fields)

#Dictionary that
date_event_type = {}

data_file['time_t'] = pandas.to_datetime(data_file.time_t)
data_file['time_d'] = pandas.to_datetime(data_file.time_d)

for row_index in range(len(data_file['time_t'])):
    event_date = data_file['time_t'][row_index].date()
    if event_date not in date_event_type:
        date_event_type[event_date] = {}
    event_type = data_file['USER_Event_Type'][row_index]
    if event_type not in date_event_type[event_date]:
        date_event_type[event_date][event_type] = 0
    date_event_type[event_date][event_type] += 1


for date in date_event_type:
    print(date)
    for event_type in date_event_type[date]:
        print(event_type + ' : ' ),
        print(date_event_type[date][event_type])
