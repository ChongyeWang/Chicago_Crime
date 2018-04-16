import json
from pprint import pprint
from dateutil import parser

data = json.load(open('viz_data/s2_MT_2017_10_18.json'))
json_entries = []
for i, item in enumerate(data['activities-steps-intraday']['dataset']):
    date = data['activities-steps'][0]['dateTime']+"T"+item['time']
    if(item['value']>0):
        json_entries.append({"date": str(date), "details": {"value": item['value'], "object": "vmName"}})
        print(item['value'])
with open("mtdata.js", "w") as f:
    f.write("var data = [{\"name\":\"Person1\", \"data\":")
    f.write(json.dumps(json_entries, ensure_ascii=False))
    f.write("}]")


data = json.load(open('viz_data/s2_HR_2017_10_18.json'))
json_entries = []
for i, item in enumerate(data['activities-heart-intraday']['dataset']):
    date = data['activities-heart'][0]['dateTime']+"T"+item['time']
    if(item['value']>0):
        json_entries.append({"date": str(date), "details": {"value": item['value'], "object": "vmName"}})
        print(item['value'])
with open("hrdata.js", "w") as f:
    f.write("var data = [{\"name\":\"Person1\", \"data\":")
    f.write(json.dumps(json_entries, ensure_ascii=False))
    f.write("}]")