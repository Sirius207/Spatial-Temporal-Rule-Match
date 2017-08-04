import sys
import os
import time
import json
import datetime, sched

# from bucket717 import bucketRecord
import urllib2
from config import CONFIG
POSITION_URL = CONFIG['POSITION_URL']
DAYS = CONFIG['TIMELINE']
UPLIMIT = CONFIG['UPLIMIT']
INTERVAL = 86400 # every second  

#
# Scheduler
#
class PeriodicScheduler(object):                                        
    def __init__(self):
        self.scheduler = sched.scheduler(time.time, time.sleep)
    def setup(self, interval, action, actionargs=()):
        action(*actionargs)
        self.scheduler.enter(interval, 1, self.setup, (interval, action, actionargs))
    def run(self):
        self.scheduler.run()
#
# Get Bucket Position
#
def parse_bucket_position():
    content = urllib2.urlopen(POSITION_URL).read()
    BucketsPosition = json.loads(content)
    return BucketsPosition
#
# Get Lsat Week Data
#
def parse_bucket_data():
    ''' Get API Bucket Data'''
    StartDate = (datetime.datetime.now() - datetime.timedelta(days=DAYS)).strftime("%Y-%m-%d")
    URL = 'http://report.denguefever.tw/bucket-record/?start=' \
        + StartDate + '&county=%E5%8F%B0%E5%8D%97'
    content = urllib2.urlopen(URL).read()
    bucket = json.loads(content)

    MORE = {
        'type': 'json'
    }

    BucketsPosition = parse_bucket_position()

    bucketFile = open('bucket.dat', 'w')
    for item in bucket['bucket-record']:
        if item['egg_count'] >= UPLIMIT:
            # print item['egg_count']
            MORE['bucket_id'] = item['bucket_id']
            dataLine = str(BucketsPosition[item['bucket_id']]['lng']) + \
            '#' + str(BucketsPosition[item['bucket_id']]['lat']) + \
            '#' + item['investigate_date'] + \
            '#' + json.dumps(MORE) + '\n'
            bucketFile.writelines(dataLine)
    bucketFile.close()

#
# Pub MQTT Message
#
def send():
    ''' send MQTT Bucket Message '''
    bucketFile = open('bucket.dat', 'r')
    point_list = list()
    while True:
        line = bucketFile.readline()
        if not line: break
        tmp = line.split("#")
        point_list.append((tmp[0], tmp[1], tmp[2], datetime.datetime.strptime(tmp[2], "%Y-%m-%d").date(), tmp[3]))

    point_list.sort(key=lambda tup: tup[3])

    for value in point_list:
        print (value[0], value[1], value[2], value[4])
        os.system("python Mmqtt.py localhost STevent "+value[0]+"#"+value[1]+"#"+value[2]+"#"+"'" +value[4] + "'")
        time.sleep(1)
    bucketFile.close()

#
# 
#
def periodic_event():
    ''' periodic process ''' 
    parse_bucket_data()
    send()
    print datetime.datetime.now()

periodic_scheduler = PeriodicScheduler()
periodic_scheduler.setup(INTERVAL, periodic_event) # it executes the event just once  
periodic_scheduler.run() # it starts the scheduler
