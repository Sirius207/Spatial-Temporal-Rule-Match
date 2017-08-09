import datetime
import sys
import os
import time
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
    os.system("python Mmqtt.py iot.eclipse.org ncku/net/STevent "+value[0]+"#"+value[1]+"#"+value[2]+"#"+"'" +value[4] + "'")
    time.sleep(1)
bucketFile.close()

