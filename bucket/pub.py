import datetime
import sys
import os
import time
f = open(sys.argv[1],'r')
Tlist = list()
while True:
    line = f.readline()
    if not line:break
    tmp = line.split("#")
    Tlist.append((tmp[0],tmp[1],tmp[2],datetime.datetime.strptime(tmp[2], "%Y-%m-%d").date(), tmp[3]))


Tlist.sort(key=lambda tup: tup[3]) 

for x in Tlist:
    print (x[0],x[1],x[2], x[4])

    os.system("python Mmqtt.py localhost STevent "+x[0]+"#"+x[1]+"#"+x[2]+"#"+"'" +x[4] + "'")
    time.sleep(1)

