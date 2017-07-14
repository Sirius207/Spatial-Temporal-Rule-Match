import sys
import time
import datetime
import urllib
import urllib2
import json

def InsertData(a,b,c,d):
    url = 'http://localhost:8080/api/1.0/'
    voltparams = json.dumps([a,b,c,d])
    httpparams = urllib.urlencode({
            'Procedure': 'input.insert',
            'Parameters' : voltparams
    })
    data = urllib2.urlopen(url, httpparams).read()
    result = json.loads(data)

f = open(sys.argv[1],'r')
Tlist = list()
while True:
    line = f.readline()
    if not line:break
    tmp = line.split(",")
    if "105" not in tmp[0] or len(tmp[0].split("/"))!=3:continue
    tmpp = tmp[0].split("/")
    Time = str(int(tmpp[0])+1911)+"/"+tmpp[1]+"/"+tmpp[2]
    TS = time.mktime(datetime.datetime.strptime(Time, "%Y/%m/%d").timetuple())

    Tlist.append( (TS,tmp[5],tmp[6]) )

Slist = sorted(Tlist, key=lambda tup: tup[0])
cnt = 0
for x in Slist:
    InsertData(cnt,x[1],x[2],int(x[0]*1000))
    #print str(cnt)+"\t"+str(x[0])+"\t"+str(x[1])+"\t"+str(x[2])
    cnt += 1
