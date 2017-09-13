import paho.mqtt.client as mqtt
import time
import datetime
import math
import psycopg2
import json
from math import radians, cos, sin, asin, sqrt

from config import CONFIG
BROKER_ADDRESS = CONFIG['BROKER_ADDRESS']
TOPIC = CONFIG['TOPIC']

from location import LOCATION

from config import DBCONFIG
HOST = DBCONFIG['HOST']
DBNAME = DBCONFIG['DBNAME']
USER = DBCONFIG['USER']
PASSWORD = DBCONFIG['PASSWORD']

def on_publish(client, userdata, mid):
	print("mid: "+str(mid))

def on_connect(client, userdata, rc):
    client.subscribe("test")
    client.subscribe(TOPIC)

def on_message(client, userdata, msg):

    if msg.topic == 'ncku/netdb/test':
        print(str(msg.payload))

    elif msg.topic == TOPIC:
        print(str(msg.payload))

        LampData = json.loads(msg.payload)
        lampId = str(LampData['id'])
        Eid = str(time.time()).replace(".", "") + '-' + lampId
        Lon = float(LOCATION[lampId]['Lon'])
        Lat = float(LOCATION[lampId]['Lat'])
        Time = str(datetime.date.today())

        # Save to DB
        cursor.execute("INSERT INTO lamps (id, created_at, counts) \
		VALUES(%s, %s, %s);", (LampData['id'], datetime.datetime.now(), LampData['cnt']))
        conn.commit()

        print(Eid)


conn_string = "host=" + HOST + " dbname=" + DBNAME + \
    " user=" + USER + " password=" + PASSWORD
print ("Connecting to database\n	->%s" % (conn_string))
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()
print ("Connected!\n")

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.connect(BROKER_ADDRESS, 1883, 60)
client.loop_forever()
