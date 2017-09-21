import paho.mqtt.client as mqtt
import paho.mqtt.publish as publish
from datetime import datetime
import psycopg2
import json
############
from config import CONFIG
BROKER_ADDRESS = CONFIG['BROKER_ADDRESS']
TOPIC = CONFIG['TOPIC_ALIVE']
############
from config import DBCONFIG
HOST = DBCONFIG['HOST']
DBNAME = DBCONFIG['DBNAME']
USER = DBCONFIG['USER']
PASSWORD = DBCONFIG['PASSWORD']
########################################
# Receive Lamps Alive Message
def on_connect(client, userdata, rc):
    client.subscribe(TOPIC)

def on_message(client, userdata, msg):
    ''' update lamps state '''
    LampData = json.loads(msg.payload)
    if 'id' in LampData:
        print(str(msg.payload))
        print("message qos=", msg.qos)
        lampId = str(LampData['id'])
        cursor.execute("INSERT INTO lamps_alive (id, created_at) \
            VALUES(%s, %s);", (LampData['id'], datetime.now()))
        conn.commit()

########################################
# Connect to DB
conn_string = "host=" + HOST + " dbname=" + DBNAME + \
    " user=" + USER + " password=" + PASSWORD
print ("Connecting to database\n	->%s" % (conn_string))
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()
print ("Connected!\n")

########################################
# Connect to MQTT Server
client = mqtt.Client("P1") #create new instance
client.on_connect = on_connect
client.on_message = on_message
client.connect(BROKER_ADDRESS, 1883, 60)
client.loop_forever()