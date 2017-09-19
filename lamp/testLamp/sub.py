import sys, os, time, signal, json, psycopg2 
reload(sys)
sys.setdefaultencoding('utf-8')
import paho.mqtt.client as mqtt
from datetime import datetime

client = None
mqtt_looping = False

from config import CONFIG
BROKER_ADDRESS = CONFIG['BROKER_ADDRESS']
TOPIC = CONFIG['TOPIC']
from config import DBCONFIG
HOST = DBCONFIG['HOST']
DBNAME = DBCONFIG['DBNAME']
USER = DBCONFIG['USER']
PASSWORD = DBCONFIG['PASSWORD']

conn_string = "host="+HOST+" dbname="+DBNAME+" user="+USER+" password="+PASSWORD
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()
print ("Connected!\n")

def save_to_postgre(lamp_data):
    # get a connection, if a connect cannot be made an exception will be raised here
    cursor.execute("INSERT INTO lamps (id, created_at, counts) \
	VALUES(%s, %s, %s);", (lamp_data['id'], datetime.now(), lamp_data['counts']))
    conn.commit()
    print ("save Success\n")

def save_data(payload):
    lamp_data = json.loads(payload)
    save_to_postgre(lamp_data)

def on_connect(mq, userdata, rc, _):
    # subscribe when connected.
    mq.subscribe(TOPIC + '/#')

def on_message(mq, userdata, msg):
    print ("topic: %s" % msg.topic)
    print ("payload: %s" % msg.payload)
    save_data(msg.payload)

def mqtt_client_thread():
    global client, mqtt_looping
    client_id = "" # If broker asks client ID.
    client = mqtt.Client(client_id=client_id)

    # If broker asks user/password.
    user = "sirius207"
    password = "sirius207"
    client.username_pw_set(user, password)

    client.on_connect = on_connect
    client.on_message = on_message

    try:
        client.connect(BROKER_ADDRESS)
    except:
        print ("MQTT Broker is not online. Connect later.")

    mqtt_looping = True
    print ("Looping...")
    print (TOPIC)
    print (BROKER_ADDRESS)

    #mqtt_loop.loop_forever()
    cnt = 0
    while mqtt_looping:
        client.loop()

        cnt += 1
        if cnt > 20:
            try:
                client.reconnect() # to avoid 'Broken pipe' error.
            except:
                time.sleep(1)
            cnt = 0

    print ("quit mqtt thread")
    client.disconnect()

def stop_all(*args):
    global mqtt_looping
    mqtt_looping = False

if __name__ == '__main__':
    signal.signal(signal.SIGTERM, stop_all)
    signal.signal(signal.SIGQUIT, stop_all)
    signal.signal(signal.SIGINT, stop_all) # Ctrl-C

    mqtt_client_thread()

    print ("exit program")
    sys.exit(0)
