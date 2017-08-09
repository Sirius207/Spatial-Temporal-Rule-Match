import paho.mqtt.client as paho
import time
import sys

def on_publish(client, userdata, mid):
    print("mid: "+str(mid))

client = paho.Client()

client.on_publish = on_publish
client.connect(sys.argv[1], 1883)

#auth = {'username':'sirius207', 'password': 'sirius207'}

(rc, mid) = client.publish(sys.argv[2], sys.argv[3], qos=0)
