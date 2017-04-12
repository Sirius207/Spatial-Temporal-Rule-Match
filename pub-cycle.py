import sys, os, time
reload(sys)
sys.setdefaultencoding('utf-8')

import paho.mqtt.client as mqtt

# If broker asks client ID.
client_id = ""

client = mqtt.Client(client_id=client_id)

# If broker asks user/password.
user = "sirius207"
password = "sirius207"
client.username_pw_set(user, password)

client.connect("localhost")

topic = "test"
payload = "Test mqtt"

for i in xrange(10):
    client.publish(topic, "%s - %d" % (payload, i))
    time.sleep(0.01)
