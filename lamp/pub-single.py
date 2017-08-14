import paho.mqtt.publish as publish
from datetime import datetime
from json import dumps
from config import CONFIG
BROKER_ADDRESS = CONFIG['BROKER_ADDRESS']
TOPIC = CONFIG['TOPIC']

def json_serial(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError ("Type not serializable")

TEMP = {
	   'id':"1",
	   'created_at': datetime.now(),
	   'more':{"type":"json"},
}

# publish a message then disconnect.
payload =  dumps(TEMP, default=json_serial)

# If broker asks user/password.
#auth = {'username': "sirius207", 'password': "sirius207"}

# If broker asks client ID.
#client_id = "test"

publish.single(TOPIC, payload, qos=1, hostname=BROKER_ADDRESS)
