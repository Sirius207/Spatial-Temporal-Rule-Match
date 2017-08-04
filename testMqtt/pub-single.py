import paho.mqtt.publish as publish
from datetime import datetime
from json import dumps


temp_data = {
	'bucket_ID':"1",
	'latitude':"22.9920817",
	'longitude':"120.2224114",
	'created_at': datetime.now(),
	'more':{"type":"json"},
}


def json_serial(obj):

    if isinstance(obj, datetime):
        return obj.isoformat()
 
    raise TypeError ("Type not serializable")





# publish a message then disconnect.
host = "localhost"
topic = "test"
payload =  dumps(temp_data, default=json_serial)

# If broker asks user/password.
auth = {'username': "sirius207", 'password': "sirius207"}

# If broker asks client ID.
client_id = "test"


publish.single(topic, payload, qos=1, hostname=host,auth=auth, client_id=client_id)
