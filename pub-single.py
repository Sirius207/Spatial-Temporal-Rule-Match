import paho.mqtt.publish as publish

# publish a message then disconnect.
host = "localhost"
topic = "test"
payload = "hello mqtt"

# If broker asks user/password.
auth = {'username': "sirius207", 'password': "sirius207"}

# If broker asks client ID.
client_id = "test"


publish.single(topic, payload, qos=1, hostname=host,auth=auth, client_id=client_id)
