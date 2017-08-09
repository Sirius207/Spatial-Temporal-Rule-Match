import paho.mqtt.publish as publish
TOPIC = 'test'
PAYLOAD = 'hello'
HOSTNAME = 'localhost'

publish.single(TOPIC, PAYLOAD, qos=1, hostname=HOSTNAME, port=1883)