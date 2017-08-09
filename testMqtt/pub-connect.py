
import paho.mqtt.client as mqtt

mqttc = mqtt.Client()
mqttc.connect("140.116.247.113")
mqttc.loop_start()

mqttc.publish(topic="test", payload=12)
print ('ok')
