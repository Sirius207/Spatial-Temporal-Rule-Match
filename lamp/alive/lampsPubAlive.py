import paho.mqtt.publish as publish
from datetime import datetime
import time
import json
import schedule
from config import CONFIG
BROKER_ADDRESS = CONFIG['BROKER_ADDRESS']
TOPIC = CONFIG['TOPIC_CHECK']

########################################
# Publish Check Alive Message
def json_serial(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError("Type not serializable")

def send_alive_check():
    # TIME = {
    #     'test': True,
    #     # 'id': 'www'
    # }
    # # publish a message then disconnect.
    # payload = json.dumps(TIME, default=json_serial)
    publish.single(TOPIC, 'TESTLAMP', qos=1, hostname=BROKER_ADDRESS)
    # print(payload)
    print(str(datetime.now()))

########################################
send_alive_check()
# schedule.every().day.at("23:55").do(send_alive_check)
# schedule.every(1).minutes.do(send_alive_check)
# while True:
#     schedule.run_pending()
#     time.sleep(1)
