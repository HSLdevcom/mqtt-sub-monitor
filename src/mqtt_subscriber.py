import paho.mqtt.client as mqtt
from datetime import datetime
from utils.logger import Logger

class MqttSubscriber:

    def __init__(self, log: Logger, host, topic):
        self.log = log
        self.host = host
        self.topic = topic
        self.client = mqtt.Client(client_id='hsl-mqtt-monitoring')
        self.msg_count = 0

    def start_sub(self):    
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.connect(self.host, 1883, 10)
        self.client.subscribe(self.topic, qos=1)
        self.client.loop_start()

    def on_connect(self, client, userdata, flags, rc):
        self.log.info('connecting to broker: '+ self.host)
        self.log.info('connected with result code: '+ str(rc))
        self.client.subscribe(self.topic)
        self.log.info('subscribed to topic: '+ self.topic)

    def on_message(self, client, userdata, msg):
        self.msg_count += 1

    def reset_msg_count(self):
        self.msg_count = 0
