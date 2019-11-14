import paho.mqtt.client as mqtt
from datetime import datetime
from logger import log

class MqttSubscriber:

    def __init__(self, host, topic):
        self.host = host
        self.topic = topic
        self.client = mqtt.Client(client_id='hfp-monitoring')
        self.msg_count = 0

    def start_sub(self):    
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.connect(self.host, 1883, 10)
        self.client.subscribe(self.topic, qos=1)
        self.client.loop_start()

    def on_connect(self, client, userdata, flags, rc):
        log('connecting to broker: '+ self.host, b_print=True)
        log('connected with result code '+ str(rc))
        self.client.subscribe(self.topic)
        log('subscribed to topic: '+ self.topic)

    def on_message(self, client, userdata, msg):
        self.msg_count += 1

    def reset_msg_count(self):
        self.msg_count = 0
