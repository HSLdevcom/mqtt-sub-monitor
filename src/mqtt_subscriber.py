import paho.mqtt.client as mqtt
from datetime import datetime

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
        self.client.loop_forever()

    def on_connect(self, client, userdata, flags, rc):
        print('connected with result code '+str(rc))
        connect_info = 'connected with result code '+str(rc)+' at '+ datetime.utcnow().strftime('%y/%m/%d %H:%M:%S')
        broker_info = 'broker: '+ self.host + ' topic: '+ self.topic
        with open('sub_log.txt', 'a') as the_file:
            the_file.write(connect_info +'\n')
            the_file.write(broker_info + '\n')
        self.client.subscribe(self.topic)

    def on_message(self, client, userdata, msg):
        # print(msg.topic)
        self.msg_count += 1

    def reset_msg_count(self):
        self.msg_count = 0
