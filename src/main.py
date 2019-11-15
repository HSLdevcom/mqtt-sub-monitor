import os
import sys
from flask import Flask, jsonify
from mqtt_subscriber import MqttSubscriber
from sub_monitor import SubMonitor

app = Flask(__name__)

# mqtt_host='mqtt.hsl.fi'
# mqtt_topic='/hfp/v2/journey/ongoing/#'
try:
    mqtt_host = os.environ['MQTT_HOST']
    mqtt_topic = os.environ['MQTT_TOPIC']
except Exception:
    print('env variables MQTT_HOST or MQTT_TOPIC missing, exiting app')
    sys.exit()
try:
    monitor_interval_secs = int(os.environ['INTERVAL_SECS'])
except Exception:
    monitor_interval_secs = 4

mqtt_sub = MqttSubscriber(mqtt_host, mqtt_topic)
sub_monitor = SubMonitor(mqtt_sub, monitor_interval_secs)
mqtt_sub.start_sub()

@app.route('/')
def default():
    return "paths available: /anomalies & /status" 

@app.route('/anomalies')
def anomaly_logs():
    return jsonify(sub_monitor.get_anomaly_log())

@app.route('/status')
def sub_status():
    return jsonify(sub_monitor.get_status())

if __name__ == '__main__':
    app.run(debug=False,host='0.0.0.0')
