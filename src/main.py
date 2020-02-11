import os
import sys
from distutils.util import strtobool
from flask import Flask, jsonify
from mqtt_subscriber import MqttSubscriber
from sub_monitor import SubMonitor
from utils.env_vars import set_env_vars
from utils.logger import Logger

log = Logger()
set_env_vars(log)
app = Flask(__name__)

flask_port = int(os.getenv('FLASK_PORT')) if ('FLASK_PORT' in os.environ) else 5000
mqtt_host = os.getenv('MQTT_HOST')
mqtt_topic = os.getenv('MQTT_TOPIC')
msg_rate_monitoring: bool = bool(strtobool(os.getenv('MSG_RATE_MONITORING'))) if ('MSG_RATE_MONITORING' in os.environ) else True
msg_rate_interval_secs = int(os.getenv('MSG_RATE_INTERVAL_SECS')) if ('MSG_RATE_INTERVAL_SECS' in os.environ) else 5
recording: bool = bool(strtobool(os.getenv('RECORDING'))) if ('RECORDING' in os.environ) else False

mqtt_sub = MqttSubscriber(log, mqtt_host, mqtt_topic)
sub_monitor = SubMonitor(log, mqtt_sub, msg_rate_interval_secs)

if (msg_rate_monitoring == True):
    sub_monitor.start()

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
    app.run(debug=False, host='0.0.0.0', port=flask_port)
