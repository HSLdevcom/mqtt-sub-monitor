import os
import sys
from distutils.util import strtobool
from flask import Flask, jsonify
from mqtt_subscriber import MqttSubscriber
from mqtt_msg_rate_monitor import MqttMsgRateMonitor
from mqtt_recorder import MqttRecorder
from utils.env_vars import set_env_vars
from utils.logger import Logger

log = Logger()
set_env_vars(log)

mqtt_host = os.getenv('MQTT_HOST')
mqtt_topic = os.getenv('MQTT_TOPIC')
msg_rate_monitoring: bool = bool(strtobool(os.getenv('MSG_RATE_MONITORING'))) if ('MSG_RATE_MONITORING' in os.environ) else True
msg_rate_interval_secs = int(os.getenv('MSG_RATE_INTERVAL_SECS')) if ('MSG_RATE_INTERVAL_SECS' in os.environ) else 5
recording: bool = bool(strtobool(os.getenv('RECORDING'))) if ('RECORDING' in os.environ) else False
record_hourly_files: bool = bool(strtobool(os.getenv('RECORD_HOURLY_FILES'))) if ('RECORD_HOURLY_FILES' in os.environ) else False
max_record_size_gb: float = float(os.getenv('MAX_RECORD_SIZE_GB')) if ('MAX_RECORD_SIZE_GB' in os.environ) else None

mqtt_sub = MqttSubscriber(log, mqtt_host, mqtt_topic)
msg_rate_monitor = MqttMsgRateMonitor(log, mqtt_sub, msg_rate_interval_secs)

if (msg_rate_monitoring == True):
    msg_rate_monitor.start()

recorder: MqttRecorder = None
if (recording == True):
    recorder = MqttRecorder(log, 'records/', hourly_files=record_hourly_files, max_record_size_gb=max_record_size_gb)
    mqtt_sub.add_recorder(recorder)

mqtt_sub.start_sub()

app = Flask(__name__)

@app.route('/')
def default():
    return "paths available: /recorder_status, /msg_rate_anomalies & /msg_rate_status" 

@app.route('/recorder_status')
def recorder_status():
    if (recorder is not None):
        return jsonify(recorder.get_status())
    else: 
        return 'no recorder found'

@app.route('/msg_rate_anomalies')
def anomaly_logs():
    return jsonify(msg_rate_monitor.get_anomaly_log())

@app.route('/msg_rate_status')
def sub_status():
    return jsonify(msg_rate_monitor.get_status())

flask_port = int(os.getenv('FLASK_PORT')) if ('FLASK_PORT' in os.environ) else 5000

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=flask_port)
