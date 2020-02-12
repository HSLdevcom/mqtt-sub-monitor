import os
import sys
from distutils.util import strtobool
from flask import Flask, jsonify, send_from_directory
from markupsafe import escape
from mqtt_subscriber import MqttSubscriber
from mqtt_msg_rate_monitor import MqttMsgRateMonitor
from mqtt_recorder import MqttRecorder
from utils.rotating_record_file_writer import RotatingRecordFileWriter
from utils.azure_blob_uploader import AzureBlobUploader
from utils.env_vars import set_env_vars
from utils.logger import Logger

log = Logger()
set_env_vars(log)
app = Flask(__name__)

mqtt_host = os.getenv('MQTT_HOST')
mqtt_topic = os.getenv('MQTT_TOPIC')
msg_rate_monitoring: bool = bool(strtobool(os.getenv('MSG_RATE_MONITORING'))) if ('MSG_RATE_MONITORING' in os.environ) else True
msg_rate_interval_secs = int(os.getenv('MSG_RATE_INTERVAL_SECS')) if ('MSG_RATE_INTERVAL_SECS' in os.environ) else 5
recording: bool = bool(strtobool(os.getenv('RECORDING'))) if ('RECORDING' in os.environ) else False
max_storage_size_gb: float = float(os.getenv('MAX_STORAGE_SIZE_GB')) if ('MAX_STORAGE_SIZE_GB' in os.environ) else None
max_record_size_mb: int = int(os.getenv('MAX_RECORD_SIZE_MB')) if ('MAX_RECORD_SIZE_MB' in os.environ) else 500
uploading: bool = bool(strtobool(os.getenv('BLOB_UPLOAD_ENABLED'))) if ('BLOB_UPLOAD_ENABLED' in os.environ) else False

mqtt_sub = MqttSubscriber(log, mqtt_host, mqtt_topic)
msg_rate_monitor = MqttMsgRateMonitor(log, mqtt_sub, msg_rate_interval_secs)
writer = RotatingRecordFileWriter(log, max_record_size_mb=max_record_size_mb)
uploader = AzureBlobUploader(log, writer)
recorder = MqttRecorder(log, writer, max_storage_size_gb=max_storage_size_gb)

if (msg_rate_monitoring == True):
    msg_rate_monitor.start()

if (recording == True):
    mqtt_sub.add_recorder(recorder)
    recorder.start()

if (uploading == True):
    uploader.start()

mqtt_sub.start_sub()

@app.route('/')
def default():
    return jsonify({
        'available_paths': ['/get_latest_record', '/get_record/<record_name>', '/msg_rate_anomalies'],
        'recorder_status': recorder.get_status() if recorder is not None else None,
        'subscription_info': msg_rate_monitor.get_status()
    })

@app.route('/get_latest_record')
def latest_record():
    if (recorder is not None):
        return send_from_directory(recorder.records_dir, recorder.current_record_file)
    else:
        return 'no recorder found'

@app.route('/get_record/<record_name>')
def specific_record(record_name):
    if (recorder is not None):
        return send_from_directory(recorder.records_dir, escape(record_name))
    else:
        return 'no recorder found'

@app.route('/msg_rate_anomalies')
def anomaly_logs():
    return jsonify(msg_rate_monitor.get_anomaly_log())

flask_port = int(os.getenv('FLASK_PORT')) if ('FLASK_PORT' in os.environ) else 5000

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=flask_port)
