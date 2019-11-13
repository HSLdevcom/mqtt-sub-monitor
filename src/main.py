from flask import Flask, jsonify
from mqtt_subscriber import MqttSubscriber
from sub_monitor import SubMonitor

app = Flask(__name__)

mqtt_sub = MqttSubscriber(host='mqtt.hsl.fi', topic='/hfp/v2/journey/ongoing/#')
sub_monitor = SubMonitor(mqtt_sub, 2)
mqtt_sub.start_sub()

@app.route("/anomalies")
def anomaly_logs():
    return jsonify(sub_monitor.get_anomaly_log())

@app.route("/status")
def sub_status():
    return jsonify(sub_monitor.get_status())

if __name__ == '__main__':
    app.run(debug=False,host='0.0.0.0')
