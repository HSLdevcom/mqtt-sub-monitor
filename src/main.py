from apscheduler.schedulers.background import BackgroundScheduler
from mqtt_subscriber import MqttSubscriber
from datetime import datetime

mqtt_sub = MqttSubscriber(host='mqtt.hsl.fi', topic='/hfp/v2/journey/ongoing/#')

def report_stats():
    low_msg_rate_str = 'low msg rate in sub:\n' if mqtt_sub.msg_count < 100 else '' 
    msg_rate_str = str(mqtt_sub.msg_count) +' msg/2s at '+ datetime.utcnow().strftime('%y/%m/%d %H:%M:%S')
    with open('sub_log.txt', 'a') as the_file:
        the_file.write(low_msg_rate_str)
        the_file.write(msg_rate_str +'\n')
    mqtt_sub.reset_msg_count()

mqtt_monitor = BackgroundScheduler()
mqtt_monitor.add_job(report_stats, 'interval', seconds=2)
mqtt_monitor.start()

mqtt_sub.start_sub()
