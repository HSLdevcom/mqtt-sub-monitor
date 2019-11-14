from apscheduler.schedulers.background import BackgroundScheduler
from mqtt_subscriber import MqttSubscriber
from datetime import datetime
from logger import log

class SubMonitor:

    def __init__(self, mqtt_sub: MqttSubscriber, monitor_interval_secs: int):
        self.mqtt_sub = mqtt_sub
        self.monitor_interval_secs = monitor_interval_secs
        self.prev_msg_count = None
        self.prev_msg_time = None
        self.anomaly_log = []
        self.scheduler = BackgroundScheduler()
        self.scheduler.add_job(self.report_sub_stats, 'interval', seconds=monitor_interval_secs)
        self.scheduler.start()

    def report_sub_stats(self):
        current_count = self.mqtt_sub.msg_count
        self.mqtt_sub.reset_msg_count()
        time_str = datetime.utcnow().strftime('%y/%m/%d %H:%M:%S')
        if (self.prev_msg_count is not None and self.prev_msg_count > 0):
            rate_change_ratio = round((current_count - self.prev_msg_count)/self.prev_msg_count, 2)
            rate_change_str = str(round(rate_change_ratio * 100))
            msg_rate_str = str(current_count) +' msg/'+str(self.monitor_interval_secs)+'s - '+ rate_change_str + ' %'
            
            status_dict = { 
                'time_utc': time_str, 
                'msg_rate_'+str(self.monitor_interval_secs)+'s': current_count,
                'msg_rate_change_p': round(rate_change_ratio*100, 2)
            }

            if (rate_change_ratio > 0.7):
                log('msg rate increased:')
                self.anomaly_log.append(status_dict)
            if (rate_change_ratio < -0.7):
                log('msg rate dropped:')
                self.anomaly_log.append(status_dict)
            log(msg_rate_str)

        self.prev_msg_count = current_count
        self.prev_msg_time = time_str

    def get_anomaly_log(self):
        return self.anomaly_log

    def get_status(self):
        return {
            'msg_rate_'+str(self.monitor_interval_secs)+'s': self.prev_msg_count,
            'broker': self.mqtt_sub.host,
            'topic': self.mqtt_sub.topic,
            'last_msg_time_utc': self.prev_msg_time
        }
