import os
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
from utils.logger import Logger

class MqttRecorder:

    def __init__(self, log: Logger, records_dir: str = 'records/', hourly_files: bool = False, max_record_size_gb: int = None):
        self.log = log
        self.disabled: bool = True
        self.records_dir: str = records_dir
        self.current_record_file: str = self.get_new_record_name()
        self.all_record_files: list = [self.get_new_record_name()]
        self.max_record_size_gb: int = max_record_size_gb
        self.b_hourly_files: bool = hourly_files
        self.scheduler = None
        self.last_status_time = datetime.utcnow()
        self.last_record_size = 0
        self.recording_rate_mb_s = 0

    def start(self) -> None:
        self.log.info('starting msg recording to: '+ self.current_record_file + ' with max record size: '+ str(self.max_record_size_gb) + ' G')
        self.scheduler = BackgroundScheduler()

        if (self.b_hourly_files == True):
            self.log.info('set recording to use hourly files')
            self.scheduler.add_job(self.maybe_update_record_file, 'interval', seconds=2)

        if (self.max_record_size_gb is not None):
            self.scheduler.add_job(self.maybe_disable_recorder, 'interval', seconds=2)

        self.scheduler.add_job(self.log_recording_rate, 'interval', seconds=10)
        self.scheduler.start()
        self.disabled = False

    def record(self, msg) -> None:
        if (self.disabled != True):
            with open(self.records_dir + self.current_record_file, 'a') as the_file:
                the_file.write(self.get_write_time() +' '+ msg.topic +' '+ str(msg.payload) + '\n')
    
    def maybe_update_record_file(self) -> None:
        new_record_name = self.get_new_record_name()
        if (self.current_record_file != new_record_name):
            self.current_record_file = new_record_name
            self.all_record_files.append(new_record_name)

    def get_records_size_mb(self):
        records_size = sum(os.path.getsize(self.records_dir + f) for f in os.listdir(self.records_dir) if os.path.isfile(self.records_dir + f))
        records_size_mb = round(records_size/1000000, 1)
        return records_size_mb

    def get_records_size_gb(self):
        return round(self.get_records_size_mb()/1000, 3)

    def maybe_disable_recorder(self):
        if (self.disabled == False):
            if (self.get_records_size_gb() >= self.max_record_size_gb):
                self.log.warning('reached maximum record size ('+ str(self.max_record_size_gb) +' G), recording disabled from now on')
                self.disabled = True

    def log_recording_rate(self):
        status_update_time = datetime.utcnow()
        records_size_mb = self.get_records_size_mb()

        status_interval_secs = (status_update_time - self.last_status_time).total_seconds()
        records_size_delta_mb = records_size_mb - self.last_record_size

        self.last_status_time = status_update_time
        self.last_record_size = records_size_mb

        self.recording_rate_mb_s = records_size_delta_mb/status_interval_secs
        data_rate_mb_min = self.recording_rate_mb_s * 60

        self.log.info('recording at rate mb/s: '+ str(round(self.recording_rate_mb_s, 3)) + ' = /min: '+ str(round(data_rate_mb_min, 3)) + ' /h: '+ str(round(data_rate_mb_min * 60, 3)))
    

    def get_new_record_name(self) -> str:
        return datetime.utcnow().strftime('%y-%m-%d-%H') + 'UTC.txt'        

    def get_write_time(self):
        return datetime.utcnow().strftime('%y/%m/%d %H:%M:%S.%f')[:-4] + '-UTC'

    def get_status(self):
        return {
            'disabled': self.disabled,
            'total_record_size_G': self.get_records_size_gb(),
            'recording_rate_mb_s': round(self.recording_rate_mb_s, 3),
            'recording_rate_mb_h': round(self.recording_rate_mb_s*60*60, 3),
            'max_record_size_G': self.max_record_size_gb,
            'current_record_file': self.current_record_file,
            'all_record_files': self.all_record_files,
        }
