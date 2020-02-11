from apscheduler.schedulers.background import BackgroundScheduler
from utils.logger import Logger
from datetime import datetime

class MqttRecorder:

    def __init__(self, log: Logger, records_dir: str = 'records/', hourly_files: bool = False):
        self.log = log
        self.records_dir = records_dir
        self.current_record_file: str = self.get_new_record_file_name()
        self.scheduler: None
        if (hourly_files == True):
            self.set_recording_to_hourly_files()
        self.log.info('started recording to: '+ self.current_record_file + ', hourly files: '+ str(hourly_files))

    def get_new_record_file_name(self):
        return self.records_dir + datetime.utcnow().strftime('%y-%m-%d-%H') + 'UTC.txt'

    def record(self, payload):
        with open(self.current_record_file, 'a') as the_file:
            the_file.write(str(payload) + '\n')

    def set_recording_to_hourly_files(self):
        self.scheduler = BackgroundScheduler()
        self.scheduler.add_job(self.set_hourly_record_file, 'interval', seconds=1)

    def set_hourly_record_file(self):
        if (self.current_record_file != self.get_new_record_file_name()):
            self.current_record_file = self.get_new_record_file_name()
