import os
from apscheduler.schedulers.background import BackgroundScheduler
from utils.logger import Logger
from datetime import datetime

class MqttRecorder:

    def __init__(self, log: Logger, records_dir: str = 'records/', hourly_files: bool = False, max_record_size_gb: int = None):
        self.log = log
        self.disabled: bool = False
        self.records_dir: str = records_dir
        self.current_record_file: str = self.get_new_record_file_name()
        self.max_record_size_gb: int = max_record_size_gb
        self.b_hourly_files: bool = hourly_files
        self.scheduler = None
        self.setup_recorder_updater()
        self.log.info('set maximum record size to: '+ str(max_record_size_gb) +' G')
        self.log.info('starting mqtt recording to: '+ self.current_record_file)

    def get_new_record_file_name(self) -> str:
        return self.records_dir + datetime.utcnow().strftime('%y-%m-%d-%H') + 'UTC.txt'

    def record(self, msg) -> None:
        if (self.disabled != True):
            with open(self.current_record_file, 'a') as the_file:
                the_file.write(msg.topic +' '+ str(msg.payload) + '\n')

    def setup_recorder_updater(self) -> None:
        self.scheduler = BackgroundScheduler()

        if (self.b_hourly_files == True):
            self.log.info('set recording to use hourly files')
            self.scheduler.add_job(self.maybe_update_record_file, 'interval', seconds=1)

        if (self.max_record_size_gb is not None):
            self.scheduler.add_job(self.maybe_disable_recorder, 'interval', seconds=1)

        # start scheduler if jobs were added
        if (self.b_hourly_files == True or self.max_record_size_gb is not None):
            self.scheduler.start()

    def maybe_update_record_file(self) -> None:
        if (self.current_record_file != self.get_new_record_file_name()):
            self.current_record_file = self.get_new_record_file_name()

    def maybe_disable_recorder(self):
        records_size = sum(os.path.getsize(self.records_dir + f) for f in os.listdir(self.records_dir) if os.path.isfile(self.records_dir + f))
        records_size_MB = round(records_size/1000000, 1)
        records_size_GB = round(records_size_MB/1000, 3)
        if (self.disabled == False):
            if (records_size_GB >= self.max_record_size_gb):
                self.log.warning('reached maximum record size ('+ str(self.max_record_size_gb) +' G), recording disabled from now on')
                self.disabled = True
