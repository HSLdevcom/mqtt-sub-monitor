import os
from datetime import datetime
from utils.logger import Logger
from apscheduler.schedulers.background import BackgroundScheduler

class RotatingRecordFileWriter:

    def __init__(self, log: Logger, records_dir: str = 'records/', max_record_size_mb: int = None):
        self.log = log
        self.records_dir: str = records_dir
        self.current_record_file: str = ''
        self.current_record_file: str = self.get_new_record_name()
        self.current_record_file_index: int = 0
        self.max_record_size_mb: int = max_record_size_mb
        self.log.info('using max record size: '+ str(self.max_record_size_mb) + ' M')
        self.written_records = []
        self.scheduler = BackgroundScheduler()
        self.scheduler.add_job(self.maybe_update_record_file, 'interval', seconds=2)
        self.scheduler.start()

    def write(self, to_write_str: str) -> None:
        with open(self.records_dir + self.current_record_file, 'a') as the_file:
            the_file.write(to_write_str)

    def maybe_update_record_file(self) -> None:
        if (self.get_current_record_file_size() >= self.max_record_size_mb):
            self.written_records.append(self.current_record_file)
            self.current_record_file = self.get_new_record_name()

    def get_current_record_file_size(self) -> int:
        try:
            records_size = os.path.getsize(self.records_dir + self.current_record_file)
            return int(round(records_size/1000000))
        except FileNotFoundError:
            return 0

    def get_new_record_name(self) -> str:
        datetime_str = datetime.utcnow().strftime('%y-%m-%d-%H:%M:%S')
        if (self.current_record_file[:17] == datetime_str):
            self.current_record_file_index += 1
        else:
            self.current_record_file_index = 0
        return datetime_str + 'UTC_' + str(self.current_record_file_index)+'.txt'
