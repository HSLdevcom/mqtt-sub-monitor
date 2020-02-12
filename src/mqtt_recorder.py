import os
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
from utils.rotating_record_file_writer import RotatingRecordFileWriter
from utils.azure_blob_uploader import AzureBlobUploader
from utils.logger import Logger

class MqttRecorder:

    def __init__(self, log: Logger, writer: RotatingRecordFileWriter, records_dir: str = 'records/', max_storage_size_gb: float = None):
        self.log = log
        self.writer = writer
        self.disabled: bool = True
        self.records_dir: str = records_dir
        self.max_storage_size_gb: float = max_storage_size_gb
        self.last_storage_size = 0
        self.last_status_time = datetime.utcnow()
        self.recording_rate_mb_s = 0
        self.scheduler = None

    def start(self) -> None:
        self.log.info('starting mqtt recorder with max storage size: '+ str(self.max_storage_size_gb) + ' G')
        self.scheduler = BackgroundScheduler()

        if (self.max_storage_size_gb is not None):
            self.scheduler.add_job(self.maybe_disable_recorder, 'interval', seconds=2)

        self.scheduler.start()
        self.disabled = False

    def record(self, msg) -> None:
        if (self.disabled != True):
            self.writer.write(self.get_write_time() +' '+ msg.topic +' '+ str(msg.payload) + '\n')
    
    def get_storage_size_mb(self) -> float:
        records_size = sum(os.path.getsize(self.records_dir + f) for f in os.listdir(self.records_dir) if os.path.isfile(self.records_dir + f))
        records_size_mb = round(records_size/1000000, 1)
        return records_size_mb

    def get_storage_size_gb(self) -> float:
        return round(self.get_storage_size_mb()/1000, 3)

    def maybe_disable_recorder(self) -> None:
        if (self.disabled == False):
            if (self.get_storage_size_gb() >= self.max_storage_size_gb):
                self.log.warning('reached maximum record size ('+ str(self.max_storage_size_gb) +' G), recording disabled from now on')
                self.disabled = True
            else:
                self.disabled = False

    def get_write_time(self) -> str:
        return datetime.utcnow().strftime('%y/%m/%d %H:%M:%S.%f')[:-4] + '-UTC'

    def get_status(self):
        return {
            'disabled': self.disabled,
            'total_record_size_G': self.get_storage_size_gb(),
            'max_storage_size_G': self.max_storage_size_gb,
            'max_record_size_M': self.writer.max_record_size_mb,
            'current_record_file': self.writer.current_record_file,
            'all_record_files': self.writer.get_records(),
        }
