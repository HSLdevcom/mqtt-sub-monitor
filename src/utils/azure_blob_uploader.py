import os
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from utils.logger import Logger
from utils.rotating_record_file_writer import RotatingRecordFileWriter

class AzureBlobUploader:

    def __init__(self, log: Logger, writer: RotatingRecordFileWriter, records_dir: str = 'records/'):
        self.log = log
        self.records_dir: str = records_dir
        self.writer = writer
        self.uploaded_records = []
        self.scheduler = BackgroundScheduler()
        self.blob_access_key = os.getenv('AZURE_BLOB_ACCESS_KEY')
        self.blob_conn_string = os.getenv('AZURE_BLOB_CONNECTION_STRING')

    def start(self):
        self.log.info('starting to upload records to azure blob storage')
        self.scheduler.add_job(self.upload_records, 'interval', seconds=5)
        self.scheduler.start()

    def upload_records(self):
        records_to_upload = self.writer.written_records
        uploaded = []
        for to_upload in records_to_upload:
            if (to_upload not in self.uploaded_records):
                self.log.info('uploading file: '+ to_upload)
                self.uploaded_records.append(to_upload)
