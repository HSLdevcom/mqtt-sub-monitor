import os
from time import sleep
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from utils.logger import Logger
from utils.rotating_record_file_writer import RotatingRecordFileWriter
from azure.storage.blob import BlockBlobService, PublicAccess

class AzureBlobUploader:

    def __init__(self, log: Logger, writer: RotatingRecordFileWriter, records_dir: str = 'records/'):
        self.log = log
        self.records_dir: str = records_dir
        self.writer = writer
        self.uploaded_records = []
        self.scheduler = BackgroundScheduler()
        self.blob_service = None
        self.blob_account_name: str = os.getenv('BLOB_ACCOUNT_NAME')
        self.blob_container_name = os.getenv('BLOB_CONTAINER_NAME')
        self.blob_lifetime_hours = float(os.getenv('BLOB_LIFETIME_HOURS')) if ('BLOB_LIFETIME_HOURS' in os.environ) else None
        self.blob_access_key = os.getenv('BLOB_ACCESS_KEY')

    def start(self):
        self.blob_service = BlockBlobService(
            account_name=self.blob_account_name, 
            account_key=self.blob_access_key
            )

        self.blob_service.create_container(self.blob_container_name)

        self.log.info('starting to upload records to container: '+ self.blob_container_name + ' under blob account: '+ self.blob_account_name)
        self.scheduler.add_job(self.upload_records, 'interval', seconds=5)
        if (self.blob_lifetime_hours is not None):
            self.scheduler.add_job(self.delete_old_blobs, 'interval', seconds=5)
        self.scheduler.start()

    def get_records_to_upload(self):
        return [fn for fn in os.listdir(self.records_dir) if ('.txt' in fn and fn != self.writer.current_record_file)]

    def upload_records(self):
        records_to_upload = self.get_records_to_upload()
        for to_upload in records_to_upload:
            if (to_upload not in self.uploaded_records):
                try:
                    self.log.info('uploading file: '+ to_upload)
                    self.blob_service.create_blob_from_path(
                        self.blob_container_name, to_upload, self.records_dir + to_upload)
                    self.uploaded_records.append(to_upload)
                    self.delete_uploaded_file(to_upload)
                except Exception:
                    sleep(2)
                    pass
    
    def delete_uploaded_file(self, uploaded):
        if os.path.exists(self.records_dir + uploaded):
                os.remove(self.records_dir + uploaded)

    def delete_old_blobs(self):
        generator = self.blob_service.list_blobs(self.blob_container_name)
        blob_lifetime_secs = round(self.blob_lifetime_hours * 60 * 60)
        utc_now = datetime.utcnow()
        for blob in generator:
            blob_time_str = blob.name[:17]
            blob_time = datetime.strptime(blob_time_str, '%y-%m-%d-%H:%M:%S')
            time_diff = utc_now - blob_time
            blob_age_secs = round(time_diff.total_seconds())
            if (blob_age_secs >= blob_lifetime_secs):
                try:
                    self.blob_service.delete_blob(self.blob_container_name, blob.name)
                    self.log.debug('deleted blob: '+ blob.name)
                except Exception:
                    sleep(2)
                    pass
