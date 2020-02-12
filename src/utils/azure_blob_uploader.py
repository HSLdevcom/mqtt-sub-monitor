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
        self.blob_account_name: str = 'mqttrecords'
        self.blob_container_name = os.getenv('AZURE_BLOB_CONTAINER_NAME')
        self.blob_access_key = os.getenv('AZURE_BLOB_ACCESS_KEY')
        self.blob_conn_string = os.getenv('AZURE_BLOB_CONNECTION_STRING')

    def start(self):
        self.blob_service = BlockBlobService(
            account_name=self.blob_account_name, 
            account_key=self.blob_access_key
            )

        self.blob_service.create_container(self.blob_container_name)

        self.log.info('starting to upload records to container: '+ self.blob_container_name + ' under blob account: '+ self.blob_account_name)
        self.scheduler.add_job(self.upload_records, 'interval', seconds=5)
        self.scheduler.start()

    def upload_records(self):
        records_to_upload = self.writer.get_records()
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
