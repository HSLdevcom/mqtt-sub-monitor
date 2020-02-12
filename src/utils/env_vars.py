import os
from glob import glob
import csv
import traceback
from utils.logger import Logger

def set_env_vars(log: Logger):
    try:
        found_secrets = False
        for var in glob('/run/secrets/*'):
            k=var.split('/')[-1]
            v=open(var).read().rstrip('\n')
            os.environ[k] = v
            log.info('read docker secret: '+ str(k) +' (len: '+ str(len(v))+')')
            found_secrets = True
    except Exception:
        traceback.print_exc()
        pass
    if (found_secrets == False):
        log.warning('no docker secrets found')

    try:
        with open('.env', newline='') as csvfile:
            reader = csv.DictReader(csvfile, fieldnames=['key', 'value'], delimiter='=')
            for row in reader:
                os.environ[row['key']] = row['value']
    except Exception:
        log.warning('no .env file found')
        pass
