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
        fh = open('.env', 'r')
        lines = fh.read().splitlines()
        for line in lines:
            line.rstrip('\n')
            row = line.partition('=')
            os.environ[row[0]] = row[2]
        fh.close()
    except Exception:
        log.warning('no .env file found')
        pass
