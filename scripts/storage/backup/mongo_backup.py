import asyncio
import os
import time
from datetime import datetime, timedelta
from scripts.utils.mylogger import mylogger

logger = mylogger(__file__)

class MongoBackup:
    
    '''
        mongo backup by python
        developrt: mr-exception
        github: mr-exception
    '''
    
    # configs:
    interval_m = 5
    outputs_dir = '/home/andre/Dropbox/amdatt/amdatt/database/backups/'
    
    host = "localhost"  # if host is your local machine leave it NA
    port = "27017"  # if mongo is on default port (37017) leave in NA
    db = 'aion'

    username = "NA"  # if there is no username set, leave it in NA
    password = "NA"  # if there is no password set, leave it in NA
    def __init__(self,collections_to_backup):
        self.collections = collections_to_backup
        self.window = 24 #hours
        self.name = 'mongo backup'
        self.date_joiner = '~'
        self.DATEFORMAT = "%Y-%m-%d{}%H:%M:%S".format(self.date_joiner)

    def render_output_locations(self,collection):
        filepath = self.outputs_dir + collection+'_'+ time.strftime(self.DATEFORMAT)
        logger.warning('filepath:%s',filepath)
        return filepath
        
    def is_up_to_date(self):
        try:
            counter = 0
            yesterday =  datetime.combine(datetime.today().date(),datetime.min.time()) - timedelta(days=1)
            with os.scandir(self.outputs_dir) as files:
                logger.warning('files:%s',files)
                for collection in self.collections:
                    for filename in files:
                        logger.warning('collection:filename=%s:%s',collection,filename.name)
                        if collection in filename.name:
                            logger.warning('file:%s',filename.name)
                            timestamp = filename.name.split('_')[-1]
                            logger.warning('timestamp:%s',timestamp)

                            timestamp = datetime.strptime(timestamp, self.DATEFORMAT)
                            if timestamp >= yesterday:
                                counter += 1
                                logger.warning('counter:%s', counter)
                                break


            if counter >= len(self.collections):
                return True
            return False

        except Exception:
            logger.error('is up to date',exc_info=True)

    async def update(self):
        try:
            command = "mongodump"
            if self.host != 'NA':
                command += " --host " + self.host
            if self.port != 'NA':
                command += " --port " + self.port
            '''
            if self.username != 'NA':
                command += " --username " + self.username
            if self.password != 'NA':
                command += " --password " + self.password
            '''
            command += ' --db '+self.db

            for collection in self.collections:
                command_tmp = command
                command_tmp += " --collection "+ collection
                command_tmp += " --out " + self.render_output_locations(collection)
                os.system(command_tmp)
                logger.warning("command:%s",command_tmp)
        except Exception:
            logger.error('update',exc_info=True)


    async def run(self):
        # create warehouse table in clickhouse if needed
        # self.create_table_in_clickhouse()
        while True:
            if self.is_up_to_date():
                logger.warning("%s SLEEPING FOR 24 hours:UP TO DATE", self.name)
                await asyncio.sleep(self.window * 60 * 60)  # sleep
            else:
                await asyncio.sleep(1)
            await self.update()