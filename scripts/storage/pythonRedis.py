import json

from scripts.utils.mylogger import mylogger
import pickle
import redis
import zlib
import pandas as pd
from tornado.gen import coroutine
from datetime import datetime, timedelta
import numpy as np

logger = mylogger(__file__)
EXPIRATION_SECONDS = 86400*4 #retain for 4 days in redis

class PythonRedis:
    conn = redis.StrictRedis(
        host='localhost',
        port=6379)

    def __init__(self):
        pass

    # convert dates from timestamp[ms] to datetime[ns]
    def ms_to_date(self, ts, precision='s'):
        try:
            if isinstance(ts, int):
                # change milli to seconds
                if ts > 16307632000:
                    ts = ts // 1000
                if precision == 'ns':
                    ts = datetime.utcfromtimestamp(ts)
                    # convert to nanosecond representation
                    ts = np.datetime64(ts).astype(datetime)
                    ts = pd.Timestamp(datetime.date(ts))
                elif precision == 's':# convert to ms respresentation
                    ts = datetime.fromtimestamp(ts)

            elif isinstance(ts, datetime):
                return ts
                #logger.warning('ms_to_date: %s', ts)
            return ts
        except Exception:
            logger.error('ms_to_date', exc_info=True)
            return ts

    # delta is integer: +-
    def get_relative_day(self, day, delta):
        if isinstance(day, str):
            day = datetime.strptime('%Y-%m-%d')
        elif isinstance(day, int):
            day = self.ms_to_date(day)
        day = day + timedelta(days=delta)
        day = datetime.strftime(day, '%Y-%m-%d')
        return day

    # convert ms to string date
    def datetime_or_ts_to_str(self, ts):
        if isinstance(ts,str) == False:
            # convert to datetime if necessary
            ts = self.ms_to_date(ts)
            ts = datetime.strftime(ts, '%Y-%m-%d')
        return ts

    # key_params: list of parameters to put in key
    def compose_key(self,key_params, start_date, end_date):
        if isinstance(key_params, str):
            key_params = key_params.split(',')
        start_date = self.datetime_or_ts_to_str(start_date)
        end_date = self.datetime_or_ts_to_str(end_date)
        #logger.warning('start_date in compose key:%s', start_date)
        key = ''
        for kp in key_params:
            if not isinstance(kp, str):
                kp = str(kp)
            key += kp + ':'
        key = '{}{}:{}'.format(key, start_date, end_date)
        return key

    @coroutine
    def save(self,item, key_params, start_date, end_date,type=""):
        try:
            #convert dates to strings

            if type in ["list","dataframe",""]:
                key = self.compose_key(key_params, start_date, end_date)
                self.conn.setex(name=key, time=EXPIRATION_SECONDS,
                                value=zlib.compress(pickle.dumps(item)))
            elif type == 'checkpoint':
                '''
                self.conn.hmset(key_params,item)
                '''
                item = json.dumps(item)
                self.conn.set(key_params, item)
                self.conn.expire(key_params,EXPIRATION_SECONDS*50)

                logger.warning('CHECKPOINT UPDATED OR SAVED:%s', key_params)

        except Exception:
            logger.error('save to redis',exc_info=True)


    def load(self, key_params, start_date, end_date, key=None, item_type=''):
        try:
            if key is None:
                start_date = self.datetime_or_ts_to_str(start_date)
                end_date = self.datetime_or_ts_to_str(end_date)

                key = self.compose_key(key_params,start_date,end_date)

            logger.warning('load-item key:%s', key)
            if item_type != 'checkpoint':
                item = pickle.loads(zlib.decompress(self.conn.get(key)))

                if item_type == "dataframe":
                    logger.warning("from redis load:%s",item.head(5))
            elif item_type == 'checkpoint':
                if self.conn.exists(key):
                    #item=self.conn.hgetall(key)
                    item = self.conn.get(key)
                    item = json.loads(item)
                    item = item.decode('UTF-8')

                    logger.warning("Checkpoint loaded from redis:%s",item)
                else:
                    item = None

            return item
        except Exception:
            logger.error('load item', exc_info=True)
            return None

    @coroutine
    def save_dict(self,dct,key_params='block_tx_warehouse',type='churned'):
        try:
            if dct:
                if type == 'churned':
                    dct['warehouse'] = self.compose_key(dct['warehouse'],
                                                        dct['reference_start_date'],
                                                        dct['reference_end_date'])
                    self.save(dct,dct['key_params'],dct['reference_start_date'],
                              dct['reference_end_date'])
                elif type == 'checkpoint':
                    self.save(dct,key_params,"", "",type=type)
        except Exception:
            logger.error("save_dict", exc_info=True)




