import asyncio
import gc
from datetime import datetime, timedelta, date, time
from statistics import mean

from pandas.io.json import json_normalize
from tornado import gen
from tornado.gen import coroutine

from config.df_construct_config import warehouse_inputs as cols, table_dict,columns
from scripts.ETL.checkpoint import checkpoint_dict, Checkpoint
from scripts.storage.pythonClickhouse import PythonClickhouse
from scripts.storage.pythonMongo import PythonMongo
from scripts.storage.pythonRedis import PythonRedis
from scripts.streaming.streamingDataframe import StreamingDataframe
from scripts.utils.mylogger import mylogger
from scripts.utils.poolminer import explode_transaction_hashes
from dask import dataframe as dd
import pandas as pd
from scripts.storage.pythonMysql import PythonMysql
from scripts.tablemanager.table import Table
import numpy as np

logger = mylogger(__file__)
# create clickhouse table

class AccountExternalWarehouse(Checkpoint):
    def __init__(self, table,mysql_credentials,items):
        Checkpoint.__init__(self, table)
        self.is_up_to_date_window = 24  # hours to sleep to give reorg time to happen
        self.window = 24  # hours
        self.dct = checkpoint_dict[table]
        self.checkpoint_column = 'timestamp'
        self.my = PythonMysql(mysql_credentials)
        self.mysql_credentials = mysql_credentials
        self.initial_date = datetime.strptime("2018-04-25 00:00:00",self.DATEFORMAT)
        self.columns = sorted(list(self.dct.keys()))
        # construction
        # external columns to load
        self.table = table
        self.table1 = 'account_authoritative'
        self.table2 = 'external_daily'
        self.table3 = 'external_hourly'
        self.offset = self.initial_date
        self.rename_dct = {}
        self.coin_price_cols = []
        self.items = items
        self.price_labels = ['open','low','market_cap','high','volume','close']
        self.github_labels = ['fork','release','push','watch','issue']
        self.daily_cryto = {

        }

    def am_i_up_to_date(self, offset_update=None):
        try:

            today = datetime.combine(datetime.today().date(), datetime.min.time())
            yesterday = today - timedelta(days=1)
            self.offset_update = offset_update
            if offset_update is not None:
                self.offset, self.offset_update = self.reset_offset(offset_update)
            else:
                self.offset = self.get_value_from_clickhouse(self.table)

            # first check if the ETL table is up to timestamp
            logger.warning('my max date:%s',self.offset)
            if self.offset < yesterday:
                table = {
                    self.table1 : {
                        'storage': 'mysql',
                        'db':'aion_analytics'
                    },
                    'external_daily': {
                        'storage': 'mongo',
                        'db':'aion'
                    },
                    'external_hourly':{
                        'storage': 'mongo',
                        'db': 'aion'
                    }
                }
                # get max timestamp from
                dates = []
                if self.mysql_credentials == 'staging':
                    res1,max_date1= self.is_up_to_date(
                        table=self.table1,timestamp=yesterday,
                        storage_medium='mysql',window_hours=self.window,db='aion_analytics')
                    #logger.warning('res, max_date1=%s,%s', res1, max_date1)
                    if res1:
                        dates.append(yesterday)
                    else:
                        dates.append(max_date1)

                res2,max_date2 = self.is_up_to_date(
                        table=self.table2,timestamp=yesterday,
                        storage_medium='mongo',window_hours=self.window,db='aion')
                if res2:
                    dates.append(yesterday)
                else:
                    dates.append(max_date2)

                #logger.warning('res2, max_date2=%s,%s',res2,max_date2)

                res3, max_date3 = self.is_up_to_date(
                    table=self.table3, timestamp=yesterday,
                    storage_medium='mongo', window_hours=self.window, db='aion')
                logger.warning('res3, max_date3=%s,%s',res3,max_date3)

                #logger.warning('feeder tables max dates=%s',dates)
                # compare our max timestamp to the minimum of the max dates of the tables
                if self.offset < min(dates):
                    return False
                else:
                    logger.warning('max min(timestamp) in construction table(s) =%s', min(dates))
                    return True
            return True
        except Exception:
            logger.error('am i up to timestamp', exc_info=True)

    def is_up_to_date(self,table,timestamp,storage_medium,window_hours,db):
        try:
            if storage_medium == 'mysql':
                construct_max = self.get_value_from_mysql(table,column='block_timestamp',
                                                    min_max='MAX',db=db)

            elif storage_medium == 'clickhouse':
                try:
                    construct_max = self.get_value_from_clickhouse(table,column='timestamp',
                                                        min_max='MAX',db=db)
                except:
                    logger.warning('%s table does not exist in clickhouse',table)
                    return False, self.initial_date
            elif storage_medium == 'mongo':
                construct_max = self.get_value_from_mongo(table,column='timestamp',
                                                    min_max='MAX',db=db)

            if isinstance(construct_max,int):
                construct_max = datetime.fromtimestamp(construct_max)
            if isinstance(construct_max,str):
                construct_max = datetime.strptime(construct_max, self.DATEFORMAT)
                construct_max = construct_max.date()

            #logger.warning('self.table:%s',self.table)
            #logger.warning("timestamp:construct max=%s:%s",timestamp,construct_max)
            if construct_max >= timestamp:
                # logger.warning("CHECKPOINT:UP TO DATE")
                return False, construct_max
            # logger.warning("NETWORK ACTIVITY CHECKPOINT:NOT UP TO DATE")
            return True, None
        except Exception:
            logger.error("is_up_to_date", exc_info=True)

    def adjust_labels(self,df):
        try:
            for value in list(df.columns):
                tmp = value.split(".")
                try:
                    tmp1 = tmp[1].split('_')
                    if tmp[0] == tmp1[0]:
                        val = tmp[1]
                        val = val.replace('.', '_')
                        val = val.replace('-', '_')
                        self.rename_dct[value] = val
                    else:
                        val = value
                        val = val.replace('.', '_')
                        val = val.replace('-', '_')
                        self.rename_dct[value] = val
                except:
                    #logger.warning("%s is ok!",value)
                    pass

            #logger.warning("rename dict:%s",len(self.rename_dct))
            df_temp = df.rename(index=str, columns=self.rename_dct)
            return df_temp
        except Exception:
            logger.error('adjust labels',exc_info=True)


    def make_table(self,df):
        try:
            dct = table_dict[self.table].copy()
            for value in df.columns.tolist():
                if self.string_contains(self.price_labels,value):
                    dct[value] = 'Float64'
                elif 'timestamp' in value:
                    dct[value] = 'Datetime'
                elif self.string_contains(self.github_labels,value):
                    dct[value] = 'UInt64'
                else:
                    pass

            cols = sorted(list(dct.keys()))
            x = len(table_dict[self.table].keys())
            logger.warning('LENGTH OF DICT FROM CONFIG = %s',x)
            if (len(list(df.columns))) == len(list(dct.keys())):
                logger.warning("COLUMNS MATCHED")

                self.cl.create_table(self.table, dct, cols,order_by='block_timestamp')
                self.columns = sorted(list(df.columns))
            else:
                logger.warning('length df=%s',len(list(df.columns)))
                logger.warning('length dict=%s',len(list(dct.keys())))
                logger.warning('df=%s',list(df.columns))
                logger.warning("COLUMNS NOT MATCHED-%s",list(set(list(df.columns)) - set(list(dct.keys()))))

        except Exception:
            logger.error('make table',exc_info=True)


    def string_contains(self,lst,string):
        for item in lst:
            if item in string:
                return True
        return False

    def type_to_int(self,df):
        try:
            intdict = {}
            for col in df.columns.tolist():
                if self.string_contains(['month','day','hour','year'],col):
                    intdict[col] = int
                elif self.string_contains(self.github_labels, col):
                    intdict[col] = int
                else:
                    pass
            df = df.astype(intdict)
            return df
        except Exception:
            logger.error('',exc_info=True)

    def initialize_table(self):
        try:
            timestamp = self.initial_date
            df1 = self.load_account_data(timestamp)
            cols = []

            cols1 = ['release', 'push', 'watch', 'fork', 'issue',
                     'open', 'low', 'market_cap', 'high', 'volume', 'close']
            for item in self.items:
                for col in cols1:
                    item = item.replace('.', '_')
                    item = item.replace('-', '_')
                    cols.append(item + '_' + col)
            dct = {
                'timestamp': [timestamp] * 24,
                'year': [timestamp.year] * 24,
                'month': [timestamp.month] * 24,
                'day': [timestamp.day] * 24,
                'hour': list(range(0, 24)),
            }
            cols.append('sp_close')
            cols.append('sp_volume')
            cols.append('russell_close')
            cols.append('russell_volume')
            for col in cols:
                dct[col] = np.zeros(24).tolist()

            # '''''''''
            df = pd.DataFrame(data=dct)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = self.type_to_int(df)
            df2 = dd.from_pandas(df, npartitions=1)

            df = df1.merge(df2, on=['year', 'month', 'day', 'hour'], how='inner')
            self.make_table(df)
        except:
            logger.error('table', exc_info=True)

    ''' 
    def reset_offset(self, timestamp):
        try:
            return datetime.strptime(timestamp, self.DATEFORMAT)
        except Exception:
            logger.error('reset offset :%s', exc_info=True)
    '''

    # //////////////////////////////////////////////////////////////

    def load_external_data(self,start, table):
        try:
            end = start + timedelta(days=1)
            #logger.warning('start:end=%s:%s',start,end)
            df = json_normalize(list(self.pym.db[table].find({
                'timestamp':{'$gte':start, '$lt':end}
            })))

            if df is not None:
                if len(df) > 0:
                    df = df.drop(['_id'],axis=1)
                    if table == self.table2:
                        if 'hour' in df.columns.tolist():
                            df = df.drop('hour',axis=1)

                    #logger.warning('df from %s:%s',table,df.head(10))

                    df1 = self.adjust_labels(df)

                    df1 = dd.from_pandas(df1, npartitions=1)
                    return df1

            # make empty dataframe since github is not yet uploaded
            df1 = self.make_empty_dataframe(start)
            return df1

        except Exception:
            logger.error('load external data', exc_info=True)

    def load_account_data(self,start):
        try:
            end = start + timedelta(days=1)
            qry = """select * from {}.{} where block_timestamp >= '{}' and block_timestamp 
                < '{}' """.format(self.my.schema,self.table1,start,end)
            # logger.warning("qry:%s",qry)
            df = pd.read_sql(qry, self.my.connection)
            if df is not None:
                if len(df) > 0:
                    df = df.drop(['id','big_decimal_balance', 'nonce', 'transaction_hash',
                                  'block_number_of_first_event', 'block_number','token_addr',
                                  'contract_creator'], axis=1)
                    # tack on timestamp column from timestamp column
                    df = dd.from_pandas(df, npartitions=10)
                    #logger.warning('length of df account authoritative :%s',list(df.columns))
                    #logger.warning('df account authoritative :%s',df.head(10))

                    return df
            return None
        except Exception:
            logger.error('load account data', exc_info=True)

    def make_warehouse(self, df1,df2,table):
        try:
            '''
            if table == self.table2:
                logger.warning('df1:%s',df1[['year','month','day']].head(10))
                logger.warning('df2:%s',df2[['year','month','day']].head(10))
            '''
            if df1 is not None and df2 is not None:
                if len(df1) > 0 and len(df2) > 0:
                    if table == self.table2:
                        df = df1.merge(df2,on=['year','month','day'],how='inner')
                    elif table == self.table3:
                        if 'timestamp' in df1.columns.tolist():
                            df1 = df1.drop('timestamp',axis=1)
                        df = df1.merge(df2,on=['year','month','day','hour'],how='inner')

                    del df1
                    del df2
                    gc.collect()

                    if table in [self.table3,self.table2]:
                        #logger.warning('after merge:%s',df.head(10))
                        pass
                    return df
            return None

        except Exception:
            logger.error('make warehouse', exc_info=True)

    def make_empty_dataframe(self, timestamp):
        try:
            cols = []
            cols1 = ['release', 'push', 'watch', 'fork', 'issue']

            for item in self.items:
                for col in cols1:
                    item = item.replace('.', '_')
                    item = item.replace('-', '_')
                    cols.append(item+'_'+col)
            dct = {
                'year': [timestamp.year] * 24,
                'month': [timestamp.month] * 24,
                'day': [timestamp.day] * 24,
                'hour': list(range(0,24)),
                'timestamp':[datetime(timestamp.year,timestamp.month,timestamp.day,
                             0,0,0)]*24
            }

            for col in cols:
                dct[col] = np.zeros(24).tolist()

            df = pd.DataFrame(data=dct)
            df = self.type_to_int(df)
            df = dd.from_pandas(df,npartitions=1)
            return df

        except Exception:
            logger.error('make empty dataframe',exc_info=True)

    async def update(self):
        try:

            yesterday = datetime.combine(datetime.today().date(), datetime.min.time()) - timedelta(days=1)
            offset = self.offset
            offset = datetime.combine(offset.date(), datetime.min.time())

            if offset < yesterday:
                offset = offset+timedelta(days=1)
                logger.warning('offset:%s', offset)

                # set the timestamp for the daily crypto load
                df1 = self.load_account_data(offset)
                df2 = self.load_external_data(offset,self.table2)
                #logger.warning('df2:%s',df2.head(10))
                df = self.make_warehouse(df1,df2,self.table2)
                df3 = self.load_external_data(offset, self.table3)

                df = self.make_warehouse(df,df3,self.table3)
                # logger.warning('df after warehouse:%s',df.head(10))
                self.columns = list(df.columns)
                self.save_df(df,columns=self.columns,table=self.table,timestamp_col='block_timestamp')

                if self.offset_update is not None:
                    self.update_checkpoint_dict(offset)
                    self.save_checkpoint()
                del df2, df3
                gc.collect()
        except Exception:
            logger.error('update',exc_info=True)

    def reset_offset(self,offset_update):
        try:
            key = self.key_params
            # get it from redis
            self.checkpoint_dict = self.redis.load([], '', '', key=key, item_type='checkpoint')
            if self.checkpoint_dict is not None:  # reset currently in progress
                # make a new checkpoint_dct if necessary
                if self.checkpoint_dict['start'] is not None:
                    if self.checkpoint_dict['start'] == offset_update['start']:
                        if self.checkpoint_dict['end'] == offset_update['end']:
                            offset = datetime.strptime(self.checkpoint_dict['offset'], self.DATEFORMAT)
                            end = datetime.strptime(self.checkpoint_dict['end'], self.DATEFORMAT)
                            if offset >= end:  # stop reset proceedure
                                offset = self.get_value_from_clickhouse(self.table)
                                offset_update = None
                                logger.warning('OFFSET RESET FINISHED')
                                logger.warning(" %s CHECKPOINT dictionary (re)set|retrieved and saved:%s", self.table,
                                               self.checkpoint_dict)
                            return offset, offset_update
            self.checkpoint_dict = self.dct
            self.checkpoint_dict['start'] = offset_update['start']
            self.checkpoint_dict['end'] = offset_update['end']
            self.checkpoint_dict['offset'] = offset_update['start']
            offset = datetime.strptime(self.checkpoint_dict['offset'], self.DATEFORMAT)
            logger.warning('OFFSET RESET BEGUN')
            return offset,offset_update

        except Exception:
            logger.error('reset offset',exc_info=True)

    async def run(self,offset_update):
        #self.initialize_table()
        """
        --offset up_date takes the form
        offset_update = {
            'start': datetime.strptime('2018-06-20 00:00:00,self.DATEFORMAT'),
            'end': datetime.strptime('2018-08-12 00:00:00,self.DATEFORMAT')
        }
        """
        while True:
            if self.am_i_up_to_date(offset_update):
                logger.warning("%s UP TO DATE- WENT TO SLEEP FOR %s HOURS",self.table,self.window)
                await asyncio.sleep(self.window*60*60)
            else:
                await  asyncio.sleep(1)
            await self.update()




