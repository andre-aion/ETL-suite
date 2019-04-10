import asyncio
import gc
from datetime import datetime, timedelta, date, time
from pprint import pprint
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

def set_vars(cryptos):
    try:
        groupby_dct = {
            'index': {},
            'github': {}
        }
        github_cols = ['watch', 'fork', 'issue', 'release', 'push']
        index_cols = ['close', 'high', 'low', 'market_cap', 'volume']
        idvars = {
            'index' :[],
            'github':[]
        }
        vars_dict = {
            'github': {
                'watch': [],
                'fork': [],
                'issue': [],
                'release': [],
                'push': [],
            },
            'index':{
                'close': [],
                'high': [],
                'low': [],
                'market_cap': [],
                'volume': []
            }
        }
        for crypto in cryptos:
            if crypto == 'bitcoin-cash':
                crypto = 'bitcoin_cash'
            for col in github_cols:
                key = crypto + '_' + col
                vars_dict['github'][col].append(key)
                idvars['github'].append(key)
                groupby_dct['github'][key] = 'sum'
            for col in index_cols:
                key = crypto + '_' + col
                vars_dict['index'][col].append(key)
                idvars['index'].append(key)
                groupby_dct['index'][key] = 'sum'
        return groupby_dct, vars_dict, idvars
    except Exception:
        logger.error('set groupby dict', exc_info=True)

def get_coin_name(x,length):
    return x[:-length]


class CryptoDaily(Checkpoint):
    def __init__(self, table,items):
        Checkpoint.__init__(self, table)
        self.is_up_to_date_window = 24  # hours to sleep to give reorg time to happen
        self.window = 24  # hours
        self.dct = checkpoint_dict[table]
        self.checkpoint_column = 'timestamp'
        self.checkpoint_key = table
        self.initial_date = datetime.strptime("2018-04-25 00:00:00",self.DATEFORMAT)
        self.columns = sorted(list(self.dct.keys()))
        # construction
        # external columns to load
        self.table = table
        self.table2 = 'external_daily'
        self.table3 = 'github'
        self.offset = self.initial_date
        self.rename_dct = {}
        self.coin_price_cols = []
        self.items = items
        self.price_labels = ['open','low','market_cap','high','volume','close']
        self.github_labels = ['fork','release','push','watch','issue']
        self.daily_cryto = {

        }
        self.groupby_dict, self.vars_dict, self.idvars = set_vars(self.items)
        self.idvars['index'].append('timestamp') # ensure that timestamp remains in the melting operations

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
            offset = self.offset
            if isinstance(self.offset,date):
                offset= datetime.combine(self.offset,datetime.min.time())
            if offset < yesterday:
                table = {
                    'external_daily': {
                        'storage': 'mongo',
                        'db':'aion'
                    },
                    'github':{
                        'storage': 'mongo',
                        'db': 'aion'
                    }
                }
                # get max timestamp from
                dates = []
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

                # compare our max timestamp to the minimum of the max dates of the tables
                if offset < min(dates):
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
                construct_max = self.get_value_from_mysql(table,column='timestamp',
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
            if isinstance(construct_max, date):
                construct_max = datetime.combine(construct_max, datetime.min.time())

            #logger.warning('self.table:%s',self.table)
            #logger.warning("timestamp:construct max=%s:%s",timestamp,construct_max)
            if construct_max.date() >= timestamp.date():
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
                        val = val.replace('0x_', 'Ox_')
                        self.rename_dct[value] = val
                    else:
                        val = value
                        val = val.replace('.', '_')
                        val = val.replace('-', '_')
                        val = val.replace('0x_', 'Ox_')
                        self.rename_dct[value] = val
                except:
                    #logger.warning("%s is ok!",value)
                    pass

            #logger.warning("rename dict:%s",len(self.rename_dct))
            df_temp = df.rename(index=str, columns=self.rename_dct)
            return df_temp
        except Exception:
            logger.error('adjust labels',exc_info=True)

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


    # //////////////////////////////////////////////////////////////

    def load_external_data(self,start, table):
        try:
            end = start + timedelta(days=1)
            #logger.warning('start:end=%s:%s',start,end)
            df = json_normalize(list(self.pym.db[table].find({
                'timestamp':{'$gte':start, '$lt':end}
            })))
            to_drop = ['0x_release','0x_push','0x_watch','0x_fork','0x_issue',
                        '0x_open','0x_low','0x_market_cap','0x_high','0x_volume','0x_close',
                        'Ox_release', 'Ox_push', 'Ox_watch', 'Ox_fork', 'Ox_issue',
                        'Ox_open', 'Ox_low', 'Ox_market_cap', 'Ox_high', 'Ox_volume', 'Ox_close'
                       ]
            if df is not None:
                if len(df) > 0:
                    df = df.drop(['_id'],axis=1)
                    for col in to_drop:
                        if col in df.columns:
                            df = df.drop(col, axis=1)
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

    def make_empty_dataframe(self, timestamp):
        try:
            cols = []
            cols1 = ['release', 'push', 'watch', 'fork', 'issue']

            for item in self.items:
                for col in cols1:
                    item = item.replace('.', '_')
                    item = item.replace('-', '_')
                    item = item.replace('0x','Ox')
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


    def melt_df(self, df, table, dct, offset):
        try:
            df = df.compute()
            df.reset_index()
            if table == 'github':
                temp_dct = {
                    'watch': [],
                    'fork': [],
                    'issue': [],
                    'release': [],
                    'push': [],
                    'timestamp':[],
                    'crypto':[]

                }
            else:
                temp_dct = {
                    'close': [],
                    'high': [],
                    'low': [],
                    'market_cap': [],
                    'volume': [],
                    'sp_volume':[],
                    'sp_close':[],
                    'russell_volume':[],
                    'russell_close':[],
                    'crypto':[]
                }
            if table == 'github':  # date for each coin
                temp_dct['timestamp'] = [datetime(offset.year, offset.month, offset.day, 0, 0, 0)] * len(self.items)

            # loop through items
            counter = 0
            for key, values in dct.items():
                for col in values:
                    # label for each coin, only run once
                    if counter == 0:
                        key_len = len(key) + 1
                        temp_dct['crypto'].append(get_coin_name(col,key_len))
                        if table == 'external_daily':  # russell and sp for each coin
                            for item in ['russell_close', 'russell_volume', 'sp_close', 'sp_volume']:
                                try:
                                    tmp = df[[item]]
                                    val = tmp.values[0]
                                except:
                                    val = [0]
                                temp_dct[item].append(val[0])
                    # get value from dataframe
                    try:
                        tmp = df[[col]]
                        val = tmp.values[0]
                    except:
                        val = [0]
                    temp_dct[key].append(val[0])
                counter += 1
            # convert to dataframe
            df = pd.DataFrame.from_dict(temp_dct)
            #logger.warning('%s df after melt:%s',table,df)
            #df = dd.from_pandas(df,npartitions=5)
            return df
        except Exception:
            logger.error('melt coins', exc_info=True)

    def make_crypto_df_long(self, df, table, offset):
        try:
            dct = self.vars_dict['index']
            if table == 'github':
                agg_dct = self.groupby_dict['github']
                dct = self.vars_dict['github']
                df = df.drop('hour',axis=1) # the hour column is not required
                # do a daily aggregate for each coin
                df = df.groupby(['year','month','day']).agg(agg_dct)
                #logger.warning('post groupby to remove hour:%s',df.head(10))

            df = self.melt_df(df,table,dct,offset)

            #logger.warning("df length:%s",len(df))
            return df
        except Exception:
            logger.error('make daily long df', exc_info=True)

    def save_crypto_df_long(self,df1,df2,offset):
        try:
            # first join the frames
            df1 = df1.merge(df2, on=['crypto'], how='inner')
            #logger.warning("df after merge:%s",df1.head(20))
            self.save_df(df1,columns=list(df1.columns),table='crypto_daily',timestamp_col='timestamp')
            if self.offset_update is not None:
                self.update_checkpoint_dict(offset)
                self.save_checkpoint()
            del df1, df2

        except Exception:
            logger.error('save crypto',exc_info=True)


    async def update(self):
        try:

            yesterday = datetime.combine(datetime.today().date(), datetime.min.time()) - timedelta(days=1)
            offset = self.offset
            if isinstance(offset,date):
                offset = datetime.combine(offset, datetime.min.time())
            offset = datetime.combine(offset.date(), datetime.min.time())

            if offset < yesterday:
                offset = offset+timedelta(days=1)
                logger.warning('offset:%s', offset)

                # set the timestamp for the daily crypto load
                df2 = self.load_external_data(offset,self.table2)
                df_long2 = self.make_crypto_df_long(df2,self.table2,offset) # make the long dataframe
                #logger.warning('df2:%s',df2.head(10))
                df3 = self.load_external_data(offset, self.table3)
                df_long3 = self.make_crypto_df_long(df3,self.table3,offset) # make the long dataframe

                # save the long version of the df
                self.save_crypto_df_long(df_long2,df_long3,offset)

                del df2, df3, df_long2, df_long3
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




