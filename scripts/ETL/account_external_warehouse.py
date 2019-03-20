import asyncio
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
    def __init__(self, table,mysql_credentials):
        Checkpoint.__init__(self, table)
        self.is_up_to_date_window = 24  # hours to sleep to give reorg time to happen
        self.window = 24  # hours
        self.dct = checkpoint_dict[table]
        self.checkpoint_column = 'date'
        self.my = PythonMysql(mysql_credentials)
        self.initial_date = datetime.strptime("2018-04-25 00:00:00",self.DATEFORMAT)
        self.columns = sorted(list(self.dct.keys()))
        # construction
        # external columns to load
        self.table1 = 'account_authoritative'
        self.table2 = 'external'
        self.offset = self.initial_date

    # /////////////////////////// UTILS ///////////////////////////

    def am_i_up_to_date(self, table1, table2):
        try:
            today = datetime.combine(datetime.today().date(), datetime.min.time())
            yesterday = today - timedelta(days=1)
            # first check if the ETL table is up to date
            res,my_max_date =  self.is_up_to_date(
                    table=self.table,date=yesterday,
                    storage_medium='clickhouse',window_hours=self.window,db='aion')
            if res:
                return True
            else: # if not up to date check feeder tables
                table = {
                    'account_authoritatitve' : {
                        'storage': 'mysql',
                        'db':'aion_analytics'
                    },
                    'external_daily': {
                        'storage': 'mongo',
                        'db':'aion'
                    },
                    'github':{
                        'storage': 'mongo',
                        'db': 'aion'
                    }
                }
                # get max date from
                res1,max_date1= self.is_up_to_date(
                    table='account_authoritative',date=yesterday,
                    storage_medium='mysql',window_hours=self.window,db='aion_analytics')

                res2,max_date2 = self.is_up_to_date(
                        table='external',date=yesterday,
                        storage_medium='mongo',window_hours=self.window,db='aion')

                res3, max_date3 = self.is_up_to_date(
                    table='external', date=yesterday,
                    storage_medium='mongo', window_hours=self.window, db='aion')
                dates = [self.datetime_to_date(max_date1), self.datetime_to_date(max_date2),
                         self.datetime_to_date(max_date3)]

                logger.warning('feeder tables max dates=%s',dates)
                # compare our max date to the minimum of the max dates of the tables
                if my_max_date <= min(dates):
                    self.offset = my_max_date
                    return False
                else:
                    logger.warning('max r=date in construction table =%s', min(dates))
                    return True
        except Exception:
            logger.error('am i up to date', exc_info=True)

    def is_up_to_date(self,table,date,storage_medium,window_hours,db):
        try:
            if storage_medium == 'mysql':
                max_val = self.get_value_from_mysql(table,
                                                              column='block_timestamp',
                                                              min_max='MAX',db=db)

            elif storage_medium == 'clickhouse':
                max_val = self.get_value_from_clickhouse(table,column='date',
                                                                   min_max='MAX',db=db)
            elif storage_medium == 'mongo':
                max_val = self.get_value_from_mongo(table,column='date',
                                                              min_max='MAX',db=db)

            if isinstance(max_val,int):
                max_val = datetime.fromtimestamp(max_val)
            if isinstance(max_val, str):
                max_val = datetime.strptime(max_val, self.DATEFORMAT)
                max_val = max_val.date()

            logger.warning('self.table:%s',self.table)
            logger.warning("measure date:construct max=%s:%s",date,max_val)
            if max_val >= date:
                # logger.warning("CHECKPOINT:UP TO DATE")
                return True, None
            # logger.warning("NETWORK ACTIVITY CHECKPOINT:NOT UP TO DATE")
            return False, max_val
        except Exception:
            logger.error("is_up_to_date", exc_info=True)

    def adjust_labels(self,df):
        try:
            rename_dct = {}
            coin_price_cols = []
            for col in list(df.columns):
                tmp = col.split(".")
                col = col.replace('.', '_')
                col = col.replace('-', '_')
                col = col.replace('0x_', 'Ox_')
                try:
                    tmp1 = tmp[1].split('_')
                    if tmp[0] == tmp1[0]:
                        rename_dct[col] = tmp[1]
                    else:
                        coin_price_cols.append(col)
                except:
                    coin_price_cols.append(col)
                    logger.warning("%s is ok!",col)
            #logger.warning("rename dict:%s",rename_dct)
            df = df.rename(index=str, columns=rename_dct)
            #logger.warning("post rename columns:%s",df.columns)

            #self.create_table(df, rename_dct, coin_price_cols)
            return df
        except Exception:
            logger.error('adjust labels',exc_info=True)

    def create_table(self,df,rename_dct,coin_price_cols):
        try:
            dct = table_dict[self.table].copy()
            for key,value in rename_dct.items():
                value = value.replace('.','_')
                value = value.replace('-','_')
                value = value.replace('0x_','Ox_')

                dct[value] = 'UInt64'
            for value in coin_price_cols:
                if value == 'date':
                    dct[value] = 'Datetime'
                else:
                    #logger.warning('coin price col:%s',col)
                    value = value.replace('.','_')
                    value = value.replace('-', '_')
                    value = value.replace('0x_', 'Ox_')

                    dct[value] = 'Float64'
            cols = sorted(list(dct.keys()))
            x = len(table_dict[self.table].keys())
            logger.warning('DDDDDDDDDDDDDD = %s',x)
            if (len(list(df.columns))+len(table_dict[self.table].keys())) == len(list(dct.keys())):
                logger.warning("COLUMNS MATCHED")
                self.cl.create_table(self.table, dct, cols)
            else:
                logger.warning('length df=%s',len(list(df.columns)))
                logger.warning('length dict=%s',len(list(dct.keys())))
                logger.warning('df=%s',list(df.columns))
                #logger.warning("COLUMNS NOT MATCHED-%s",list(set(list(df.columns)) - set(list(dct.keys()))))

        except Exception:
            logger.error('create table',exc_info=True)

    # //////////////////////////////////////////////////////////////

    def load_external_data(self,date):
        try:
            df = json_normalize(list(self.pym.db[self.table2].find({
                'date':{'$eq':date}
            })))
            if df is not None:
                if len(df) > 0:
                    for col in list(df.columns):
                        if 'processed' in col:
                            df = df.drop(col,axis=1)
                    df = df.drop('_id',axis=1)
                    #logger.warning('df from external data:%s',list(df.columns))

                    df = self.adjust_labels(df)
                    df = dd.from_pandas(df, npartitions=25)
                    # add arbitrary column for join
                    df = df.assign(a=1)
                    return df
            return None
        except Exception:
            logger.error('load external data', exc_info=True)

    def load_account_data(self,offset):
        try:
            start_date = offset
            end_date = offset + timedelta(days=1)
            qry = """select * from {}.{} where block_timestamp >= '{}' and block_timestamp 
                < '{}' """.format(self.my.schema,self.table1,start_date,end_date)
            df = pd.read_sql(qry, self.my.connection)
            if df is not None:
                if len(df) > 0:
                    df = df.drop(['id','big_decimal_balance', 'nonce', 'transaction_hash',
                                  'block_number_of_first_event', 'block_number',
                                  'contract_creator'], axis=1)
                    # tack on date column from timestamp column
                    df = dd.from_pandas(df, npartitions=25)
                    df = df.assign(a=1)
                    logger.warning('df account authoritative :%s',list(df.columns))
                    return df
            return None
        except Exception:
            logger.error('load account data', exc_info=True)

    def make_warehouse(self, df1,df2):
        try:
            if df1 is not None and df2 is not None:
                if len(df1) > 0 and len(df2) > 0:
                    df = df1.merge(df2,on='a')
                    df = df.drop(['a'],axis=1)
                    return df
                    logger.warning('after merge:%s',df.head(10))
            return None

        except Exception:
            logger.error('make warehouse', exc_info=True)

    async def update(self):
        try:
            # find last data loaded
            #offset = self.get_value_from_clickhouse(self.table)
            offset = self.offset
            offset = datetime.combine(offset.date(), datetime.min.time())
            yesterday = datetime.combine(datetime.today().date(), datetime.min.time()) - timedelta(days=1)

            if offset < yesterday:
                offset = offset+timedelta(days=1)

                df1 = self.load_account_data(offset)
                if df1 is not None:
                    df2 = self.load_external_data(offset)
                    if df2 is not None:
                        df = self.make_warehouse(df1,df2)
                        if df is not None:
                            logger.warning('df after warehouse:%s',df.head(10))
                            # save dataframe
                            self.save_df(df)
                            pass
        except Exception:
            logger.error('update',exc_info=True)

    async def run(self):
        await self.update()
        '''
        #self.create_table_in_clickhouse()
        while True:
            if self.am_i_up_to_date(self.table1,self.table2):
                logger.warning("%s UP TO DATE- WENT TO SLEEP FOR %s HOURS",self.table,self.window)
                await asyncio.sleep(self.window*60*60)
            else:
                await  asyncio.sleep(1)
            await self.update()
        '''
