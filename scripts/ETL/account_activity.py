"""

- load df from account transactions for each day
- check entire previous history to determine new, existing accounts
- churn day is when sum went to zero, timestamp should be saved
- length of time on network should be recorded
-
"""
import gc
from datetime import timedelta, datetime, date

import asyncio

from config.checkpoint import checkpoint_dict
from config.df_construct_config import table_dict, columns
from scripts.ETL.checkpoint import Checkpoint
from scripts.storage.pythonClickhouse import PythonClickhouse
from scripts.storage.pythonMysql import PythonMysql
from scripts.storage.pythonRedis import PythonRedis
from scripts.utils.mylogger import mylogger
from scripts.utils.myutils import concat_dfs
import pandas as pd
import dask.dataframe as dd
from dask.multiprocessing import get

logger = mylogger(__file__)


class AccountActivity(Checkpoint):
    def __init__(self, table):
        Checkpoint.__init__(self, table)
        self.cl = PythonClickhouse('aion')
        self.my = PythonMysql('aion')
        self.redis = PythonRedis()

        self.cl = PythonClickhouse('aion')
        self.redis = PythonRedis()
        self.window = 3  # hours
        self.DATEFORMAT = "%Y-%m-%d %H:%M:%S"
        self.is_up_to_date_window = 4  # hours to sleep to give reorg time to happen
        self.table = table
        self.table_dict = table_dict[table]
        # track when data for block and tx is not being updated
        self.df = ''
        self.initial_date = datetime.strptime("2018-04-25 00:00:00",self.DATEFORMAT)
        # manage size of warehouse
        self.df_size_lst = []
        self.df_size_threshold = {
            'upper': 70000,
            'lower': 50000
        }

        self.columns = sorted(list(table_dict[table].keys()))
        # lst to make new df for account balance
        self.new_activity_lst = []
        self.address_lst = [] # all addresses ever on network

        # what to bring back from tables
        self.cols_dict = {
            'internal_transfer': {
                'cols': ['from_addr', 'to_addr', 'transaction_hash',
                         'block_number','block_timestamp','approx_value'],
                'value': 'approx_value'
            },
            'token_transfers': {
                'cols': ['from_addr', 'to_addr', 'transaction_hash',
                         'block_number','transfer_timestamp','approx_value'],
                'value': 'approx_value'
            },
            'transaction': {
                'cols': ['from_addr', 'to_addr','transaction_hash',
                         'block_number', 'block_timestamp','approx_value'],
                'value': 'approx_value'
            }
        }


    def load_df(self, start_date, end_date,cols,table,storage):
        try:
            start_date = datetime.combine(start_date, datetime.min.time())
            if storage == 'mysql':
                df = self.my.load_data(table=table, cols=cols,
                                    start_date=start_date, end_date=end_date)
            elif storage == 'clickhouse':
                df = self.cl.load_data(table=table, cols=cols,
                                    start_date=start_date, end_date=end_date)
            # logger.warning('line 157, load:%s',df['block_timestamp'])
            # convert to datetime to date

            return df
        except Exception:
            logger.warning('load_df', exc_info=True)


    def get_addresses(self,start_date):
        try:

            df = self.load_df(self.initial_date,start_date,['address'],self.table,'clickhouse')
            if df is not None:
                if len(df)>0:
                    df = df.compute()
                    return list(df['address'].unique())
            return []

        except Exception:
            logger.error('get addresses', exc_info=True)

    def create_address_transaction(self,row,table):
        try:
            if row is not None:
                block_timestamp = row['block_timestamp']
                if isinstance(row['block_timestamp'],str):
                    block_timestamp = datetime.strptime(block_timestamp,self.DATEFORMAT)
                if table == 'token_transfers':
                    #logger.warning("TOKEN TRANSFER")
                    event = "token transfer"
                else:
                    event = "native transfer"

                # DETERMING IF NEW ADDRESS
                # if first sighting is from_ then make 'value' negative
                #logger.warning('self address list:%s',self.address_lst)
                if row['from_addr'] in self.address_lst:
                    from_activity = 'active'
                elif row['from_addr'] not in self.address_lst:
                    from_activity = 'joined'
                    self.address_lst.append(row['from_addr'])

                if row['to_addr'] in self.address_lst:
                    to_activity = 'active'
                elif row['to_addr'] not in self.address_lst:
                    to_activity = 'joined'
                    self.address_lst.append(row['to_addr'])
                temp_lst = [
                   {
                        'activity': from_activity,
                        'address': row['from_addr'],
                        'block_day': block_timestamp.day,
                        'block_hour': block_timestamp.hour,
                        'block_month':block_timestamp.month,
                        'block_number':row['block_number'],
                        'block_timestamp':block_timestamp,
                        'block_year':block_timestamp.year,
                        'day_of_week': block_timestamp.strftime('%a'),
                        'event': event,
                        'from_addr': row['from_addr'],
                        'to_addr':row['to_addr'],
                        'transaction_hash':row['transaction_hash'],
                        'value':row['value'] * -1
                   },
                    {
                        'activity': to_activity,
                        'address': row['to_addr'],
                        'block_day': block_timestamp.day,
                        'block_hour': block_timestamp.hour,
                        'block_month': block_timestamp.month,
                        'block_number': row['block_number'],
                        'block_timestamp': block_timestamp,
                        'block_year': block_timestamp.year,
                        'day_of_week': block_timestamp.strftime('%a'),
                        'event': event,
                        'from_addr': row['from_addr'],
                        'to_addr': row['to_addr'],
                        'transaction_hash': row['transaction_hash'],
                        'value': row['value']
                    },
                ]

                # for each to_addr
                self.new_activity_lst = self.new_activity_lst+temp_lst
        except Exception:
            logger.error('add balance:',exc_info=True)



    async def update(self):
        try:
            # SETUP
            offset = self.get_offset()
            if isinstance(offset, str):
                offset = datetime.strptime(offset, self.DATEFORMAT)
            start_date = offset
            end_date = start_date + timedelta(hours=self.window)
            self.update_checkpoint_dict(end_date)
            # get data
            logger.warning('LOAD RANGE %s:%s',start_date,end_date)
            for table in self.cols_dict.keys():
                cols = self.cols_dict[table]['cols']
                # load production data from staging
                df = self.load_df(start_date,end_date,cols,table,'mysql')
                #logger.warning('TABLE:COLS %s:%s',table,cols)

                if df is not None:
                    if len(df)>0:
                        #logger.warning("%s LOADED, WINDOW:- %s", table.upper(), df.head())
                        # convert to plus minus transactions
                        df = df.compute()
                        if len(self.address_lst) <= 0:
                            self.address_lst = self.get_addresses(start_date)
                        df.apply(lambda row: self.create_address_transaction(row,table), axis=1)

                        del df
                        gc.collect()

            # save data
            if len(self.new_activity_lst) > 0:
                # register new events
                df = pd.DataFrame(self.new_activity_lst)
                # save dataframe
                df = dd.from_pandas(df, npartitions=5)
                #logger.warning("INSIDE SAVE DF:%s", df.columns.tolist())

                self.save_df(df)
                self.df_size_lst.append(len(df))
                self.window_adjuster() # adjust size of window to load bigger dataframes

                self.new_activity_lst = [] #reset for next window
                # logger.warning('FINISHED %s',table.upper())

            # update composite list
        except Exception:
            logger.error('get addresses', exc_info=True)

        # check max date in a construction table


    async def run(self):
        # create warehouse table in clickhouse if needed
        # self.create_table_in_clickhouse()
        while True:
            await self.update()
            if self.is_up_to_date(construct_table='transaction',window_hours=self.window):
                logger.warning("ACCOUNT ACTIVITY SLEEPING FOR 3 hours:UP TO DATE")
                await asyncio.sleep(10800)  # sleep three hours
            else:
                await asyncio.sleep(1)