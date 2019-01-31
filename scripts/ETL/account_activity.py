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
        self.checkpoint_dict = checkpoint_dict[table]
        self.cl = PythonClickhouse('aion')
        self.redis = PythonRedis()
        self.window = 3  # hours
        self.DATEFORMAT = "%Y-%m-%d %H:%M:%S"
        self.is_up_to_date_window = 4  # hours to sleep to give reorg time to happen
        self.table = table
        self.table_dict = table_dict[table]
        # track when data for block and tx is not being updated
        self.key_params = 'checkpoint:' + table
        self.df = ''
        self.dct = checkpoint_dict[table]
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
                         'block_number','block_timestamp','value_transferred'],
                'value': 'value_transferred'
            },
            'token_transfers': {
                'cols': ['from_addr', 'to_addr', 'transaction_hash',
                         'block_number','transfer_timestamp','raw_value'],
                'value': 'scaled_value'
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


    def get_addresses(self,start_date,end_date):
        try:
            df = self.load_df(start_date,end_date,['address'],self.table,'clickhouse')
            if df is not None:
                if len(df)>0:
                    df = df.compute()
                    return list(df['address'].unique())
            return []

        except Exception:
            logger.error('get addresses', exc_info=True)



    def create_address_transaction(self,row):
        try:
            if row is not None:
                #logger.warning('%s',row)
                if isinstance(row['block_timestamp'],str):
                    row['block_timestamp'] = datetime.strptime(row['block_timestamp'],self.DATEFORMAT)

                temp_lst = [
                   {
                        'address': row['from_addr'],
                        'block_day': row['block_timestamp'].day,
                        'block_hour': row['block_timestamp'].hour,
                        'block_month':row['block_timestamp'].month,
                        'block_number':row['block_number'],
                        'block_timestamp':row['block_timestamp'],
                        'block_year':row['block_timestamp'].year,
                        'day_of_week': row['block_timestamp'].strftime('%a'),
                        'from_addr': row['from_addr'],
                        'to_addr':row['to_addr'],
                        'transaction_hash':row['transaction_hash'],
                        'value':row['value'] * -1
                   },
                    {
                        'address': row['to_addr'],
                        'block_day': row['block_timestamp'].day,
                        'block_hour': row['block_timestamp'].hour,
                        'block_month': row['block_timestamp'].month,
                        'block_number': row['block_number'],
                        'block_timestamp': row['block_timestamp'],
                        'block_year': row['block_timestamp'].year,
                        'day_of_week': row['block_timestamp'].strftime('%a'),
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

    def get_new_addresses(self,this_timestamp,address_lst):
        try:
            if self.address_lst is not None:
                if len(self.address_lst) <= 0:
                    self.address_lst = self.get_addresses(self.initial_date,this_timestamp)
            else:
                self.address_lst = self.get_addresses(self.initial_date, this_timestamp)

            return list(set(self.address_lst).difference(address_lst))
        except Exception:
            logger.error('get new addresses', exc_info=True)

    def register_new_address(self,addresses):
        try:
            if addresses is not None:
                if len(addresses)>0:
                    for address in addresses:
                        for record in self.new_activity_lst:
                            if record['address'] == 'address':
                                block_number = record['block_number']
                                block_timestamp = record['block_timestamp']

                                # add a record to the activity dictionary
                                self.new_activity_lst.append({
                                    'activity':'joined',
                                    'address': address,
                                    'block_day': block_timestamp.day,
                                    'block_hour': block_timestamp.hour,
                                    'block_month': block_timestamp.month,
                                    'block_number': block_number,
                                    'block_timestamp': block_timestamp,
                                    'block_year': block_timestamp.year,
                                    'day_of_week': block_timestamp.strftime('%a'),
                                    'from_addr': 'na',
                                    'to_addr': 'na',
                                    'transaction_hash': record['transaction_hash'],
                                    'value': record['value'],
                                })
                                break
                            #logger.warning('ACCOUNT ACTIVITY NEW ADDRESS REGISTERED')
        except Exception:
            logger.error('register new addresses', exc_info=True)


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
                df = self.load_df(start_date,end_date,cols,table,'mysql')
                #logger.warning('TABLE:COLS %s:%s',table,cols)

                if df is not None:
                    if len(df)>0:
                        #logger.warning("%s LOADED, WINDOW:- %s:%s", table.upper(), start_date, end_date)
                        # convert to plus minus transactions
                        df = df.compute()
                        df.apply(lambda row: self.create_address_transaction(row), axis=1)
                        del df
                        gc.collect()
                        if len(self.new_activity_lst) > 0:
                            for item in self.new_activity_lst:
                                if table == 'token_transfers':
                                    item['activity'] = 'token'
                                else:
                                    item['activity'] = 'native'
                #logger.warning('FINISHED %s',table.upper())


            # save data
            if len(self.new_activity_lst) > 0:
                #logger.warning('REGISTERING EVENTS %s',len(self.new_activity_lst))

                # register new events
                df = pd.DataFrame(self.new_activity_lst)
                new_activity_lst = df['address'].unique().tolist()
                new_addresses = self.get_new_addresses(end_date, new_activity_lst)
                self.register_new_address(new_addresses)

                # save dataframe
                df = dd.from_pandas(df, npartitions=5)
                self.save_df(df)
                self.df_size_lst.append(len(df))
                self.window_adjuster() # adjust size of window to load bigger dataframes

                # update the persistent addresses list if necessary
                if new_addresses is not None:
                    if len(new_addresses) > 0:
                        self.address_lst = list(set(self.address_lst + new_addresses))
                self.new_activity_lst = [] #reset for next window

            # update composite list
        except Exception:
            logger.error('get addresses', exc_info=True)

        # check max date in a construction table



    def is_up_to_date(self, construct_table):
        try:
            offset = self.checkpoint_dict['offset']
            if offset is None:
                offset = self.initial_date
                self.checkpoint_dict['offset'] = self.initial_date
            if isinstance(offset, str):
                offset = datetime.strptime(offset, self.DATEFORMAT)

            construct_max_val = self.get_value_from_mysql(construct_table, 'MAX')
            if isinstance(construct_max_val,int):
                construct_max_val = datetime.utcfromtimestamp(construct_max_val).strftime(self.DATEFORMAT)

            if isinstance(construct_max_val, str):
                construct_max_val = datetime.strptime(construct_max_val, self.DATEFORMAT)

            if offset >= construct_max_val - timedelta(hours=self.is_up_to_date_window):
                # logger.warning("CHECKPOINT:UP TO DATE")
                return True
            logger.warning("NETWORK ACTIVITY CHECKPOINT:NOT UP TO DATE")

            return False
        except Exception:
            logger.error("is_up_to_date", exc_info=True)
            return False

    async def run(self):
        # create warehouse table in clickhouse if needed
        # self.create_table_in_clickhouse()
        while True:
            await self.update()
            if self.is_up_to_date(construct_table='transaction'):
                logger.warning("ACCOUNT ACTIVITY SLEEPING FOR 1 DAY:UP TO DATE")
                await asyncio.sleep(10800)  # sleep three hours
            else:
                await asyncio.sleep(1)