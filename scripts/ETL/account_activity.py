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
            'upper': 50000,
            'lower': 30000
        }

        self.columns = sorted(list(table_dict[table].keys()))
        # lst to make new df for account balance
        self.new_activity_lst = []
        self.address_lst = [] # all addresses ever on network

        # what to bring back from tables
        self.construction_cols_dict = {
            'internal_transfer': {
                'cols': ['from_addr', 'to_addr', 'transaction_hash',
                         'block_number','block_timestamp','value'],
                'value': 'value'
            },
            'token_transfers': {
                'cols': ['from_addr', 'to_addr', 'transaction_hash',
                         'block_number','transfer_timestamp','value'],
                'value': 'value'
            },
            'transaction': {
                'cols': ['from_addr', 'to_addr','transaction_hash',
                         'block_number', 'block_timestamp','value'],
                'value': 'approx_value'
            }
        }

    """
        - get addresses from start til now
        - filter for current addresses and extant addresses
    """
    def get_addresses(self,start_date,end_date):
        try:
            # get addresses from start of time til end block
            self.df = self.load_df(self.initial_date,end_date,['address'],self.table,'clickhouse')

            if self.df is not None:
                if len(self.df)>0:
                    # if the compreshensive address list is empty, refill it
                    # this occurs at ETL restart, etc.
                    if len(self.address_lst) <=0:
                        self.before = self.df[['address','block_timestamp']]
                        self.before = self.before[self.before.block_timestamp > start_date]
                        self.address_lst = self.before['address'].unique().tolist()
                    # prune til we only have from start of time til start date
                    # so that new joiners in the current block can be labelled
                    df_current = self.df[self.df.block_timestamp >= start_date and
                                              self.df.block_timestamp <= end_date]

                    df_current = df_current.compute()
                    return list(df_current['address'].unique())
            return []

        except Exception:
            logger.error('get addresses', exc_info=True)


    def determine_churn(self, current_addresses):
        try:
            # the addresses in the current block the sum to zero
            # (tranactions from the beginning, have churned
            df = self.df[self.df.from_addr == current_addresses]
            df = self.df.groupby('from_addr')['value'].sum()
            df = df.reset_index()
            df = df[df.value == 0]
            churned_addresses = df['from_addr'].unique().tolist()
            return churned_addresses
        except Exception:
            logger.error('determine churn', exc_info=True)

    def create_address_transaction(self,row,table,churned_addresses):
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
                    if row['from_addr'] in self.churned_addresses:
                        from_activity = 'churned'
                    else:
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
            for table in self.construction_cols_dict.keys():
                cols = self.construction_cols_dict[table]['cols']
                # load production data from staging
                df = self.load_df(start_date,end_date,cols,table,'mysql')
                #logger.warning('TABLE:COLS %s:%s',table,cols)

                if df is not None:
                    if len(df)>0:
                        #logger.warning("%s LOADED, WINDOW:- %s", table.upper(), df.head())
                        # convert to plus minus transactions
                        df = df.compute()
                        # get addresses in current block
                        self.address_lst = self.get_addresses(start_date,end_date)
                        churned_addresses = self.determine_churn()
                        df.apply(lambda row: self.create_address_transaction(row,table,churned_addresses), axis=1)

                        del df
                        gc.collect()

            # save data
            if len(self.new_activity_lst) > 0:
                df = pd.DataFrame(self.new_activity_lst)
                # save dataframe
                df = dd.from_pandas(df, npartitions=5)
                #logger.warning("INSIDE SAVE DF:%s", df.columns.tolist())

                self.save_df(df)
                self.df_size_lst.append(len(df))
                self.window_adjuster() # adjust size of window to load bigger dataframes

                self.address_lst += self.new_activity_lst
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
            if self.is_up_to_date(construct_table='transaction',
                                  storage_medium='mysql',
                                  window_hours=self.window):
                logger.warning("ACCOUNT ACTIVITY SLEEPING FOR 3 hours:UP TO DATE")
                await asyncio.sleep(10800)  # sleep three hours
            else:
                await asyncio.sleep(1)