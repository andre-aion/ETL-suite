import time
from datetime import timedelta, datetime, date

import asyncio

from config.checkpoint import checkpoint_dict
from config.df_construct_config import table_dict, columns
from scripts.ETL.checkpoint import Checkpoint
from scripts.storage.pythonClickhouse import PythonClickhouse
from scripts.storage.pythonRedis import PythonRedis
from scripts.utils.mylogger import mylogger
from scripts.utils.myutils import concat_dfs
import pandas as pd
import dask.dataframe as dd
from dask.multiprocessing import get

logger = mylogger(__file__)


class AccountTransactions(Checkpoint):
    def __init__(self, table):
        Checkpoint.__init__(self, table)
        self.cl = PythonClickhouse('aion')
        self.redis = PythonRedis()
        self.checkpoint_dict = checkpoint_dict[table]
        self.cl = PythonClickhouse('aion')
        self.redis = PythonRedis()
        self.data_to_process_window = 6  # hours
        self.DATEFORMAT = "%Y-%m-%d %H:%M:%S"
        self.is_up_to_date_window = 8  # hours to sleep to give reorg time to happen
        self.table = table
        self.table_dict = table_dict[table]
        # track when data for block and tx is not being updated
        self.checkpoint_column = 'block_timestamp'
        self.key_params = 'checkpoint:' + table
        self.df = ''
        self.dct = checkpoint_dict[table]
        self.initial_date = "2018-04-25 12:00:00"
        # manage size of warehouse
        self.df_size_lst = []
        self.df_size_threshold = {
            'upper': 60000,
            'lower': 40000
        }

        self.columns = sorted(list(table_dict[table].keys()))
        self.input_cols = ['block_timestamp','block_number','value','from_addr',
                            'to_addr','year','month','day','transaction_hash']
        # lst to make new df for account balance
        self.new_lst = []


    def load_df(self, offset,table='transaction'):
        try:
            start_date = offset
            if not isinstance(start_date,datetime):
                start_date = datetime.combine(start_date, datetime.min.time())
            end_date = start_date + timedelta(hours=self.data_to_process_window)
            logger.warning("ACCOUNT BALANCE UPDATE WINDOW- %s:%s", start_date, end_date)

            df = self.cl.load_data(table=table, cols=self.input_cols,
                                   start_date=start_date, end_date=end_date)
            # logger.warning('line 157, load:%s',df['block_timestamp'])
            # convert to datetime to date

            return df
        except Exception:
            logger.warning('load_df', exc_info=True)



    def save_df(self, df):
        try:

            self.cl.upsert_df(df, self.columns, self.table)
            self.checkpoint_dict['timestamp'] = datetime.now().strftime(self.DATEFORMAT)
            self.save_checkpoint()
            logger.warning("BLOCK_TX_WAREHOUSE UPDATED,CHECKPOINT,offset:%s, timestamp:%s",
                           self.checkpoint_dict['offset'],
                           self.checkpoint_dict['timestamp'])
        except:
            logger.error("save dataframe to clickhouse", exc_info=True)

    def create_balance(self,row):
        try:
            #logger.warning("line 87, row:%s",row)
            if row is not None:
                temp_lst = [
                   {
                        'address': row['from_addr'],
                        'block_day': row['day'],
                        'block_month':row['month'],
                        'block_number':row['block_number'],
                        'block_timestamp':row['block_timestamp'],
                        'block_year':row['year'],
                       'day_of_week': row['block_timestamp'].strftime('%a'),
                       'from_addr': row['from_addr'],
                        'tx_nrg_consumed': row['nrg_consumed'],
                        'nrg_price': row['nrg_price'],
                        'to_addr':row['to_addr'],
                        'transaction_hash':row['transaction_hash'],
                        'value':row['value'] * -1,
                   },
                    {
                        'address': row['to_addr'],
                        'block_day': row['day'],
                        'block_month': row['month'],
                        'block_number': row['block_number'],
                        'block_timestamp': row['block_timestamp'],
                        'block_year': row['year'],
                        'day_of_week': row['block_timestamp'].strftime('%a'),
                        'from_addr': row['from_addr'],
                        'tx_nrg_consumed': row['nrg_consumed'],
                        'nrg_price': row['nrg_price'],
                        'to_addr': row['to_addr'],
                        'transaction_hash': row['transaction_hash'],
                        'value': row['value'],

                    },

                ]
                # for each to_addr
                self.new_lst = self.new_lst+temp_lst
        except Exception:
            logger.error('add balance:',exc_info=True)


    async def update(self):
        try:
            if self.checkpoint_dict is None:
                self.checkpoint_dict = self.get_checkpoint_dict()
                """
                1) get checkpoint dictionary
                2) if offset is not set
                    - set offset as max from warehouse
                    - if that is zero, set to genesis blcok
                """

            # handle reset or initialization
            if self.checkpoint_dict['offset'] is None:
                offset = self.get_value_from_clickhouse(self.table, min_max='MAX')
                # logger.warning("Checkpoint initiated in update warehoused:%s", offset)
                if offset is None:
                    offset = self.initial_date
                self.checkpoint_dict['offset'] = offset

            # convert offset to datetime if needed
            offset = self.checkpoint_dict['offset']
            if isinstance(offset, str):
                offset = datetime.strptime(offset, self.DATEFORMAT)

            # LOAD THE DATE
            # go backwards two days to ensure no data lost, upsert will deduplicate
            start_datetime = offset
            end_datetime = start_datetime + timedelta(hours=self.data_to_process_window)
            self.update_checkpoint_dict(end_datetime)
            df_input = self.load_df(start_datetime)

            if len(df_input) > 0:
                df_input = df_input.compute()
                df_input.apply(self.create_balance, axis=1)
                #logger.warning('line 156  before df map_partitions:%s', df_input.head(5))
                # make dataframe
                if len(self.new_lst) > 0:
                    df = pd.DataFrame(self.new_lst)
                    # convert to dask
                    df = dd.from_pandas(df, npartitions=1)
                    #logger.warning('line 168 df before save:%s',df.head(5))
                    self.new_lst = []
                    self.save_df(df)

        except Exception:
            logger.error('update:',exc_info=True)


    # check max date in a construction table
    def is_up_to_date(self, construct_table):
        try:
            offset = self.checkpoint_dict['offset']
            if offset is None:
                offset = self.initial_date
                self.checkpoint_dict['offset'] = self.initial_date
            if isinstance(offset, str):
                offset = datetime.strptime(offset, self.DATEFORMAT)

            construct_max_val = self.get_value_from_clickhouse(construct_table, 'MAX')
            if isinstance(construct_max_val, str):
                construct_max_val = datetime.strptime(construct_max_val, self.DATEFORMAT)
                construct_max_val = construct_max_val.date()

            if offset >= construct_max_val - timedelta(days=self.is_up_to_date_window):
                #logger.warning("CHECKPOINT:UP TO DATE")
                return True
            logger.warning("ACCOUNT BALANCE CHECKPOINT:NOT UP TO DATE")

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
                logger.warning("ACCOUNT BALANCE SLEEPING FOR 1 DAY:UP TO DATE")
                await asyncio.sleep(10800)  # sleep one day
            else:
                await asyncio.sleep(1)
