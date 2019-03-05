import asyncio
from datetime import datetime, timedelta, date, time
from statistics import mean

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
import dask.dataframe as dd
import pandas as pd
from scripts.storage.pythonMysql import PythonMysql

logger = mylogger(__file__)
# create clickhouse table

class AccountActivityWarehouse(Checkpoint):
    def __init__(self, table,checkpoint_dict=checkpoint_dict,
                 table_dict=table_dict,columns=columns):
        Checkpoint.__init__(self, table)

        self.DATEFORMAT = "%Y-%m-%d %H:%M:%S"
        self.is_up_to_date_window = 3 # hours to sleep to give reorg time to happen
        self.window = 3 #hours
        self.table = table
        self.table_dict = table_dict[table]
        self.columns = sorted(list(table_dict[table].keys()))
        # track when data for block and tx is not being updated
        self.key_params = 'checkpoint:'+ table
        self.df = ''
        self.dct = checkpoint_dict[table]
        self.table_dict = table_dict[table]
        self.initial_date = "2018-04-24 20:00:00"
        # manage size of warehouse
        self.df_size_lst = []
        self.df_size_threshold = {
            'upper': 1500,
            'lower': 1000
        }
        self.df_aa = None
        self.df_warehouse = None
        self.cols = {}
        self.cols['account_activity'] = [
            'address','account_type',
            'activity', 'event', 'value', 'transaction_hash',
            'day_of_week']
        self.cols['block_tx_warehouse'] = [
            'block_size', 'block_time', 'difficulty', 'nrg_limit',
            'nrg_reward', 'num_transactions', 'block_nrg_consumed', 'nrg_price',
            'transaction_nrg_consumed', 'transaction_hash', 'block_timestamp',
            'block_year', 'block_month', 'block_day', 'from_addr', 'to_addr']

        self.pym = PythonMongo('aion')

    def cast_date(self,x):
        x = pd.to_datetime(str(x))
        x = x.strftime(self.DATEFORMAT)
        return x

    def cast_cols(self,df):
        try:
            meta = {
                'transaction_hash': 'str',
                'address': 'str',
                'event':'str',
                'account_type':'str',
                'activity':'str',
                'value': 'float',
                'block_nrg_consumed': 'int',
                'transaction_nrg_consumed': 'int',
                'difficulty': 'int',
                'total_difficulty': 'int',
                'nrg_limit': 'int',
                'block_size': 'int',
                'block_time': 'int',
                'nrg_reward': 'float',
                'block_year': 'int',
                'block_day': 'int',
                'block_month': 'int',
                'from_addr': 'str',
                'to_addr': 'str',
                'nrg_price': 'int',
                'num_transactions': 'int'
            }
            for column, type in meta.items():
                if type =='float':
                    values = {column:0}
                    df = df.fillna(value=values)
                    df[column] = df[column].astype(float)
                elif type == 'int':
                    values = {column:0}
                    df = df.fillna(value=values)
                    df[column] = df[column].astype(int)
                elif type == 'str':
                    values = {column:'unknown'}
                    df = df.fillna(value=values)
                    df[column] = df[column].astype(str)
            return df
            #logger.warning('casted %s as %s',column,type)
        except Exception:
            logger.error('convert string', exc_info=True)


    def make_warehouse(self, df_aa, df_block_tx, external):
        #logger.warning("df_tx in make__warehose:%s", df_tx.head(5))
        #logger.warning("df_block in make_warehouse:%s", df_block.head(5))
        try:
            # join account activity and block tx warehouse
            df = df_aa.merge(df_block_tx,on=['transaction_hash'])  # do the merge
            #logger.warning('pre merge 2 columns:%s',df.columns.tolist())

            df = df.merge(external, on=['block_year','block_month','block_day'],how='left')
            #logger.warning('post merge 2 columns:%s',df.columns.tolist())
            #logger.warning("post merge 2:%s",df[['sp_close','block_day','block_timestamp','address']].head(40))
            df = df.fillna(0)

            df = df.compute()
            df.drop_duplicates(keep='first',inplace=True)
            df = dd.from_pandas(df,npartitions=15)

            #logger.warning("WAREHOUSE MADE, Merged columns:%s", df.head())
            return df
        except Exception:
            logger.error("make warehouse", exc_info=True)


    def load_external_data(self,start_date,end_date):
        try:
            # ensure start date is date, not datetime
            if isinstance(start_date,datetime):
                start_date = start_date.date()
                start_date = datetime.combine(start_date,time.min)
            if isinstance(end_date,datetime):
                end_date = end_date.date()
                end_date = datetime.combine(end_date,time.min)
                end_date = end_date + timedelta(days=1)

            df = self.pym.load_data('external',start_date,end_date)
            if df is not None:
                if len(df) > 0:
                    # convert date to correct format
                    df = df.drop('date',axis=1)
                else: #construct dataframe
                    mid_date = start_date + timedelta(days=1)
                    data = [[start_date.month,start_date.day,start_date.year,0,0,0,0],
                            [mid_date.month, mid_date.day, mid_date.year, 0, 0, 0, 0],
                            [end_date.month, end_date.day, end_date.year, 0, 0, 0, 0]]
                    df = pd.DataFrame(data, columns = [
                        'block_month', 'block_day','block_year',
                        'russell_close', 'russell_volume', 'sp_close', 'sp_volume'])
                    df = df.drop_duplicates(keep='first')
                    logger.warning('created df:%s',df.head())
                    df = dd.from_pandas(df,npartitions=1)
                return df

        except Exception:
            logger.error("load external data", exc_info=True)


    async def update_warehouse(self, input_table1, input_table2):
        try:
            offset = self.get_offset()
            if isinstance(offset,str):
                offset = datetime.strptime(offset, self.DATEFORMAT)

            # LOAD THE DATE
            start_datetime = offset
            end_datetime = start_datetime + timedelta(hours=self.window)
            self.update_checkpoint_dict(end_datetime)

            df_block_tx = self.load_df(
                start_date=start_datetime, end_date=end_datetime, table=input_table2,
                cols=self.cols[input_table2], storage_medium='clickhouse')
            logger.warning("LOADING DATA IN ACCOUNT ACTIVITY WAREHOUSE %s:%s", start_datetime, end_datetime)

            if df_block_tx is not None:
                if len(df_block_tx) > 0:
                    # load account actvity data to present date
                    df_aa = self.load_df(start_date=start_datetime,end_date=end_datetime,
                                         table=input_table1,
                                         cols=self.cols[input_table1],
                                         storage_medium='clickhouse')
                    if df_aa is not None:
                        if len(df_aa) > 0:
                            # load external data
                            external = self.load_external_data(start_datetime, end_datetime)

                            # merge dataframes
                            self.df_warehouse = self.make_warehouse(df_aa, df_block_tx, external)


                            if self.df_warehouse is not None:
                                self.df_size_lst.append(len(self.df_warehouse))
                                self.window_adjuster()
                                if len(self.df_warehouse) > 0:
                                    #logger.warning("df_warehouse columns:%s",self.df_warehouse.columns.tolist())
                                    # save warehouse to clickhouse
                                    self.update_checkpoint_dict(end_datetime)
                                    self.save_df(self.df_warehouse)
                            self.df_warehouse = None

        except Exception:
            logger.error("update warehouse", exc_info=True)

    """
        warehouse is up to date if max value in warehouse checkpoint >= max value in  
        construct table
    """

    async def run(self):
        # create warehouse table in clickhouse if needed
        #self.create_table_in_clickhouse()
        while True:
            await self.update_warehouse('account_activity','block_tx_warehouse')
            if self.is_up_to_date(construct_table='account_activity',
                                  storage_medium='clickhouse',
                                  window_hours=self.window):
                logger.warning("BLOCK_TX_WAREHOUSE UP TO DATE: WENT TO SLEEP FOR THREE HOURS")
                await asyncio.sleep(10800)
            else:
                await  asyncio.sleep(1)
