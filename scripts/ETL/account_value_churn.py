import asyncio
import time
from datetime import datetime, timedelta, date
from statistics import mean

from tornado import gen
from tornado.gen import coroutine

from config.df_construct_config import warehouse_inputs as cols, table_dict,columns
from scripts.ETL.checkpoint import checkpoint_dict, Checkpoint
from scripts.storage.pythonClickhouse import PythonClickhouse
from scripts.storage.pythonRedis import PythonRedis
from scripts.streaming.streamingDataframe import StreamingDataframe
from scripts.utils.mylogger import mylogger
from scripts.utils.poolminer import explode_transaction_hashes
import dask.dataframe as dd
import pandas as pd
from scripts.storage.pythonMysql import PythonMysql

logger = mylogger(__file__)
# create clickhouse table

class AccountValueChurn(Checkpoint):
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
        self.initial_date = "2018-04-23 20:00:00"
        # manage size of warehouse
        self.df_size_lst = []
        self.df_size_threshold = {
            'upper': 30000,
            'lower': 20000
        }
        self.df_aa = None
        self.df_warehouse = None
        self.cols = {}
        self.cols['account_activity'] = [
            'block_timestamp', 'address',
            'activity', 'event', 'value', 'transaction_hash',
            'day_of_week']
        self.cols['block_tx_warehouse'] = [
            'block_size', 'block_time', 'difficulty', 'nrg_limit',
            'nrg_reward', 'num_transactions', 'block_nrg_consumed', 'nrg_price',
            'transaction_nrg_consumed', 'transaction_hash', 'block_timestamp',
            'block_year', 'block_month', 'block_day', 'from_addr', 'to_addr']



    def cast_date(self,x):
        x = pd.to_datetime(str(x))
        x = x.strftime(self.DATEFORMAT)
        return x

    def cast_cols(self,df):
        try:
            meta = {
                'block_number': 'int',
                'transaction_hash': 'str',
                'miner_address': 'str',
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

    def label_churn(self,row,churned_addresses):
        if row['address'] in churned_addresses:
            #logger.warning("churned_labeled")
            row['activity'] = 'churned'
        return row['activity']

    def determine_churn(self,df):
        try:
            df1 = df[df.value >= 0] # filter out double entry
            df1 = df1.groupby('address')['value'].sum()
            df1 = df1.reset_index()
            df1 = df1[df1.value == 0] # churn == 0 balance

            df1 = df1.compute()
            churned_addresses = df1['address'].unique().tolist()
            #logger.warning('length churned addresses:%s',len(churned_addresses))
            if len(churned_addresses) > 0:
                self.df_warehouse['activity'] = self.df_warehouse.apply(lambda row:
                                                              self.label_churn(row,churned_addresses),
                                                                        axis=1,meta=object)

        except Exception:
            logger.error('determine churn:', exc_info=True)


    def make_warehouse(self, df_aa, df_block_tx):
        #logger.warning("df_tx in make__warehose:%s", df_tx.head(5))
        #logger.warning("df_block in make_warehouse:%s", df_block.head(5))
        try:
            # join account activity and block tx warehouse
            df_aa = df_aa.drop('block_timestamp',axis=1)
            df = df_aa.merge(df_block_tx,on=['transaction_hash'])  # do the merge
            #logger.warning("post merge:%s",df.head(10))
            #df = df.map_partitions(self.cast_cols)
            #df = df.drop(['level_0','index'],axis=1)
            # deduplicate
            df = df.compute()
            df.drop_duplicates(keep='first', inplace=True)
            df = dd.from_pandas(df,npartitions=5)

            #logger.warning("WAREHOUSE MADE, Merged columns:%s", df.head())
            return df
        except Exception:
            logger.error("make warehouse", exc_info=True)

    def update_account_activity_df(self,df_aa,end_datetime):
        try:
            initial_date = datetime.strptime(self.initial_date, self.DATEFORMAT)
            if self.df_aa is None:
                self.df_aa = self.cl.load_data(table='account activity', cols=self.cols['account_activity'],
                                               start_date=initial_date,
                                               end_date=end_datetime)
            else:
                # concatenate new data to existing data
                self.df_aa = self.df_aa.append(df_aa, ignore_index=True)

        except Exception:
            logger.error("up account activity", exc_info=True)

    async def update_warehouse(self, input_table1, input_table2):
        try:
            offset = self.get_offset()
            if isinstance(offset,str):
                offset = datetime.strptime(offset, self.DATEFORMAT)

            # LOAD THE DATE
            start_datetime = offset
            end_datetime = start_datetime + timedelta(hours=self.window)
            self.update_checkpoint_dict(end_datetime)

            cols = ['block_size', 'block_time', 'difficulty', 'nrg_limit',
                    'nrg_reward', 'num_transactions', 'block_nrg_consumed', 'nrg_price',
                    'transaction_nrg_consumed', 'transaction_hash', 'block_timestamp',
                    'block_year', 'block_month', 'block_day']
            df_block_tx = self.load_df(
                start_date=start_datetime, end_date=end_datetime, table=input_table2,
                cols=cols, storage_medium='clickhouse')
            logger.warning("make account_value_churn %s:%s", start_datetime, end_datetime)

            if df_block_tx is not None:
                if len(df_block_tx) > 0:
                    self.update_account_activity_df()
                    # load all data to present date
                    df_aa = self.load_df(start_datetime,end_datetime,'account_activity',
                                         self.cols['block_tx_warehouse'],
                                         storage_medium='clickhouse')
                    if df_aa is not None:
                        if len(df_aa) > 0:
                            self.update_account_activity_df(df_aa,end_datetime)
                            # make two dataframes to pandas
                            self.df_warehouse = self.make_warehouse(df_aa, df_block_tx)
                            # determine churn

                            if self.df_warehouse is not None:
                                self.df_size_lst.append(len(self.df_warehouse))
                                self.window_adjuster()
                                if len(self.df_warehouse) > 0:
                                    self.determine_churn(self.df_warehouse)
                                    #logger.warning("df_warehouse columns:%s",self.df_warehouse.columns.tolist())

                                    # save warehouse to clickhouse
                                    self.update_checkpoint_dict(end_datetime)
                                    self.save_df(self.df_warehouse)
                            self.df_warehouse = None
                    self.all_df = None

        except Exception:
            logger.error("update warehouse", exc_info=True)

    def get_value_from_clickhouse(self,table,min_max='MAX'):
        try:
            qry = "select count() from {}.{}".format('aion', table)
            numrows = self.cl.client.execute(qry)
            if numrows[0][0] >= 1:
                qry = """select {}({}) from {}.{} AS result LIMIT 1""" \
                    .format(min_max,self.checkpoint_column, 'aion', table)
                result = self.cl.client.execute(qry)
                #logger.warning('%s value from clickhouse:%s',min_max,result[0][0])
                return result[0][0]
            return self.initial_date  #if block_tx_warehouse is empty
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
