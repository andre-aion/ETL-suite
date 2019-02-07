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
import dask as dd
import pandas as pd
from scripts.storage.pythonMysql import PythonMysql

logger = mylogger(__file__)
# create clickhouse table

class Warehouse(Checkpoint):
    def __init__(self, table,checkpoint_dict=checkpoint_dict,
                 table_dict=table_dict,columns=columns):
        Checkpoint.__init__(self, table)

        self.DATEFORMAT = "%Y-%m-%d %H:%M:%S"
        self.is_up_to_date_window = 3 # hours to sleep to give reorg time to happen
        self.window = 3 #hours
        self.table = table
        self.table_dict = table_dict[table]
        self.columns = columns[table]
        # track when data for block and tx is not being updated
        self.key_params = 'checkpoint:'+ table
        self.df = ''
        self.dct = checkpoint_dict[table]
        self.table_dict = table_dict[table]
        self.initial_date = "2018-04-23 20:00:00"
        # manage size of warehouse
        self.df_size_lst = []
        self.df_size_threshold = {
            'upper': 60000,
            'lower': 40000
        }

        # what to bring back from tables
        self.construction_cols_dict = {
            'block': cols[self.table]['block'],
            'transaction':cols[self.table]['transaction'],
        }

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

    def make_warehouse(self, df_tx, df_block):
        #logger.warning("df_tx in make__warehose:%s", df_tx.head(5))
        #logger.warning("df_block in make_warehouse:%s", df_block.head(5))
        try:
            df_block = df_block.map_partitions(explode_transaction_hashes)
            #logger.warning('df_block after explode hashes:%s',df_block['transaction_hashes'].tail(30))
            df_block.reset_index()

            # join block and transaction table
            if len(df_tx)>0:
                df = df_block.merge(df_tx, how='left',
                                    left_on='transaction_hashes',
                                    right_on='transaction_hash')  # do the merge
                if df is not None:
                    if 'transaction_hashes' in df.columns.tolist():
                        df = df.drop(['transaction_hashes'], axis=1)
                df = df.map_partitions(self.cast_cols)
            else:
                #convert to pandas for empty join
                df_block = df_block.compute()
                df = df_block.reindex(df_block.columns.union(df_tx.columns), axis=1)

                df = df.reset_index()
                df['block_timestamp'] = df['block_timestamp'].apply(lambda x:self.cast_date(x))

                #logger.warning('block timestamp after empty merge:%s',df['block_timestamp'])
                # reconvert to dask
                df = dd.dataframe.from_pandas(df, npartitions=1)
                df = df.reset_index()
                df = df.drop(['level_0','index'],axis=1)
                df = self.cast_cols(df)



            #logger.warning("WAREHOUSE MADE, Merged columns:%s", df.head())
            return df
        except Exception:
            logger.error("make warehouse", exc_info=True)

    async def update_warehouse(self, input_table1, input_table2):
        try:
            offset = self.get_offset()
            if isinstance(offset,str):
                offset = datetime.strptime(offset, self.DATEFORMAT)

            # LOAD THE DATE
            start_datetime = offset
            end_datetime = start_datetime + timedelta(hours=self.window)
            self.update_checkpoint_dict(end_datetime)
            logger.warning("WAREHOUSE UPDATE WINDOW- %s:%s", start_datetime,end_datetime)
            df_block = self.load_df(
                start_date=start_datetime,end_date=end_datetime,table=input_table1,
                cols=self.construction_cols_dict['block'],storage_medium='mysql')

            # add balance column to df_tx
            # SLIDE WINDOW, UPSERT DATA, SAVE CHECKPOINT
            if df_block is not None:
                if len(df_block) > 0:
                    df_tx = self.load_df(
                        start_date=start_datetime, end_date=end_datetime, table=input_table2,
                        cols=self.construction_cols_dict['transaction'], storage_medium='mysql')
                    if df_tx is None:
                        # make two dataframes to pandas
                        mycols = self.construction_cols_dict[input_table2]
                        df_tx = StreamingDataframe(input_table2,mycols,[]).get_df()
                        logger.warning("SD df_tx columns:%s", df_tx.columns.tolist())

                    rename = {'nrg_consumed': 'transaction_nrg_consumed',
                              'approx_value': 'value'}
                    df_tx = df_tx.rename(columns=rename)

                    df_warehouse = self.make_warehouse(df_tx, df_block)
                    if df_warehouse is not None:
                        self.df_size_lst.append(len(df_warehouse))
                        self.window_adjuster()
                        if len(df_warehouse) > 0:
                            # save warehouse to clickhouse
                            self.update_checkpoint_dict(end_datetime)
                            self.save_df(df_warehouse)

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
            await self.update_warehouse('block','transaction')
            if self.is_up_to_date(construct_table='block',
                                  storage_medium='mysql',
                                  window_hours=self.window):
                logger.warning("BLOCK_TX_WAREHOUSE UP TO DATE: WENT TO SLEEP FOR THREE HOURS")
                await asyncio.sleep(10800)
            else:
                await  asyncio.sleep(1)
