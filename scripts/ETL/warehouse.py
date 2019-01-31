import asyncio
import time
from datetime import datetime, timedelta, date
from statistics import mean

from tornado import gen
from tornado.gen import coroutine

from config.df_construct_config import warehouse_inputs as cols, table_dict,columns
from scripts.ETL.checkpoint import checkpoint_dict
from scripts.storage.pythonClickhouse import PythonClickhouse
from scripts.storage.pythonRedis import PythonRedis
from scripts.streaming.streamingDataframe import StreamingDataframe
from scripts.utils.mylogger import mylogger
from scripts.utils.poolminer import explode_transaction_hashes
import dask as dd
import pandas as pd


logger = mylogger(__file__)
# create clickhouse table

class Warehouse:
    def __init__(self, table,checkpoint_dict=checkpoint_dict,
                 table_dict=table_dict,columns=columns):
        self.checkpoint_dict = checkpoint_dict[table]
        self.cl = PythonClickhouse('aion')
        self.redis = PythonRedis()
        self.data_to_process_window = 1 # hours
        self.DATEFORMAT = "%Y-%m-%d %H:%M:%S"
        self.is_up_to_date_window = 3 # hours to sleep to give reorg time to happen
        self.table = table
        self.table_dict = table_dict[table]
        self.columns = columns[table]
        # track when data for block and tx is not being updated
        self.checkpoint_column = 'block_timestamp'
        self.key_params = 'checkpoint:'+ table
        self.df = ''
        self.dct = checkpoint_dict[table]
        self.initial_date = "2018-04-23 20:00:00"
        # manage size of warehouse
        self.df_size_lst = []
        self.df_size_threshold = {
            'upper': 60000,
            'lower': 40000
        }

    def save_checkpoint(self):
        try:
            self.redis.save(self.checkpoint_dict,self.key_params,"","",type='checkpoint')
            logger.warning('block_tx_warehouse CHECKPOINT SAVED TO REDIS:%s', self.key_params)
        except Exception:
            logger.error("Construct table query",exc_info=True)

    def get_checkpoint_dict(self,col='block_timestamp', db='aion'):
        # try to load from , then redis, then clickhouse
        try:
            key = 'checkpoint:' + self.table
            self.checkpoint_dict = self.redis.load([], '', '', key=key, item_type='checkpoint')
            if self.checkpoint_dict is None: # not in redis
                self.checkpoint_dict = self.dct
                # get last date from clickhouse
                qry = "select count() from {}.{}".format(db,self.table)
                numrows = self.cl.client.execute(qry)
                if numrows[0][0] >= 1:
                    result = self.get_value_from_clickhouse(self.table,'MAX')
                    self.checkpoint_dict['offset'] = result
                    self.checkpoint_dict['timestamp'] = datetime.now().strftime(self.DATEFORMAT)
            else:
                # ensure that the offset in redis is set/reset
                if self.checkpoint_dict['offset'] is None:
                    result = self.get_value_from_clickhouse(self.table,'MAX')
                    self.checkpoint_dict['offset'] = result
                    self.checkpoint_dict['timestamp'] = datetime.now().strftime(self.DATEFORMAT)

            #logger.warning("CHECKPOINT dictionary (re)set or retrieved:%s",self.checkpoint_dict)
            return self.checkpoint_dict
        except Exception:
            logger.error("get checkpoint dict",exc_info=True)

    def create_table_in_clickhouse(self):
        try:
            self.cl.create_table(self.table,self.table_dict,self.columns)
        except Exception:
            logger.error("Create self.table in clickhosue",exc_info=True)

    def reset_offset(self,reset_value):
        try:
            self.checkpoint_dict = self.get_checkpoint_dict()
            if isinstance(reset_value,datetime) or isinstance(reset_value,date):
                reset_value = datetime.strftime(reset_value,self.DATEFORMAT)
            self.checkpoint_dict['offset'] = reset_value
            self.checkpoint_dict['timestamp'] = datetime.now().strftime(self.DATEFORMAT)
            self.save_checkpoint()
            logger.warning("CHECKPOINT reset:%s",self.checkpoint_dict)
        except Exception:
            logger.error('reset checkpoint :%s', exc_info=True)

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
                'approx_value': 'float',
                'block_nrg_consumed': 'int',
                'transaction_nrg_consumed': 'int',
                'difficulty': 'int',
                'total_difficulty': 'int',
                'nrg_limit': 'int',
                'block_size': 'int',
                'block_time': 'int',
                'approx_nrg_reward': 'float',
                'block_year': 'int',
                'block_day': 'int',
                'block_month': 'int',
                'from_addr': 'str',
                'to_addr': 'str',
                'nrg_price': 'int',
                'num_transactions': 'int'
            }
            logger.warning('df:%s',df.head())
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
                logger.warning('df when df_tx is empty: %s',df.columns.tolist())

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


    def update_checkpoint_dict(self,end_datetime):
        try:
            #logger.warning("INSIDE UPDATE CHECKPOINT DICT")

            # update checkpoint
            self.checkpoint_dict['offset'] = datetime.strftime(end_datetime + timedelta(seconds=1),
                                                               self.DATEFORMAT)
            self.checkpoint_dict['timestamp'] = datetime.now().strftime(self.DATEFORMAT)
        except Exception:
            logger.error("make warehouse", exc_info=True)

    def save_df(self,df):
        try:

            self.cl.upsert_df(df,self.columns,self.table)
            self.checkpoint_dict['timestamp'] = datetime.now().strftime(self.DATEFORMAT)
            self.save_checkpoint()
            logger.warning("BLOCK_TX_WAREHOUSE UPDATED,CHECKPOINT,offset:%s, timestamp:%s",
                           self.checkpoint_dict['offset'],
                           self.checkpoint_dict['timestamp'])
        except:
            logger.error("save dataframe to clickhouse", exc_info=True)

    async def update_warehouse(self, input_table1, input_table2):
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
                #logger.warning("Checkpoint initiated in update warehoused:%s", offset)
                if offset is None:
                    offset = self.initial_date
                self.checkpoint_dict['offset'] = offset


            # convert offset to datetime if needed
            offset = self.checkpoint_dict['offset']
            if isinstance(offset,str):
                offset = datetime.strptime(offset, self.DATEFORMAT)

            # LOAD THE DATE
            start_datetime = offset
            end_datetime = start_datetime + timedelta(hours=self.data_to_process_window)
            self.update_checkpoint_dict(end_datetime)
            logger.warning("WAREHOUSE UPDATE WINDOW- %s:%s", start_datetime,end_datetime)
            df_block = self.cl.load_data(input_table1,cols[self.table][input_table1],
                                         start_datetime,end_datetime)

            df_block = df_block.rename(columns={'month':'block_month','day':'block_day',
                                                'year':'block_year'})
            df_tx = self.cl.load_data(input_table2,cols[self.table][input_table2],
                                             start_datetime,end_datetime)

            # add balance column to df_tx
            # SLIDE WINDOW, UPSERT DATA, SAVE CHECKPOINT
            if len(df_block) > 0:
                if df_tx is None:
                    # make two dataframes to pandas
                    mycols = cols[input_table2]
                    new_cols = ['transaction_nrg_consumed' if word == 'nrg_consumed' else word for word in mycols]
                    df_tx = StreamingDataframe(input_table2,new_cols,dedup_cols=[]).df
                df_warehouse = self.make_warehouse(df_tx, df_block)
            if df_warehouse is not None:
                self.df_size_lst.append(len(df_warehouse))
                #logger.warning("WAREHOUSE length %s", self.df_size_lst)
                self.window_adjuster(offset)
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

    def window_adjuster(self, offset):
        """
        :logic:
        1)dynamically adjust size of window to control save_window when the warehouse
            is caught up to current date
        2)dynamically adjust window so to control dataframe size.
        """
        if offset >= datetime.now() - timedelta(days=3):
            self.data_to_process_window = 1 # grab one hour of data
        else:
            if len(self.df_size_lst) >= 1:
                if mean(self.df_size_lst) >= self.df_size_threshold['upper']:
                    self.data_to_process_window = round(self.data_to_process_window * .75)
                    logger.warning("block_tx_warehouse WINDOW ADJUSTED DOWNWARDS TO %s", self.data_to_process_window)
                elif mean(self.df_size_lst) <= self.df_size_threshold['lower']:
                    self.data_to_process_window = round(self.data_to_process_window * 1.25)
                    logger.warning("block_tx_warehouse WINDOW ADJUSTED UPWARDS TO %s", self.data_to_process_window)

    """
        warehouse is up to date if max value in warehouse checkpoint >= max value in  
        construct table
    """

    # check max date in a construction table
    def is_up_to_date(self,construct_table='block'):
        try:
            offset = self.checkpoint_dict['offset']
            if offset is None:
                offset = self.initial_date
                self.checkpoint_dict['offset'] = self.initial_date
            if isinstance(offset,str):
                offset = datetime.strptime(offset,self.DATEFORMAT)
            construct_max_val = self.get_value_from_clickhouse(construct_table, 'MAX')
            logger.warning("block_tx_warehouse contruct max_val in is_up_to_date:%s",construct_max_val)
            if offset >= construct_max_val - timedelta(hours=self.is_up_to_date_window):
                return True
            return False
        except Exception:
            logger.error("block_tx_warehouse s_up_to_date", exc_info=True)
            return False

    async def run(self):
        # create warehouse table in clickhouse if needed
        #self.create_table_in_clickhouse()
        while True:
            await self.update_warehouse('block','transaction')
            if self.is_up_to_date(construct_table='block'):
                logger.warning("BLOCK_TX_WAREHOUSE UP TO DATE: WENT TO SLEEP FOR THREE HOURS")

                await asyncio.sleep(10800)
            else:
                await  asyncio.sleep(5)


