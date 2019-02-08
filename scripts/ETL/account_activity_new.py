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

# get contract address
contract_addresses = []
my = PythonMysql('aion')
DATEFORMAT = "%Y-%m-%d %H:%M:%S"
initial_date = datetime.strptime("2018-04-25 00:00:00", "%Y-%m-%d %H:%M:%S")
start_date = my.date_to_int(initial_date)
end_date = my.date_to_int(datetime.now())
qry = """SELECT contract_addr FROM aion.contract WHERE deploy_timestamp >= {} AND 
              deploy_timestamp <= {} ORDER BY deploy_timestamp""".format(start_date, end_date)
df = pd.read_sql(qry, my.connection)
if len(df) > 0:
    contract_addresses = df['contract_addr'].tolist()

def get_account_type(address):
    try:
        my = PythonMysql('aion')
        # check contracts
        qry = """select address,transaction_hash from aion.account 
                                where address = '{}' """.format(address)
        df = pd.read_sql(qry, my.connection)
        if df is not None:
            if len(df) > 0:
                transaction_hash = df['transaction_hash'].unique().tolist()
                del df
                gc.collect()
                # logger.warning('transaction_hash searching for miner %s:%s',address,transaction_hash)
                if transaction_hash[0] == '':
                    # logger.warning("MINER FOUND:%s",transaction_hash)
                    return 'miner'
                else:
                    # logger.warning("AIONNER FOUND:%s",transaction_hash)

                    return 'aionner'

        return 'aionner'
    except Exception:
        logger.error('get_account_type:%s', exc_info=True)

def create_address_transaction(row,table,churned_addresses,address_lst,
                               new_activity_lst):
    try:
        #logger.warning('%s:',row['from_addr'])
        if row is not None:
            block_timestamp = row['block_timestamp']
            if isinstance(row['block_timestamp'],str):
                block_timestamp = datetime.strptime(block_timestamp,DATEFORMAT)
            if isinstance(row['block_timestamp'],int):
                block_timestamp = datetime.fromtimestamp(row['block_timestamp'])

            if table == 'token_transfers':
                event = "token transfer"
            else:
                event = "native transfer"

            # DETERMING IF NEW ADDRESS
            # if first sighting is from_ then make 'value' negative
            #logger.warning('self address list:%s',self.address_lst)
            if row['from_addr'] in address_lst:
                from_activity = 'active'
                if churned_addresses is not None:
                    if len(churned_addresses) > 0:
                        if row['from_addr'] in churned_addresses:
                            from_activity = 'churned'
                            logger.warning("ADDRESS CHURNED:%s",row['from_addr'])
            elif row['from_addr'] not in address_lst:
                from_activity = 'joined'
                address_lst.append(row['from_addr'])
            if row['to_addr'] in address_lst:
                to_activity = 'active'
            elif row['to_addr'] not in address_lst:
                to_activity = 'joined'
                address_lst.append(row['to_addr'])

            account_type_from = get_account_type(row['from_addr'])
            account_type_to = get_account_type(row['to_addr'])
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
                    'account_type':account_type_from,
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
                    'account_type': account_type_to,
                    'from_addr': row['from_addr'],
                    'to_addr': row['to_addr'],
                    'transaction_hash': row['transaction_hash'],
                    'value': row['value']
                },
            ]

            # for each to_addr
            new_activity_lst = new_activity_lst+temp_lst
            return new_activity_lst

    except Exception:
        logger.error('create address transaction:',exc_info=True)

def calling_create_address_transaction(df,table,churned_addresses,address_lst,
                               new_activity_lst):
    try:
        #logger.warning('%s',df.head(10))
        tmp_lst = df.apply(create_address_transaction, axis=1,
                                    args=(table, churned_addresses, address_lst,
                                    new_activity_lst,))
        new_activity_lst += tmp_lst
        return new_activity_lst
    except Exception:
        logger.error('calling create address ....:', exc_info=True)

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

        self.df = None
        self.df_history = None
        self.churn_cols = ['address', 'value','from_addr','block_timestamp']

        self.initial_date = datetime.strptime("2018-04-25 00:00:00",self.DATEFORMAT)
        # manage size of warehouse
        self.df_size_lst = []
        self.df_size_threshold = {
            'upper': 1500,
            'lower': 1000
        }

        self.columns = sorted(list(table_dict[table].keys()))
        # lst to make new df for account balance
        self.new_activity_lst = []
        self.address_lst = [] # all addresses ever on network
        self.churned_addresses = []

        # what to bring back from tables
        self.construction_cols_dict = {
            'internal_transfer': {
                'cols': ['from_addr', 'to_addr', 'transaction_hash',
                         'block_number','block_timestamp','approx_value'],
                'value': 'value'
            },
            'token_transfers': {
                'cols': ['from_addr', 'to_addr', 'transaction_hash',
                         'block_number','transfer_timestamp','approx_value'],
                'value': 'value'
            },
            'transaction': {
                'cols': ['from_addr', 'to_addr','transaction_hash',
                         'block_number', 'block_timestamp','approx_value'],
                'value': 'approx_value'
            }
        }

        # account type
        self.contract_addresses = None

    """
        - get addresses from start til now
        - filter for current addresses and extant addresses
    """
    def get_addresses(self,start_date,end_date):
        try:
            if self.df is None:
                # get addresses from start of time til end of block under consideration
                self.df = self.load_df(self.initial_date,end_date,self.churn_cols,
                                       self.table,'clickhouse')
                self.df = self.df.repartition(npartitions=30)
            else:
                if len(self.df)>0:

                    # if the compreshensive address list is empty, refill it
                    # this occurs at ETL restart, etc.
                    if self.address_lst is None:
                        self.address_lst = []
                    if len(self.address_lst) <=0:
                        self.before = self.df[self.churn_cols]
                        self.before = self.before[self.before.block_timestamp > start_date]
                        self.before = self.before[['address']]
                        self.before = self.before.compute()
                        self.address_lst = self.before['address'].unique().tolist()
                    # prune til we only have from start of time til start date
                    # so that new joiners in the current block can be labelled
                    if isinstance(start_date,int):
                        start_date = datetime.fromtimestamp(start_date)
                    if isinstance(end_date, int):
                        end_date = datetime.fromtimestamp(end_date)
                    df_current = self.df[(self.df['block_timestamp'] >= start_date) &
                                         (self.df['block_timestamp'] <= end_date)]

                    df_current = df_current[['address']]
                    df_current = df_current.compute()
                    return list(df_current['address'].unique())
            return []

        except Exception:
            logger.error('get addresses', exc_info=True)


    def determine_churn(self, current_addresses,end_date):
        try:
            if self.df is None:
                self.df = self.load_df(start_date=self.initial_date,
                                       end_date=end_date,
                                       table=self.table,
                                       cols=self.churn_cols,
                                       storage_medium='clickhouse'
                                       )
                self.df = self.df.repartition(npartitions=30)

            if self.df is not None:
                if len(self.df) > 0:
                    # the addresses in the current block the sum to zero
                    # (transactions from the beginning, have churned
                    df = self.df[self.df['from_addr'].isin(current_addresses)]
                    df = df.groupby('from_addr')['value'].sum()
                    df = df.reset_index()
                    df = df[df.value == 0]
                    df = df[['from_addr']]
                    df = df.compute()
                    churned_addresses = df['from_addr'].unique().tolist()
                    return churned_addresses
            return []
        except Exception:
            logger.error('determine churn', exc_info=True)


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
            self.new_activity_lst = []
            for table in self.construction_cols_dict.keys():
                logger.warning('CONSTRUCTION STARTED for: %s',table)
                cols = self.construction_cols_dict[table]['cols']
                # load production data from staging
                df = self.load_df(start_date,end_date,cols,table,'mysql')

                if df is not None:
                    if len(df)>0:
                        # convert to plus minus transactions
                        # get addresses in current block
                        self.address_lst = self.get_addresses(start_date,end_date)
                        if self.address_lst is not None:
                            #logger.warning("%s LOADED, WINDOW:- %s", table.upper(), len(df))
                            churned_addresses = self.determine_churn(self.address_lst,end_date)
                            '''
                            self.new_activity_lst = df.apply(create_address_transaction, axis=1,
                                                             args=(table, churned_addresses,self.address_lst,
                                                                   self.new_activity_lst,),
                                                              meta=('address_lst', 'O'))
                            '''
                            self.new_activity_lst = df.map_partitions(calling_create_address_transaction,
                                                                      table, churned_addresses, self.address_lst,
                                                                      self.new_activity_lst,
                                                                      meta=('new_activity_list', 'O'))
                        del df
                        gc.collect()

            # save data
            if len(self.new_activity_lst) > 0:
                logger.warning('%s',self.new_activity_lst)
                df = pd.DataFrame(self.new_activity_lst)
                # save dataframe
                df = dd.from_pandas(df, npartitions=10)
                logger.warning("INSIDE SAVE DF:%s", df.columns.tolist())

                self.save_df(df)
                self.df_size_lst.append(len(df))
                self.window_adjuster() # adjust size of window to load bigger dataframes

                self.address_lst += self.new_activity_lst
                self.new_activity_lst = [] #reset for next window
                # logger.warning('FINISHED %s',table.upper())

                # append newly made df to existing self.df
                temp_df = df[self.churn_cols]
                if self.df is not None:
                    # make equal partitions to use concat
                    self.df = self.df.repartition(npartitions=50)
                    self.df = self.df.reset_index(drop=True)
                    temp_df = temp_df.repartition(npartitions=50)
                    temp_df = temp_df.reset_index(drop=True)
                    self.df = dd.concat([self.df,temp_df],axis=0,interleave_partitions=True)
                    self.df = self.df.drop_duplicates() # deduplicate
                    logger.warning('df length-%s:timestamp after append:%s',len(self.df),
                                   self.df['block_timestamp'].tail(5))
                else:
                    self.df = temp_df
                del temp_df
                gc.collect()

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