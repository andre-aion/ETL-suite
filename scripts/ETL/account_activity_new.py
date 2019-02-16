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
initial_date = datetime.strptime("2018-04-25 10:00:00", "%Y-%m-%d %H:%M:%S")
start_date = my.date_to_int(initial_date)
end_date = my.date_to_int(datetime.now())
if len(contract_addresses) <= 0:
    qry = """SELECT contract_addr FROM aion.contract WHERE deploy_timestamp >= {} AND 
                  deploy_timestamp <= {} ORDER BY deploy_timestamp""".format(start_date, end_date)
    df = pd.read_sql(qry, my.connection)
    logger.warning("line 39: contract addresses loaded from mysql")
    if len(df) > 0:
        contract_addresses = df['contract_addr'].tolist()
        del df
        gc.collect()

all_df = None
global all_df


def get_account_type(address,my):
    try:
        # check contracts
        if address in contract_addresses:
            return 'contract'
        else:
            qry = """select address,transaction_hash from aion.account 
                                    where address = '{}' """.format(address)
            df = pd.read_sql(qry, my.connection)
            if df is not None:
                if len(df) > 0:
                    transaction_hash = df['transaction_hash'].unique().tolist()
                    del df
                    gc.collect()
                    #logger.warning('transaction_hash searching for miner %s:%s',address,transaction_hash)
                    if transaction_hash[0] == '':
                        # logger.warning("MINER FOUND:%s",transaction_hash)
                        return 'miner'
                    else:
                        # logger.warning("AIONNER FOUND:%s",transaction_hash)
                        return 'aionner'

        return 'aionner'
    except Exception:
        logger.error('get_account_type:', exc_info=True)

def create_address_transaction(row,table,address_lst,
                               new_activity_lst,my):
    try:
        global all_df
        all_df = extend_self_df(row)
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

            # DETERMINE IF NEW ADDRESS
            #logger.warning('self address list:%s',self.existing_addresses)
            if row['from_addr'] in address_lst:
                from_activity = determine_churn(row['from_addr'])
            elif row['from_addr'] not in address_lst:
                from_activity = 'joined'
                address_lst.append(row['from_addr'])
            if row['to_addr'] in address_lst:
                to_activity = determine_churn(row['to_addr'])
            elif row['to_addr'] not in address_lst:
                to_activity = 'joined'
                address_lst.append(row['to_addr'])

            account_type_from = get_account_type(row['from_addr'],my)
            account_type_to = get_account_type(row['to_addr'],my)
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

def calling_create_address_transaction(df,table,address_lst,
                                       new_activity_lst):
    try:
        my = PythonMysql('aion')
        tmp_lst = df.apply(create_address_transaction, axis=1,
                                    args=(table, address_lst,
                                    new_activity_lst,my))

        new_activity_lst += tmp_lst
        my.conn.close()
        my.connection.close()
        return new_activity_lst
    except Exception:
        logger.error('calling create address ....:', exc_info=True)

def extend_self_df(row):
    try:
        # concatenate to self.df
        global all_df
        if all_df is not None:
            all_df = all_df.repartition(npartitions=10)
            all_df = all_df.reset_index(drop=True)
            temp_dict = [
                {
                    'address': row['to_addr'],
                    'value': row['value']
                },
                {
                    'address': row['from_addr'],
                    'value':row['value']*-1
                }]
            df_temp = pd.DataFrame(temp_dict)
            df_temp = dd.from_pandas(df_temp,npartitions=10)
            df_temp = df_temp.reset_index(drop=True)
            all_df = dd.concat([all_df, df_temp], axis=0, interleave_partitions=True)

    except Exception:
        logger.error('extend self.df', exc_info=True)

def determine_churn(current_addresses):
    try:
        global all_df
        if all_df is not None:
            if len(all_df) > 0:
                # the addresses in the current block the sum to zero
                # (transactions from the beginning, have churned
                logger.warning('line 267 length of current addresses in period:%s',len(current_addresses))
                df = all_df[all_df['address'].isin(current_addresses)]
                df = df.groupby('address')['value'].sum()
                df = df.reset_index()
                df = df[df.value == 0]
                df = df[['address']]
                df = df.compute()
                churned_addresses = df['address'].unique().tolist()
                if len(churned_addresses)>0:
                    logger.warning('line 276: length churned addresses: %s', len(churned_addresses))
                    return 'churned'
                return 'active'
        return 'active'
    except Exception:
        logger.error('determine churn', exc_info=True)

class AccountActivity(Checkpoint):
    def __init__(self, table):
        Checkpoint.__init__(self, table)
        self.cl = PythonClickhouse('aion')
        self.my = PythonMysql('aion')
        self.redis = PythonRedis()

        self.cl = PythonClickhouse('aion')
        self.redis = PythonRedis()
        self.window = 2  # hours
        self.DATEFORMAT = "%Y-%m-%d %H:%M:%S"
        self.is_up_to_date_window = self.window + 2  # hours to sleep to give reorg time to happen
        self.table = table
        self.table_dict = table_dict[table]

        self.df = None
        self.df_history = None
        self.churn_cols = ['address', 'value']

        self.initial_date = datetime.strptime("2018-04-25 10:00:00",self.DATEFORMAT)
        # manage size of warehouse
        self.df_size_lst = []
        self.df_size_threshold = {
            'upper': 1000,
            'lower': 500
        }

        self.columns = sorted(list(table_dict[table].keys()))
        # lst to make new df for account balance
        self.new_activity_lst = []
        self.existing_addresses = [] # all addresses ever on network
        self.current_addresses = []
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
    def set_all_previous_addresses(self,start_date,end_date):
        try:
            if self.existing_addresses is None:
                if len(self.existing_addresses) <= 0:
                    # get addresses from start of time til end of block under consideration
                    df = self.load_df(self.initial_date,end_date,self.churn_cols,
                                           self.table,'clickhouse')
                    df = df.repartition(npartitions=100)
                    logger.warning('line 234: self.df loaded from clickhouse:%s',len(df))

                    self.before = self.before[self.before.block_timestamp > start_date]
                    self.before = self.before[['address']]
                    self.before = self.before.compute()
                    self.existing_addresses = self.before['address'].unique().tolist()
                    if self.existing_addresses is None:
                        self.existing_addresses = []
                    del df

            self.existing_addresses = self.existing_addresses + self.current_addresses
            #logger.warning('line 253: length of existing addresses:%s', len(self.existing_addresses))
        except Exception:
            logger.error('get addresses', exc_info=True)



    # get current addresses and add new transactions to self.df
    def set_current_addresses(self,df):
        try:
            # make df
            df = df[['value', 'from_addr','to_addr']]
            df = df.compute()
            # add to current address
            self.current_addresses += list(df['from_addr'].unique())
            self.current_addresses += list(df['to_addr'].unique())
            self.current_addresses = list(set(self.current_addresses))


        except Exception:
            logger.error('set current addresses', exc_info=True)

    async def update(self):
        try:
            global all_df
            # SETUP
            offset = self.get_offset()
            if isinstance(offset, str):
                offset = datetime.strptime(offset, self.DATEFORMAT)
            start_date = offset
            end_date = start_date + timedelta(hours=self.window)
            if all_df is None:
                cl = PythonClickhouse('aion')
                all_df = cl.load_data(start_date=initial_date,
                                      end_date=end_date,
                                      table='account_activity',
                                      cols=['address', 'value'],
                                      )
                all_df = all_df.repartition(npartitions=30)
                logger.warning("all_df loaded:%s",len(all_df))
            self.update_checkpoint_dict(end_date)
            # get data
            logger.warning('LOAD RANGE %s:%s',start_date,end_date)
            self.new_activity_lst = []
            self.current_addresses = []
            for table in self.construction_cols_dict.keys():
                logger.warning('CONSTRUCTION STARTED for: %s',table)
                cols = self.construction_cols_dict[table]['cols']
                # load production data from staging
                df = self.load_df(start_date,end_date,cols,table,'mysql')

                if df is not None:
                    if len(df)>0:
                        # convert to plus minus transactions
                        # get addresses in current block
                        self.set_all_previous_addresses(start_date,end_date) # update address list, set addresses
                        if self.existing_addresses is not None:
                            #logger.warning("%s LOADED, WINDOW:- %s", table.upper(), len(df))
                            '''
                            self.new_activity_lst = df.apply(create_address_transaction, axis=1,
                                                             args=(table, churned_addresses,self.existing_addresses,
                                                                   self.new_activity_lst,),
                                                              meta=('address_lst', 'O'))
                            '''
                            df = df.repartition(npartitions=5)
                            self.new_activity_lst = df.map_partitions(calling_create_address_transaction,
                                                                      table, self.existing_addresses,
                                                                      self.new_activity_lst,
                                                                      meta=(None, 'O')).compute(get=get)

                        del df
                        gc.collect()

            # save data
            #self.new_activity_lst = self.new_activity_lst.compute()
            lst = []
            for item in self.new_activity_lst:
                lst.append(item[0])

            if len(lst) > 0:
                #logger.warning('line 336: length of new activity list:%s',len(lst))
                self.new_activity_lst = lst
                df = pd.DataFrame(self.new_activity_lst)
                # save dataframe
                df = dd.from_pandas(df, npartitions=100)

                self.save_df(df)
                self.df_size_lst.append(len(df))
                self.window_adjuster() # adjust size of window to load bigger dataframes

                self.existing_addresses += self.new_activity_lst
                self.new_activity_lst = [] #reset for next window


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