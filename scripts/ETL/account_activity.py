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
global contract_addresses
contract_addresses = []
my = PythonMysql('aion')
DATEFORMAT = "%Y-%m-%d %H:%M:%S"
initial_date = datetime.strptime("2018-04-25 10:00:00", "%Y-%m-%d %H:%M:%S")
start_date = my.date_to_int(initial_date)
end_date = my.date_to_int(datetime.now())

global all_df
all_df = None

# to detect churned accounts
global token_holders_churned_df
token_holders_churned_df = None
global account_churned_df
account_churned_df = None

global account_churned_addresses
account_churned_addresses = None
global token_holders_churned_addresses
token_holders_churned_addresses = None

if len(contract_addresses) <= 0:
    qry = """SELECT contract_addr FROM aion.contract WHERE deploy_timestamp >= {} AND 
                  deploy_timestamp <= {} ORDER BY deploy_timestamp""".format(start_date, end_date)
    df = pd.read_sql(qry, my.connection)
    logger.warning("line 39: contract addresses loaded from mysql")
    if len(df) > 0:
        contract_addresses = list(df['contract_addr'].unique())
        del df
        gc.collect()

def get_account_type(address,my):
    try:
        # check contracts
        global contract_addresses
        if address in contract_addresses:
            logger.warning("IDENTIFIED A CONTRACT")
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
                               new_activity_lst,churned_addresses,my):
    try:
        #logger.warning('len all_df %s:',len(all_df))
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
            from_activity = 'joined'
            if row['from_addr'] in churned_addresses:
                if row['from_addr'] == row['to_addr']:
                    from_activity = 'self-to-self transfer'
                else:
                    logger.warning('CHURN LABEL APPLIED')
                    logger.warning('from addr = %s', row['from_addr'])
                    from_activity = 'churned'
            else:
                if row['from_addr'] in address_lst:
                    from_activity = 'active'

            to_activity = 'joined'
            if row['to_addr'] in churned_addresses:
                if row['from_addr'] == row['to_addr']:
                    to_activity = 'self-to-self transfer'
                else:
                    logger.warning('CHURN LABEL APPLIED')
                    logger.warning('to addr = %s', row['to_addr'])
                    to_activity = 'churned'
            else:
                if row['to_addr'] in address_lst:
                    to_activity = 'active'

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
            #logger.warning('temp lsit:%s',temp_lst)
            # for each to_addr
            new_activity_lst = new_activity_lst+temp_lst
            return new_activity_lst

    except Exception:
        logger.error('create address transaction:',exc_info=True)

def calling_create_address_transaction(df,table,address_lst,
                                       new_activity_lst,churned_addresses):
    try:
        my = PythonMysql('aion')
        tmp_lst = df.apply(create_address_transaction, axis=1,
                                    args=(table, address_lst,
                                    new_activity_lst,churned_addresses,my))

        new_activity_lst = new_activity_lst + tmp_lst
        my.conn.close()
        my.connection.close()
        return new_activity_lst
    except Exception:
        logger.error('calling create address ....:', exc_info=True)

def manage_all_df(end_date=None):
    try:
        # concatenate to self.df
        global all_df

        if all_df is None:
            cl = PythonClickhouse('aion')
            all_df = cl.load_data(start_date=initial_date,
                                  end_date=end_date,
                                  table='account_activity',
                                  cols=['address', 'value'],
                                  )
            all_df = all_df.repartition(npartitions=10)
            logger.warning("all_df loaded:%s", len(all_df))
    except Exception:
        logger.error('manage all df', exc_info=True)

def set_churned_df_addresses(block_numbers):
    try:
        global account_churned_df
        global token_holders_churned_df
        global account_churned_addresses
        global token_holders_churned_addresses
        account_churned_addresses = []
        token_holders_churned_addresses = []
        if len(account_churned_df) > 0:
            temp = account_churned_df[account_churned_df.block_number.isin(block_numbers)]
            if len(temp) > 0:
                account_churned_addresses = list(temp['address'].unique())
        if len(token_holders_churned_df) > 0:
            temp = token_holders_churned_df[
                token_holders_churned_df.block_number.isin(block_numbers)]
            if len(temp) > 0:
                token_holders_churned_addresses = list(temp['address'].unique())


    except Exception:
        logger.error('get churned df addresses', exc_info=True)


def load_churned_df(offset):
    try:

        global account_churned_df
        global token_holders_churned_df
        my = PythonMysql('aion')

        # get the block number nearest to the offset
        offset = my.date_to_int(offset)
        qry = """
            select min(block_number) as block_number from block 
            where block_timestamp >= {}
        """.format(offset)
        df = pd.read_sql(qry, my.connection)
        if len(df) > 0:
            block_number = df['block_number'].min()
        else:
            block_number = 0

        logger.warning('BLOCK NUMBER:%s',block_number)
        if account_churned_df is None:
            qry = """
                select address,last_block_number as block_number from account  
                where balance = 0 and last_block_number >= {}
            """.format(block_number)
            account_churned_df = pd.read_sql(qry, my.connection)
            logger.warning("length of account churned:%s",len(account_churned_df))

        if token_holders_churned_df is None:
            qry = """
                select contract_addr as address, block_number from token_holders 
                where scaled_balance = 0 and block_number >= {}
            """.format(block_number)
            token_holders_churned_df = pd.read_sql(qry, my.connection)
            logger.warning("length of token holders churned:%s",len(token_holders_churned_df))

    except Exception:
        logger.error('manage churned df', exc_info=True)

def determine_churn(current_addresses):
    try:
        global all_df
        global account_churned_addresses
        global token_holders_churned_addresses
        # the addresses in the current block the sum to zero
        # (transactions from the beginning, have churned
        df = all_df[all_df['address'].isin(current_addresses)]
        if len(df) > 0 :
            #logger.warning('inside determine churn, length of filtered df:%s',len(df))
            df = df.groupby('address')['value'].sum()
            #logger.warning('inside df:%s',df.head(40))

            df = df.reset_index()
            df = df[(df.value == 0) | (df.address.isin(account_churned_addresses)) |
                    (df.address.isin(token_holders_churned_addresses))]
            if len(df) > 0:
                df = df[['address']]
                df = df.compute()
                churned_addresses = df['address'].unique().tolist()
                logger.warning("number of churned addresses:%s",len(churned_addresses))
                logger.warning('churned addresses = %s', churned_addresses)
                return churned_addresses
        logger.warning("number of churned accounts is 0")

        return []
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
        self.window = 4  # hours
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
        self.all_df = None

    """
        - get addresses from start til now
        - filter for current addresses and extant addresses
    """
    def set_all_previous_addresses(self):
        try:
            #logger.warning('existing addresses:%s', self.existing_addresses)
            #logger.warning('current addresses:%s', self.current_addresses)
            self.existing_addresses = list(set(self.existing_addresses + self.current_addresses))
            logger.warning('length of current addresses:%s', len(self.current_addresses))
        except Exception:
            logger.error('get addresses', exc_info=True)


    def add_to_all_df(self,df,npartitions=10):
        global all_df
        logger.warning('Length of all_df %s', len(all_df))

        try:
            if df is not None:
                if len(df) > 0:
                    temp = df[['from_addr', 'to_addr', 'value']]
                    temp1 = temp.copy()
                    temp1 = temp1.rename(columns={'from_addr': 'address'})
                    temp1 = temp1[['address', 'value']]
                    temp1.value = temp1.value * -1
                    temp1 = temp1.repartition(npartitions=npartitions)
                    temp1 = temp1.reset_index(drop=True)

                    # prep to_addr
                    temp2 = df.copy()
                    temp2 = temp2.rename(columns={'to_addr': 'address'})
                    temp2 = temp2[['address', 'value']]
                    temp2 = temp2.repartition(npartitions=npartitions)
                    temp2 = temp2.reset_index(drop=True)

                    # concat
                    all_df = all_df.repartition(npartitions=npartitions)
                    all_df = all_df.reset_index(drop=True)
                    all_df = all_df.repartition(npartitions=npartitions)
                    all_df = all_df.reset_index(drop=True)
                    #logger.warning('length all_df before concat:%s',len(all_df))
                    all_df = dd.concat([all_df, temp1, temp2], axis=0, interleave_partitions=True)
                    #logger.warning('length all_df after concat:%s',len(all_df))

            #logger.warning('all_df: %s',all_df.head(10))
        except Exception:
            logger.error('add to all_df',exc_info=True)

    # get current addresses and add new transactions to self.df
    def set_current_addresses(self,df1):
        try:
            # make df
            df1 = df1.compute()
            lst1 = list(df1['from_addr'].unique())
            lst2 = list(df1['to_addr'].unique())
            self.current_addresses = list(set(lst1+lst2+self.current_addresses))
            #logger.warning('length of current addresses:%s',len(self.current_addresses))
        except Exception:
            logger.error('set current addresses', exc_info=True)

    async def update(self):
        try:
            # SETUP
            global all_df
            global account_churned_df

            offset = self.get_offset()
            if isinstance(offset, str):
                offset = datetime.strptime(offset, self.DATEFORMAT)
            start_date = offset
            end_date = start_date + timedelta(hours=self.window)

            # initialize if restarted/reset
            if all_df is None:
                manage_all_df(end_date=start_date)

            if account_churned_df is None:
                load_churned_df(offset)

            if len(self.existing_addresses) == 0:
                temp_df = all_df[['address']]
                temp_df = temp_df.compute()
                self.existing_addresses = list(set(list(temp_df['address'].unique())))
                del temp_df
                gc.collect()

            self.update_checkpoint_dict(end_date)
            # get data
            logger.warning('LOAD RANGE %s:%s',start_date,end_date)
            self.new_activity_lst = []
            self.current_addresses = []
            df = None
            for table in self.construction_cols_dict.keys():
                logger.warning('CONSTRUCTION STARTED for: %s',table)
                cols = self.construction_cols_dict[table]['cols']
                # load production data from staging

                if df is not None:
                    logger.warning('length of df: %s', len(df))
                    if len(df)>0:
                        temp = self.load_df(start_date, end_date, cols, table, 'mysql')
                        if len(temp) > 0:

                            df = df.repartition(npartitions=10)
                            df = df.reset_index(drop=True)

                            logger.warning('temp:%s',temp.head(5))
                            temp = temp.repartition(npartitions=10)
                            temp = temp.reset_index(drop=True)
                            df = dd.concat([df,temp],axis=0,interleave_partitions=True)
                            logger.warning('length of df after concatenation: %s', len(df))

                    else:
                        df = self.load_df(start_date, end_date, cols, table, 'mysql')
                else:
                    df = self.load_df(start_date, end_date, cols, table, 'mysql')

            if df is not None:
                if len(df) > 0:
                    self.set_current_addresses(df)

                    # add new data to all_df
                    self.add_to_all_df(df)

                    # determine the addresses churned according to balance tables
                    # get list of block numbers
                    temp = df[['block_number']]
                    temp = temp.compute()
                    block_numbers = list(temp['block_number'].unique())
                    set_churned_df_addresses(block_numbers)
                    # determine churn
                    churned_addresses = determine_churn(self.current_addresses)
                    # prep from_addr
                    # determine joined, churned, contracts, etc.
                    logger.warning('# of existing addresses:%s',len(self.existing_addresses))
                    self.new_activity_lst = df.map_partitions(calling_create_address_transaction,
                                                              table, self.existing_addresses,
                                                              self.new_activity_lst,churned_addresses,
                                                              meta=(None, 'O')).compute()

                    #logger.warning('Length of all_df %s',len(all_df))
                    # update all addresses list
                    self.set_all_previous_addresses()  # update address list, set addresses

                    # save data
                    lst = []
                    for item in self.new_activity_lst:
                        lst.append(item[0])
                        lst.append(item[1])


                    if len(lst) > 0:
                        #logger.warning('line 336: length of new activity list:%s',len(lst))
                        self.new_activity_lst = lst
                        df = pd.DataFrame(self.new_activity_lst)
                        # save dataframe
                        df = dd.from_pandas(df, npartitions=10)

                        self.save_df(df)
                        self.df_size_lst.append(len(df))
                        self.window_adjuster() # adjust size of window to load bigger dataframes

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