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


# to detect churned accounts
global token_holders_churned_df
token_holders_churned_df = None
global account_churned_df
account_churned_df = None

global account_churned_addresses
account_churned_addresses = []
global account_joined_addresses
account_joined_addresses = []
global churned_block_numbers
churned_block_numbers = []


global account_joined_df
global token_holders_joined_df
global joined_block_numbers
account_joined_df = None
token_holders_joined_df = None
joined_block_numbers = []


def load_contract_addresses(start_date, end_date):
    try:
        global contract_addresses
        my = PythonMysql('aion')
        if len(contract_addresses) <= 0:
            qry = """SELECT contract_addr FROM aion.contract WHERE deploy_timestamp >= {} AND 
                          deploy_timestamp <= {} ORDER BY deploy_timestamp"""\
                .format(my.date_to_int(start_date), my.date_to_int(end_date))
            df = pd.read_sql(qry, my.connection)
            logger.warning("line 39: contract addresses loaded from mysql")
            if len(df) > 0:
                contract_addresses = list(df['contract_addr'].unique())
                del df
                gc.collect()
    except:
        logger.warning('load contract addresses:%s',exc_info=True)


def load_churned_df(start_date):
    try:

        global account_churned_df
        global token_holders_churned_df
        my = PythonMysql('aion')

        if account_churned_df is None:
            qry = """
                select address,
                timestamp_of_last_event as block_timestamp,
                block_number_of_last_event as last_block_number
                from account  
                where balance = 0 and timestamp_of_last_event >= {}
                order by timestamp_of_last_event

            """.format(my.date_to_int(start_date))
            account_churned_df = pd.read_sql(qry, my.connection)
            logger.warning("length of account churned:%s", len(account_churned_df))

        if token_holders_churned_df is None:
            qry = """
                select contract_addr as address, timestamp_of_last_event as block_timestamp,
                block_number_of_last_event as last_block_number
                from token_holders 
                where scaled_balance = 0  and timestamp_of_last_event >= {}
                order by timestamp_of_last_event
            """.format(my.date_to_int(start_date))
            token_holders_churned_df = pd.read_sql(qry, my.connection)
            logger.warning("length of token holders churned:%s", len(token_holders_churned_df))

    except Exception:
        logger.error('manage churned df', exc_info=True)

def load_joined_df(start_date):
    try:

        global account_joined_df
        global token_holders_joined_df
        my = PythonMysql('aion')

        if account_joined_df is None:
            qry = """
                select address,timestamp_of_first_event as block_timestamp,           
                block_number_of_first_event as first_block_number
                from account where timestamp_of_first_event >= {}
                order by timestamp_of_first_event  
            """.format(my.date_to_int(start_date))
            account_joined_df = pd.read_sql(qry, my.connection)
            logger.warning("length of account joined:%s", len(account_joined_df))

        if token_holders_joined_df is None:
            qry = """
                select contract_addr as address, timestamp_of_first_event as block_timestamp,  
                block_number_of_first_event as first_block_number
                from token_holders where timestamp_of_first_event >= {}
                order by timestamp_of_first_event
            """.format(my.date_to_int(start_date))
            token_holders_joined_df = pd.read_sql(qry, my.connection)
            logger.warning("length of token holders joined:%s", len(token_holders_joined_df))
    except Exception:
        logger.error('manage churned df', exc_info=True)

def get_account_type(address):
    try:
        # check contracts
        global contract_addresses

        if address in contract_addresses:
            logger.warning("IDENTIFIED A CONTRACT")
            return 'contract'
        else:
            my = PythonMysql('aion')
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

def create_address_transaction(row, address_lst,
                               new_activity_lst):
    global account_churned_addresses
    global account_joined_addresses
    global churned_block_numbers
    global joined_block_numbers
    if row['block_number'] in churned_block_numbers:
        logger.warning("churned block numbers:%s=%s",churned_block_numbers,row['block_number'])
        logger.warning("BLOCK MATCHED FOR  CHURN")
    '''
    if row['block_number'] in joined_block_numbers:
        logger.warning("joined block numbers:%s<>%s", joined_block_numbers, row['block_number'])
        logger.warning("BLOCK MATCHED FOR JOIN")
    '''
    try:
        #logger.warning('len all_df %s:',len(all_df))
        if row is not None:
            block_timestamp = row['block_timestamp']
            if isinstance(row['block_timestamp'],str):
                block_timestamp = datetime.strptime(block_timestamp,DATEFORMAT)
            if isinstance(row['block_timestamp'],int):
                block_timestamp = datetime.fromtimestamp(row['block_timestamp'])

            event = 'transfer'
            if row['from_addr'] == row['to_addr']:
                #logger.warning('%s:%s', row['from_addr'], row['to_addr'])
                event = 'self-to-self transfer'

            # DETERMINE IF NEW ADDRESS
            #if len(churned_addresses) > 0 :
                #logger.warning('%s:%s',row['from_addr'],row['to_addr'])
            from_activity = 'active'
            if row['from_addr'] in account_joined_addresses:
                if row['block_number'] in joined_block_numbers:
                    #logger.warning('JOIN LABEL APPLIED(FROM)')
                    from_activity = 'joined'
            if row['from_addr'] in account_churned_addresses:
                # ensure that the churned transaction is only changed in the correct block
                from_activity = 'active'
                if row['block_number'] in churned_block_numbers:
                    if row['from_addr'] != row['to_addr']:
                        logger.warning('CHURN LABEL APPLIED(FROM)')
                        # logger.warning('from addr = %s', row['from_addr'])
                        from_activity = 'churned'
                    else:
                        logger.warning('CHURN TRANSFER SELF TO SELF')
                        logger.warning('%s:%s', row['from_addr'], row['to_addr'])

            to_activity = 'active'
            if row['to_addr'] in account_joined_addresses:
                if row['block_number'] in joined_block_numbers:
                    #logger.warning('JOIN LABEL APPLIED(TO)')
                    to_activity = 'joined'

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
            #logger.warning('temp lsit:%s',temp_lst)
            # for each to_addr
            new_activity_lst = new_activity_lst+temp_lst
            del temp_lst
            gc.collect()
            return new_activity_lst

    except Exception:
        logger.error('create address transaction:',exc_info=True)

def calling_create_address_transaction(df,address_lst,
                                       new_activity_lst):
    try:
        tmp_lst = df.apply(create_address_transaction, axis=1,
                                    args=(address_lst,
                                          new_activity_lst))

        new_activity_lst = new_activity_lst + tmp_lst
        #my.conn.close()
        #my.connection.close()
        del tmp_lst
        gc.collect()
        return new_activity_lst
    except Exception:
        logger.error('calling create address ....:', exc_info=True)


def set_churned_df_addresses(start_date, end_date):
    try:
        global account_churned_df
        global token_holders_churned_df
        global account_churned_addresses
        global churned_block_numbers

        my = PythonMysql('aion')

        account_churned_addresses = []
        churned_block_numbers = []

        if len(account_churned_df) > 0:
            # filter/truncate dfs
            account_churned_df = account_churned_df[account_churned_df.block_timestamp >= my.date_to_int(start_date)]
            temp = account_churned_df[account_churned_df.block_timestamp <= my.date_to_int(end_date)]
            if len(temp) > 0:
                account_churned_addresses = list(temp['address'].unique())
                churned_block_numbers = list(temp['last_block_number'].unique())

        if len(token_holders_churned_df) > 0:
            token_holders_churned_df = token_holders_churned_df[ token_holders_churned_df.block_timestamp >= start_date]
            temp = token_holders_churned_df[token_holders_churned_df <= my.date_to_int(end_date)]
            if len(temp) > 0:
                account_churned_addresses += list(temp['address'].unique())
                churned_block_numbers += list(temp['last_block_number'].unique())
        #logger.warning("# CHURNED ADDRESSES:%s",len(account_churned_addresses))
        logger.warning("CHURNED ADDRESSES: %s",account_churned_addresses)
        logger.warning("CHURNED BLOCK NUMBERS:%s",churned_block_numbers)

    except Exception:
        logger.error('get churned df addresses', exc_info=True)

def set_joined_df_addresses(start_date, end_date):
    try:
        global account_joined_df
        global token_holders_joined_df
        global account_joined_addresses
        global joined_block_numbers

        my = PythonMysql('aion')

        account_joined_addresses = []
        joined_block_numbers = []

        if len(account_joined_df) > 0:
            # filter/truncate dfs
            account_joined_df = account_joined_df[account_joined_df.block_timestamp >= my.date_to_int(start_date)]
            temp = account_joined_df[account_joined_df.block_timestamp <= my.date_to_int(end_date)]
            if len(temp) > 0:
                account_joined_addresses = list(temp['address'].unique())
                joined_block_numbers = list(temp['first_block_number'].unique())

        if len(token_holders_joined_df) > 0:
            token_holders_joined_df = token_holders_joined_df[ token_holders_joined_df.block_timestamp >= start_date]
            temp = token_holders_joined_df[token_holders_joined_df <= my.date_to_int(end_date)]
            if len(temp) > 0:
                account_joined_addresses += list(temp['address'].unique())
                joined_block_numbers += list(temp['first_block_number'].unique())
        #logger.warning("JOINED ADDRESSES:%s",len(account_joined_addresses))
        logger.warning("JOINED BLOCK NUMBERS:%s",joined_block_numbers)


    except Exception:
        logger.error('get joined df addresses', exc_info=True)

def determine_activity(current_addresses):
    try:
        global account_churned_addresses
        global account_joined_addresses
        # (transactions from the beginning, have churned
        if len(current_addresses) > 0 :
            if len(account_churned_addresses) > 0:
                #logger.warning('inside df:%s',df.head(40))
                tmp = list(set(account_churned_addresses) & set(current_addresses))
                account_churned_addresses = []
                if len(tmp) > 0:
                    account_churned_addresses = tmp

                #logger.warning("number of churned addresses:%s", len(account_churned_addresses))
                #logger.warning('churned addresses = %s', account_churned_addresses)
            else:
                logger.warning("number of churned accounts is 0")

            if len(account_joined_addresses) > 0:
                #logger.warning('inside df:%s',df.head(40))
                tmp = list(set(account_joined_addresses) & set(current_addresses))
                account_joined_addresses = []
                if len(tmp) > 0:
                    account_joined_addresses = tmp

                #logger.warning("number of joined addresses:%s", len(account_joined_addresses))
                #logger.warning('joined addresses = %s', account_joined_addresses)
            else:
                logger.warning("number of joined accounts is 0")
        else:
            logger.warning("number of churned accounts is 0")
            logger.warning("number of joined accounts is 0")


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
            'upper': 5000,
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

    def load_existing_addresses(self,end_date):
        try:
            if len(self.existing_addresses) <= 0:
                cl = PythonClickhouse('aion')
                temp_df = cl.load_data(start_date=initial_date,
                                       end_date=end_date,
                                       table='account_activity',
                                       cols=['address'],
                                       )
                temp_df = temp_df.compute()
                self.existing_addresses = list(set(list(temp_df['address'].unique())))
                del temp_df
                gc.collect()
        except Exception:
            logger.error('load existing addresses', exc_info=True)

    def set_all_previous_addresses(self):
        try:

            self.existing_addresses = list(set(self.existing_addresses + self.current_addresses))
            logger.warning('length of current addresses:%s', len(self.current_addresses))
        except Exception:
            logger.error('add to existing addresses', exc_info=True)

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
            global contract_addresses
            global account_churned_addresses
            global account_joined_addresses
            global churned_block_numbers
            global joined_block_numbers

            offset = self.get_offset()
            if isinstance(offset, str):
                offset = datetime.strptime(offset, self.DATEFORMAT)
            start_date = offset
            end_date = start_date + timedelta(hours=self.window)

            # load various data
            if account_churned_df is None:
                load_churned_df(start_date)
                load_joined_df(start_date)
            if len(self.existing_addresses) <= 0:
                self.load_existing_addresses(start_date)
            if len(contract_addresses) <= 0:
                load_contract_addresses(start_date,end_date)

            self.update_checkpoint_dict(end_date)
            # get data
            logger.warning('LOAD RANGE %s:%s',start_date,end_date)
            self.new_activity_lst = []

            self.current_addresses = []
            df = None
            for table in self.construction_cols_dict.keys():
                cols = self.construction_cols_dict[table]['cols']
                # load production data from staging

                if df is not None:
                    if len(df)>0:
                        temp = self.load_df(start_date, end_date, cols, table, 'mysql')
                        logger.warning('length of %s df: %s', table, len(temp))

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
                        logger.warning('length of %s df: %s', table, len(df))

                else:
                    df = self.load_df(start_date, end_date, cols, table, 'mysql')
                    logger.warning('length of %s df: %s',table, len(df))


            if df is not None:
                if len(df) > 0:
                    #logger.warning('length of df before split into multiple addresses: %s', len(df))

                    self.set_current_addresses(df)
                    # determine the addresses churned according to balance tables
                    # get list of block numbers
                    set_churned_df_addresses(start_date,end_date)
                    set_joined_df_addresses(start_date,end_date)

                    # determine churn
                    determine_activity(self.current_addresses)
                    # prep from_addr
                    # determine joined, churned, contracts, etc.
                    logger.warning('# of existing addresses:%s',len(self.existing_addresses))
                    self.new_activity_lst = df.map_partitions(calling_create_address_transaction,
                                                              self.existing_addresses,
                                                              self.new_activity_lst,
                                                              meta=(None, 'O')).compute()

                    #logger.warning('Length of all_df %s',len(all_df))
                    # update all addresses list
                    self.set_all_previous_addresses()  # update address list, set addresses

                    # save data
                    lst = []
                    if len(self.new_activity_lst) > 0:
                        for item in self.new_activity_lst:
                            lst.append(item[0])
                            lst.append(item[1])

                        if len(lst) > 0:
                            #logger.warning('line 336: length of new activity list:%s',len(lst))
                            self.new_activity_lst = lst
                            df = pd.DataFrame(self.new_activity_lst)

                            # save dataframe
                            df = dd.from_pandas(df, npartitions=10)
                            #logger.warning('length of df before save: %s', len(df))
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