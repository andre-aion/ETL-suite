import gc
from datetime import timedelta, datetime, date

import asyncio

from config.df_construct_config import table_dict
from scripts.ETL.checkpoint import Checkpoint
from scripts.utils.mylogger import mylogger
import pandas as pd
import dask.dataframe as dd

logger = mylogger(__file__)

class AccountActivityChurn(Checkpoint):
    def __init__(self, table):
        Checkpoint.__init__(self, table)
        self.table = table
        self.active_list = []
        self.churn_window = 7 # days
        # create tables and connection
        self.temp_lsts = {
            'new': [],
            'churned': [],
            'active': []
        }
        self.aggs = {
            'difficulty': 'mean',
            'block_nrg_consumed': 'mean',
            'nrg_limit': 'mean',
            'num_transactions': 'mean',
            'block_size': 'mean',
            'block_time': 'mean',
            'nrg_reward': 'mean',
            'value': 'mean',
            'transaction_nrg_consumed': 'mean',
            'nrg_price': 'mean',
        }
        self.df = None
        self.table_dict = table_dict[table]
        self.meta = self.table_dict
        # manage messages
        self.batch_counter = 0  # number of days before save
        self.batch_counter_threshold = 3
        self.batch_messages = []
        self.is_up_to_date_window = self.churn_window + 1
        self.columns = list(table_dict[table].keys())
        self.initial_date = '2018-05-01 00:00:00'
        self.checkpoint_column = 'block_timestamp'
        self.temp_lst = []
        self.window = 3 # hours

    def str_to_date(self, x):
        if isinstance(x, str):
            return datetime.striptime(x, self.DATEFORMAT)
        return x

    def cast_date(self, x):
        if isinstance(x,datetime):
            return x.date()
        return x

    def cast_cols(self, df):
        try:
            for column, type in self.meta.items():
                if column in list(self.aggs.keys()):  # restrict
                    if 'Float' in type:
                        values = {column: 0}
                        df = df.fillna(value=values)
                        df[column] = df[column].astype(float)
                    elif 'Int' in type:
                        values = {column: 0}
                        df = df.fillna(value=values)
                        df[column] = df[column].astype(int)
                    elif type == 'String':
                        values = {column: 'unknown'}
                        df = df.fillna(value=values)
                        df[column] = df[column].astype(str)
            return df
            # logger.warning('casted %s as %s',column,type)
        except Exception:
            logger.error('convert string', exc_info=True)

    """
        -load from -window to _window at start
        -after start drop last day, add one day

    """

    def manage_sliding_df(self, this_date):
        try:
            self.df = self.load_df(this_date)
            if self.df is not None:
                if len(self.df) > 0:
                    min, max = dd.compute(self.df['block_timestamp'].min(), self.df['block_timestamp'].max())
                    #logger.warning('AFTER LOAD, current df range %s:%s', min, max)

        except Exception:
            logger.error('', exc_info=True)

    def zero_out_datetime(self, x):
        logger.warning('line 129,x=%s', x)
        if isinstance(x, str):
            x = datetime.strptime(x, self.DATEFORMAT)
        x = datetime(year=x.year, month=x.month, day=x.day, hour=0, minute=0, second=0)

        return x

    def load_df(self, this_date, table='account_activity'):
        try:
            cols = ['address', 'value','block_timestamp']

            start_date = this_date - timedelta(days=self.churn_window)
            end_date = this_date + timedelta(days=self.churn_window + 1)

            start_date = datetime.combine(start_date, datetime.min.time())
            end_date = datetime.combine(end_date, datetime.min.time())
            logger.warning('BEFORE LOAD DATA %s:%s',start_date, end_date)

            df = self.cl.load_data(table=table, cols=cols,
                                   start_date=start_date, end_date=end_date)
            # logger.warning('line 157, load:%s',df['block_timestamp'])
            # convert to datetime to date
            # df['block_timestamp'] = df['block_timestamp'].map(self.zero_out_datetime)
            # df['block_timestamp'] = df['block_timestamp'].map(self.cast_date)

            return df
        except Exception:
            logger.warning('load_df', exc_info=True)


    def str_to_lst(self, string):
        self.temp_lists['active'] += string.split(',')

    def prep_columns(self, df, this_date):
        try:
            agg_col = "address"
            active_lst = self.get_daily_miners(df, this_date, agg_col)
            new_lst, retained_lst = self.get_new_retained(df, active_lst, this_date, agg_col)
            churned_lst = self.get_churned(df, active_lst, this_date, agg_col)
            lists = {
                'active_lst': active_lst,
                'new_lst': new_lst,
                'retained_lst': retained_lst,
                'churned_lst': churned_lst,
                'active_str': active_lst,
                'new_str': new_lst,
                'retained_str': retained_lst,
                'churned_str': churned_lst
            }
            # logger.warning("lists:%s",lists)
            # convert to string for saving
            for key in ['active_str', 'new_str', 'retained_str', 'churned_str']:
                lists[key] = ",".join(lists[key])
            return lists

        except Exception:
            logger.error('prepare for write', exc_info=True)

    """
        df is load from warehouse table covering 14 day span
        - for date x,
            - make list of active miner on that day
            - take 7 day window back grab from_addr/to_addr and make comprehensive list
            - record retained,new, mean daily difficulty, nrg-price etc
            - take 7 day window forward, grab from/to_addr and make comprehensive list
            - record churned
    """

    def get_daily_miners(self, df, this_date, col):
        try:
            lst = []
            if len(df) > 0:
                if isinstance(this_date, str):
                    this_date = datetime.strptime(this_date, self.DATEFORMAT)
                next_date = this_date + timedelta(days=1)
                #logger.warning('daily filter range=%s:%s', this_date, next_date)
                df = df[(df['block_timestamp'] >= this_date) &
                        (df['block_timestamp'] < next_date)]

                logger.warning('length of daily miners for %s:%s', this_date, len(df))
                df = df.compute()
                lst = df[col].unique()
            return lst
        except Exception:
            logger.error('get daily miners', exc_info=True)

    def get_new_retained(self, df, this_date_lst, this_date, tier_col='address'):
        try:
            # make bakwards window to find new miners
            if isinstance(this_date, str):
                this_date = datetime.strptime(this_date, self.DATEFORMAT)
            start_date = this_date - timedelta(days=self.churn_window)
            end_date = this_date
            #logger.warning('ASSESSMENT OF ACTIVE RANGE:')
            #logger.warning('%s this_date:%s', tier_col, this_date)
            #logger.warning('%s period %s:%s', tier_col, start_date, end_date)
            active_lst = []
            if len(df) > 0:
                df1 = df[(df.block_timestamp >= start_date) & (df.block_timestamp < end_date)]
                """
                    - make compound list of all miners for that period
                    - new: didn't appear
                """
                df1 = df1.compute()
                active_lst = df1[tier_col].unique().tolist()
            # logger.warning("tier_col(192):%s", tier_col)
            #logger.warning("%s backward active over window:%s", tier_col, len(active_lst))
            #logger.warning("%s this_date only list:%s", tier_col, len(this_date_lst))

            new_lst = list(set(active_lst).difference(this_date_lst))
            retained_lst = list(set(active_lst).intersection(this_date_lst))
            #logger.warning("%s new lst (226):%s", tier_col, len(new_lst))
            #logger.warning("%s retained lst(227):%s", tier_col, len(retained_lst))

            return new_lst, retained_lst
        except Exception:
            logger.error('', exc_info=True)

    def get_churned(self, df, this_date_lst, this_date, tier_col='address'):
        try:
            # make bakwards window to find new miners
            start_date = this_date + timedelta(days=1)
            end_date = this_date + timedelta(days=self.churn_window + 1)
            churned_lst = []
            if len(df) > 0:
                df1 = df[(df.block_timestamp >= start_date) & (df.block_timestamp <= end_date)]
                """
                    - make compound list of all miners for that period
                    - new: didn't appear
                """
                if len(df1) > 0:
                    df1 = df1.compute()
                    active_lst = df1[tier_col].unique().tolist()
                    #logger.warning('%s forward active list:%s', tier_col, len(active_lst))
                    churned_lst = list(set(this_date_lst).difference(active_lst))
            #logger.warning('%s churned list:%s', tier_col, len(churned_lst))
            return churned_lst
        except Exception:
            logger.error('', exc_info=True)


    def create_table_in_clickhouse(self):
        try:
            self.cl.create_table(self.table, self.table_dict, self.cols)
        except Exception:
            logger.error("Create self.table in clickhosue", exc_info=True)
            
    def get_warehouse_summary(self,this_date):
        # load daily summary from warehouse
        try:
            if isinstance(this_date,date):
                this_date = datetime.combine(this_date, datetime.min.time())
            start_date = this_date
            end_date = start_date + timedelta(days=1)
            cols = ["block_size", "block_time", "difficulty", "nrg_limit", "nrg_reward", "num_transactions",
                    "block_nrg_consumed", "transaction_nrg_consumed", "nrg_price"]
            # get data for that particular day
            df = self.cl.load_data('block_tx_warehouse',cols,start_date,end_date)
            summary = dict()
            summary["block_size"] = 0.0
            summary["block_time"] = 0.0
            summary["difficulty"] = 0.0
            summary["nrg_limit"] = 0.0
            summary["nrg_reward"] = 0.0
            summary["num_transactions"] = 0.0
            summary["block_nrg_consumed"] = 0.0
            summary["transaction_nrg_consumed"] = 0.0
            summary["nrg_price"] = 0.0
        
            if df is not None:
                if len(df) > 0:
                    df = df.compute()
                    summary["block_size"] = df.block_size.mean()
                    summary["block_time"] = df.block_time.mean()
                    summary["difficulty"] = df.difficulty.mean()
                    summary["nrg_limit"] = df.nrg_limit.mean()
                    summary["nrg_reward"] = df.nrg_reward.mean()
                    summary["num_transactions"] = df.num_transactions.mean()
                    summary["block_nrg_consumed"] = df.block_nrg_consumed.mean()
                    summary["transaction_nrg_consumed"] = df.transaction_nrg_consumed.mean()
                    summary["nrg_price"] = df.nrg_price.mean()
            del df
            gc.collect()
            return summary
            
        except Exception:
            logger.error("get warehouse summary", exc_info=True)


    def save_messages(self, this_date):
        try:
            # logger.warning("INSIDE SAVED MESSAGES, Last OFFSET:%s",this_date)
            # convert to pandas, then dask
            df = pd.DataFrame([x for x in self.batch_messages], columns=self.cols)
            #
            df = dd.from_pandas(df, npartitions=1)
            df = df.reset_index()
            df = df.drop('index', axis=1)
            # logger.warning('df before upsert called:%s',df['block_timestamp'].head())
            df['block_timestamp'] = df['block_timestamp'].map(self.cast_date)
            self.cl.upsert_df(df, cols=self.cols, table=self.table)

            # self.cl.insert(self.table,self.cols,messages=self.batch_messages)
            self.update_checkpoint_dict(datetime.strftime(this_date, self.DATEFORMAT))
            self.batch_messages = []
            self.batch_counter = 0
            self.save_checkpoint()
            logger.warning("messages inserted and offset saved, Last OFFSET:%s",this_date)
        except Exception:
            logger.error("Create self.table in clickhouse", exc_info=True)

    async def update(self):
        try:
            print("hello")
            offset = self.get_offset()
            # LOAD THE DATE
            this_date = offset + timedelta(days=1)
            #logger.warning("OFFSET INCREASED:%s",offset)
            self.manage_sliding_df(this_date)
            self.update_checkpoint_dict(this_date)
            if self.df is not None:
                if len(self.df) > 0:
                    b = self.prep_columns(self.df, this_date)
                    # calculate daily value, because of double entry, cannot simply sum,
                    # so filter out half the transactions
                    df = self.df.compute()
                    df = df[['value']]
                    df = df[df.value >= 0]
                    value_count, value = df.value.count(),df.value.mean()

                    # construct dict to append to save-list
                    summary = self.get_warehouse_summary(this_date)
                    #logger.warning("summary:%s",summary)
                    message = {
                        'block_timestamp': this_date.date(),
                        'new_lst': b['new_str'],
                        'churned_lst': b['churned_str'],
                        'retained_lst': b['retained_str'],
                        'active_lst': b['active_str'],
                        'new':len(b['new_lst']),
                        'churned':len(b['churned_lst']),
                        'retained':len(b['retained_lst']),
                        'active':len(b['active_lst']),
                        'value':round(value,4),
                        'value_counts':value_count,
                        'block_year':int(this_date.year),
                        'block_month':int(this_date.month),
                        'block_day':int(this_date.day),
                        'day_of_week': this_date.strftime('%a'),
                        'block_size': summary["block_size"],
                        'block_time': summary["block_time"],
                        'difficulty': summary["difficulty"],
                        'nrg_limit': summary["nrg_limit"],
                        'nrg_reward': summary["nrg_reward"],
                        'num_transactions':summary["num_transactions"],
                        'block_nrg_consumed':summary["block_nrg_consumed"],
                        'transaction_nrg_consumed':summary["transaction_nrg_consumed"],
                        'nrg_price':summary["nrg_price"]
                    }
                    #logger.warning('date:message-%s:%s',this_date,message)

                    self.batch_counter += 1
                    self.batch_messages.append(message)
                    if self.batch_counter < self.batch_counter_threshold:
                        # send the messages immmediately if it is up to date
                        if isinstance(this_date, datetime):
                            this_date = this_date.date()
                        window_edge_date = datetime.now() - timedelta(days=self.churn_window + 1)
                        if isinstance(window_edge_date, datetime):
                            window_edge_date = window_edge_date.date()
                        if this_date >= window_edge_date:
                            # register new events
                            df = pd.DataFrame(self.batch_messages)
                            # save dataframe
                            df = dd.from_pandas(df, npartitions=3)
                            # logger.warning("INSIDE SAVE DF:%s", df.columns.tolist())
                            self.save_df(df)
                            self.batch_counter = 0
                    else:
                        #logger.warning("batch counter(line 332):%s",self.batch_counter)
                        # register new events
                        df = pd.DataFrame(self.batch_messages)
                        # save dataframe
                        df = dd.from_pandas(df, npartitions=2)
                        # logger.warning("INSIDE SAVE DF:%s", df.columns.tolist())
                        self.save_df(df)
                        self.batch_counter = 0
                        self.batch_messages = []


        except Exception:
            logger.error("update", exc_info=True)


    async def run(self):
        # create warehouse table in clickhouse if needed
        # self.create_table_in_clickhouse()
        while True:
            await self.update()
            if self.is_up_to_date(construct_table='account_activity',
                                  storage_medium='clickhouse',
                                  window_hours= self.is_up_to_date_window
                                  ):
                logger.warning("NETWORK ACTIVITY CHURN SLEEPING FOR 1 DAY:UP TO DATE")
                await asyncio.sleep(86400)  # sleep one day
            else:
                await asyncio.sleep(1)
