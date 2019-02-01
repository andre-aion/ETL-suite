import time
from datetime import timedelta, datetime, date

import asyncio

from config.df_construct_config import table_dict, columns
from scripts.ETL.checkpoint import Checkpoint
from scripts.utils.mylogger import mylogger
from scripts.utils.myutils import concat_dfs
import pandas as pd
import dask.dataframe as dd

logger = mylogger(__file__)


class NetworkTxActivity(Checkpoint):
    def __init__(self, table):
        Checkpoint.__init__(self, table)
        self.table = table
        self.active_list = []
        self.churn_window = 7
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
        self.cols = columns[table]
        self.initial_date = '2018-05-01 00:00:00'
        self.checkpoint_column = 'block_timestamp'
        self.temp_lst = []

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
            state = 'initial'
            self.df = self.load_df(this_date, state=state)
            if len(self.df) > 0:
                min, max = dd.compute(self.df['block_timestamp'].min(), self.df['block_timestamp'].max())
                logger.warning('AFTER LOAD, current df range %s:%s', min, max)

        except Exception:
            logger.error('', exc_info=True)

    def zero_out_datetime(self, x):
        logger.warning('line 129,x=%s', x)
        if isinstance(x, str):
            x = datetime.strptime(x, self.DATEFORMAT)
        x = datetime(year=x.year, month=x.month, day=x.day, hour=0, minute=0, second=0)

        return x

    def load_df(self, this_date, state='initial', table='block_tx_warehouse'):
        try:
            cols = ['block_timestamp',
                    'difficulty',
                    'block_nrg_consumed', 'nrg_limit', 'num_transactions',
                    'block_size', 'block_time', 'nrg_reward',
                    'from_addr', 'to_addr', 'value', 'transaction_nrg_consumed', 'nrg_price']

            if state == 'initial':
                start_date = this_date - timedelta(days=self.churn_window)
                end_date = this_date + timedelta(days=self.churn_window + 1)
            else:
                start_date = this_date
                end_date = this_date + timedelta(days=1)

            start_date = datetime.combine(start_date, datetime.min.time())
            end_date = datetime.combine(end_date, datetime.min.time())
            # logger.warning('BEFORE LOAD DATA %s:%s',start_date, end_date)

            df = self.cl.load_data(table=table, cols=cols,
                                   start_date=start_date, end_date=end_date)
            # logger.warning('line 157, load:%s',df['block_timestamp'])
            # convert to datetime to date
            # df['block_timestamp'] = df['block_timestamp'].map(self.zero_out_datetime)
            # df['block_timestamp'] = df['block_timestamp'].map(self.cast_date)

            return df
        except Exception:
            logger.warning('load_df', exc_info=True)

    def update_checkpoint_dict(self, offset):
        try:
            # logger.warning("INSIDE UPDATE CHECKPOINT DICT")
            if isinstance(offset, str):
                offset = datetime.strptime(offset, self.DATEFORMAT)
            # update checkpoint
            self.checkpoint_dict['offset'] = datetime.strftime(offset, self.DATEFORMAT)
            self.checkpoint_dict['timestamp'] = datetime.now().strftime(self.DATEFORMAT)
        except Exception:
            logger.error("make warehouse", exc_info=True)

    def str_to_lst(self, string, state='new'):
        self.temp_lists['active'] += string.split(',')

    def prepare_tier(self, df, this_date, tier=1):
        try:
            agg_col = "to_addr"
            if tier in [1, "1"]:
                agg_col = "from_addr"
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

                logger.warning('length of df for %s:%s', this_date, len(df))
                df = df.compute()
                lst = df[col].unique()
            return lst
        except Exception:
            logger.error('get daily miners', exc_info=True)

    def get_new_retained(self, df, this_date_lst, this_date, tier_col='from_addr'):
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

    def get_churned(self, df, this_date_lst, this_date, tier_col='from_addr'):
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
                    logger.warning('%s forward active list:%s', tier_col, len(active_lst))
                    churned_lst = list(set(this_date_lst).difference(active_lst))
            #logger.warning('%s churned list:%s', tier_col, len(churned_lst))
            return churned_lst
        except Exception:
            logger.error('', exc_info=True)

    def prepare_tiers(self, df, this_date):
        try:
            tier_info = {
                '1': {},
                '2': {}
            }
            # load dataframe
            for key, value in tier_info.items():
                tier_info[key] = self.prepare_tier(self.df, this_date, int(key))
            return tier_info
        except Exception:
            logger.error('prepare for write', exc_info=True)

    def create_table_in_clickhouse(self):
        try:
            self.cl.create_table(self.table, self.table_dict, self.cols)
        except Exception:
            logger.error("Create self.table in clickhosue", exc_info=True)

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
            logger.error("Create self.table in clickhosue", exc_info=True)

    async def update(self):
        try:
            offset = self.get_offset()
            # LOAD THE DATE
            this_date = offset + timedelta(days=1)
            # logger.warning("OFFSET INCREASED:%s",offset)
            self.manage_sliding_df(this_date)
            if len(self.df) > 0:
                tier_info = self.prepare_tiers(self.df, this_date)
                a = tier_info["1"]
                b = tier_info["2"]
                # message for daily save
                block_size, block_time, difficulty, nrg_limit, approx_nrg_reward, num_transactions, \
                block_nrg_consumed, transaction_nrg_consumed, nrg_price, value = \
                    dd.compute(self.df.block_size.mean(), self.df.block_time.mean(), self.df.difficulty.mean(),
                               self.df.nrg_limit.mean(), self.df.approx_nrg_reward.mean(),
                               self.df.num_transactions.mean(),
                               self.df.block_nrg_consumed.mean(), self.df.transaction_nrg_consumed.mean(),
                               self.df.nrg_price.mean(),
                               self.df.value.mean())
                message = (this_date,
                           a['new_str'], a['churned_str'], a['retained_str'], a['active_str'],
                           len(a['new_lst']), len(a['churned_lst']), len(a['retained_lst']), len(a['active_lst']),
                           b['new_str'], b['churned_str'], b['retained_str'], b['active_str'],
                           len(b['new_lst']), len(b['churned_lst']), len(b['retained_lst']), len(b['active_lst']),
                           round(block_size), round(block_time), round(difficulty), round(nrg_limit),
                           round(approx_nrg_reward), round(num_transactions),
                           round(block_nrg_consumed), round(transaction_nrg_consumed), round(nrg_price),
                           round(value),
                           int(this_date.year), int(this_date.month), int(this_date.day), this_date.strftime('%a'))
                # logger.warning('date:message-%s:%s',this_date,message)

                self.batch_counter += 1
                self.update_checkpoint_dict(this_date)
                self.batch_messages.append(message)
                if self.batch_counter < self.batch_counter_threshold:
                    self.batch_counter += 1
                    # send the messages immmediately if it is up to date
                    if isinstance(this_date, datetime):
                        this_date = this_date.date()
                    window_edge_date = datetime.now() - timedelta(days=self.churn_window + 1)
                    if isinstance(window_edge_date, datetime):
                        window_edge_date = window_edge_date.date()
                    if this_date >= window_edge_date:
                        self.save_messages(this_date)
                        self.batch_counter = 1

                else:
                    # logger.warning("batch counter(line 332):%s",self.batch_counter)
                    self.save_messages(this_date)
            else:  # self.df is empty

                self.update_checkpoint_dict(this_date)

        except Exception:
            logger.error("update", exc_info=True)


    async def run(self):
        # create warehouse table in clickhouse if needed
        # self.create_table_in_clickhouse()
        while True:
            await self.update()
            if self.is_up_to_date(construct_table='block_tx_warehouse',
                                  window_hours= self.is_up_to_date_window * 24):
                logger.warning("NETWORK ACTIVITY SLEEPING FOR 1 DAY:UP TO DATE")
                await asyncio.sleep(86400)  # sleep one day
            else:
                await asyncio.sleep(1)
