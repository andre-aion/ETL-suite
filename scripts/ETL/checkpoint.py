from datetime import datetime, timedelta, date
from statistics import mean

from config.checkpoint import checkpoint_dict
from scripts.storage.pythonClickhouse import PythonClickhouse
from scripts.storage.pythonMysql import PythonMysql
from scripts.storage.pythonRedis import PythonRedis
from scripts.utils.mylogger import mylogger

logger = mylogger(__file__)

class Checkpoint:
    def __init__(self,table):
        self.checkpoint_dict = None
        self.dct = checkpoint_dict[table]
        self.key_params = 'checkpoint:'+ table
        self.redis = PythonRedis()
        self.cl = PythonClickhouse('aion')
        self.my = PythonMysql('aion')
        self.DATEFORMAT = "%Y-%m-%d %H:%M:%S"
        self.window = 24 # hours
        self.DATEFORMAT = "%Y-%m-%d %H:%M:%S"
        self.is_up_to_date_window = 4 # hours
        self.table = table
        self.initial_date = "2018-04-23 20:00:00"
        # manage size of warehouse
        self.df_size_lst = []
        self.df_size_threshold = {
            'upper': 60000,
            'lower': 40000
        }
        self.checkpoint_column = 'block_timestamp'


    def save_checkpoint(self):
        try:
            self.redis.save(self.checkpoint_dict, self.key_params, "", "", type='checkpoint')
            # logger.warning('CHECKPOINT SAVED TO REDIS:%s', self.key_params)
        except Exception:
            logger.error("Construct table query", exc_info=True)

    def get_checkpoint_dict(self, col='block_timestamp', db='aion'):
        # try to load from , then redis, then clickhouse
        try:
            key = self.key_params
            if self.checkpoint_dict is not None:
                if self.checkpoint_dict['offset'] is None:
                    # get it from redis
                    temp_dct = self.redis.load([], '', '', key=key, item_type='checkpoint')
                    if temp_dct is not None:
                        self.checkpoint_dict = temp_dct
                    else: # set if from config
                        self.checkpoint_dict = self.dct
                    #logger.warning(" %s CHECKPOINT dictionary (re)set or retrieved:%s", self.table, self.temp_dict)
            else:
                self.checkpoint_dict = self.dct
                self.get_checkpoint_dict()
                #logger.warning(" %s CHECKPOINT dictionary recursion call :%s", self.table, self.temp_dict)

        except Exception:
            logger.error("get checkpoint dict", exc_info=True)


    def get_offset(self):
        try:
            self.get_checkpoint_dict()
            # handle reset or initialization
            if self.checkpoint_dict['offset'] is None:
                if self.table == 'account_activity':
                    self.checkpoint_dict['offset'] = self.get_value_from_mysql(self.table, min_max='MAX')
                elif self.table == 'network_activity':
                    self.checkpoint_dict['offset'] = self.get_value_from_clickhouse(self.table, min_max='MAX')
                logger.warning("Checkpoint retreived from construct table:%s", self.checkpoint_dict['offset'])
                if  self.checkpoint_dict['offset'] is None:
                    self.checkpoint_dict['offset'] = self.initial_date

            # convert offset to datetime if needed
            if isinstance(self.checkpoint_dict['offset'], str):
                self.checkpoint_dict['offset'] = datetime.strptime(self.checkpoint_dict['offset'],
                                                                   self.DATEFORMAT)


            return self.checkpoint_dict['offset']

        except Exception:
            logger.error('register new addresses', exc_info=True)

    def reset_offset(self, reset_value):
        try:
            self.get_checkpoint_dict()
            if isinstance(reset_value, datetime) or isinstance(reset_value, date):
                reset_value = datetime.strftime(reset_value, self.DATEFORMAT)
            self.checkpoint_dict['offset'] = reset_value
            self.checkpoint_dict['timestamp'] = datetime.now().strftime(self.DATEFORMAT)
            self.save_checkpoint()
            logger.warning("CHECKPOINT reset:%s", self.checkpoint_dict)
        except Exception:
            logger.error('reset checkpoint :%s', exc_info=True)

    def update_checkpoint_dict(self, offset):
        try:
            #logger.warning("INSIDE UPDATE CHECKPOINT DICT")
            if isinstance(offset,str):
                offset = datetime.strptime(offset,self.DATEFORMAT)
            offset = offset + timedelta(seconds=1)
            # update checkpoint
            self.checkpoint_dict['offset'] = datetime.strftime(offset,self.DATEFORMAT)
            self.checkpoint_dict['timestamp'] = datetime.now().strftime(self.DATEFORMAT)
        except Exception:
            logger.error("make warehouse", exc_info=True)

    def save_df(self, df):
        try:

            self.cl.upsert_df(df,self.columns,self.table)
            self.checkpoint_dict['timestamp'] = datetime.now().strftime(self.DATEFORMAT)
            self.save_checkpoint()
            # logger.warning("DF with offset %s SAVED TO CLICKHOUSE,dict save to REDIS:%s",
            # self.checkpoint_dict['offset'],
            # self.checkpoint_dict['timestamp'])
        except:
            logger.error("save dataframe to clickhouse", exc_info=True)

    def get_value_from_clickhouse(self, table, min_max='MAX'):
        try:
            qry = "select count() from {}.{}".format('aion', table)
            numrows = self.cl.client.execute(qry)
            if numrows[0][0] >= 1:
                qry = """select {}({}) from {}.{} AS result LIMIT 1""" \
                    .format(min_max, self.checkpoint_column, 'aion', table)
                result = self.cl.client.execute(qry)
                # logger.warning('%s value from clickhouse:%s',min_max,result[0][0])
                return result[0][0]
            return self.initial_date  # if block_tx_warehouse is empty
        except Exception:
            logger.error("update warehouse", exc_info=True)

    def int_to_date(self, x):
        return datetime.utcfromtimestamp(x).strftime(self.DATEFORMAT)

    def get_value_from_mysql(self, table, min_max='MAX'):
        try:
            qry = """select {}({}) AS result from {}.{}  LIMIT 1""" \
                .format(min_max, self.checkpoint_column, 'aion', table)
            result = self.my.conn.execute(qry)
            row = self.my.conn.fetchone()
            result = row[0]
            #logger.warning('%s value from mysql %s:%s', min_max, table.upper(), result)
            if result is not None:
                return result
            return self.initial_date
        except Exception:
            logger.error("update warehouse", exc_info=True)

    def window_adjuster(self):
        if len(self.df_size_lst) > 5:
            if mean(self.df_size_lst) >= self.df_size_threshold['upper']:
                self.window = round(self.window * .75)
                logger.warning("WINDOW ADJUSTED DOWNWARDS FROM: %s hours", self.window)
            elif mean(self.df_size_lst) <= self.df_size_threshold['lower']:
                self.window = round(self.window * 1.25)
                logger.warning("WINDOW ADJUSTED UPWARDS FROM: %s hours",  self.window)
            self.df_size_lst = []

        # check max date in a construction table

    def is_up_to_date(self, construct_table,window_hours):
        try:
            offset = self.get_offset()
            construct_max_val = self.get_value_from_clickhouse(construct_table, 'MAX')
            if isinstance(construct_max_val, str):
                construct_max_val = datetime.strptime(construct_max_val, self.DATEFORMAT)
                construct_max_val = construct_max_val.date()

            if offset >= construct_max_val - timedelta(hours=window_hours):
                # logger.warning("CHECKPOINT:UP TO DATE")
                return True
            # logger.warning("NETWORK ACTIVITY CHECKPOINT:NOT UP TO DATE")

            return False
        except Exception:
            logger.error("is_up_to_date", exc_info=True)
            return False


