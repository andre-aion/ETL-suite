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
        self.checkpoint_dict = checkpoint_dict[table]
        self.redis = PythonRedis()
        self.cl = PythonClickhouse('aion')
        self.my = PythonMysql('aion')
        self.DATEFORMAT = "%Y-%m-%d %H:%M:%S"
        self.window = 24 # hours
        self.DATEFORMAT = "%Y-%m-%d %H:%M:%S"
        self.is_up_to_date_window = 4 # hours
        self.table = table
        self.key_params = 'checkpoint:'+ table
        self.dct = checkpoint_dict
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
            if self.checkpoint_dict is None:
                key = 'checkpoint:' + self.table
                temp_dict = self.redis.load([], '', '', key=key, item_type='checkpoint')
                if temp_dict is None:  # not in redis
                    self.checkpoint_dict = self.dct
                    # get last date from clickhouse
                    qry = "select count() from {}.{}".format(db, self.table)
                    numrows = self.cl.client.execute(qry)
                    if numrows[0][0] >= 1:
                        result = self.get_value_from_clickhouse(self.table, 'MAX')
                        self.checkpoint_dict['offset'] = result
                        self.checkpoint_dict['timestamp'] = datetime.now().strftime(self.DATEFORMAT)
                else:
                    self.checkpoint_dict = temp_dict  # retrieved from redis
                    # ensure that the offset in redis is good
                    if self.checkpoint_dict['offset'] is None:
                        result = self.get_value_from_clickhouse(self.table, 'MAX')
                        self.checkpoint_dict['offset'] = result
                        self.checkpoint_dict['timestamp'] = datetime.now().strftime(self.DATEFORMAT)

            # logger.warning("CHECKPOINT dictionary (re)set or retrieved:%s",self.checkpoint_dict)
            return self.checkpoint_dict
        except Exception:
            logger.error("get checkpoint dict", exc_info=True)

    def reset_offset(self, reset_value):
        try:
            self.checkpoint_dict = self.get_checkpoint_dict()
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

            return result
        except Exception:
            logger.error("update warehouse", exc_info=True)

    def window_adjuster(self):
        if len(self.df_size_lst) > 5:
            if mean(self.df_size_lst) >= self.df_size_threshold['upper']:
                self.window = round(self.window * .75)
                logger.warning("WINDOW ADJUSTED DOWNWARDS FROM: %s", mean(self.df_size_lst))
            elif mean(self.df_size_lst) <= self.df_size_threshold['lower']:
                self.window = round(self.window * 1.25)
                logger.warning("WINDOW ADJUSTED UPWARDS FROM: %s",  mean(self.df_size_lst))
            self.df_size_lst = []

    # check max date in a construction table
    def is_up_to_date(self, construct_table='block'):
        try:
            offset = self.checkpoint_dict['offset']
            if offset is None:
                offset = self.initial_date
                self.checkpoint_dict['offset'] = self.initial_date
            if isinstance(offset, str):
                offset = datetime.strptime(offset, self.DATEFORMAT)
            construct_max_val = self.get_value_from_clickhouse(construct_table, 'MAX')
            # logger.warning("max_val in is_up_to_date:%s",construct_max_val)
            if offset >= construct_max_val - timedelta(hours=self.is_up_to_date_window):
                return True
            return False
        except Exception:
            logger.error("is_up_to_date", exc_info=True)
            return False

    def get_offset(self):
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
                # logger.warning("Checkpoint initiated in update warehoused:%s", offset)
                if offset is None:
                    offset = self.initial_date
                self.checkpoint_dict['offset'] = offset

                # convert offset to datetime if needed
            offset = self.checkpoint_dict['offset']
            if isinstance(offset, str):
                offset = datetime.strptime(offset, self.DATEFORMAT)

            return offset

        except Exception:
            logger.error('register new addresses', exc_info=True)
