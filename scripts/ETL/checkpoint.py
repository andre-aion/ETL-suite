from datetime import datetime, timedelta, date
from statistics import mean

from config.checkpoint import checkpoint_dict
from scripts.storage.pythonClickhouse import PythonClickhouse
from scripts.storage.pythonMysql import PythonMysql
from scripts.storage.pythonRedis import PythonRedis
from scripts.storage.pythonMongo import PythonMongo

from scripts.utils.mylogger import mylogger

logger = mylogger(__file__)

class Checkpoint:
    def __init__(self,table):
        self.redis = PythonRedis()
        self.cl = PythonClickhouse('aion')
        self.pym = PythonMongo('aion')
        self.my = PythonMysql('localhost')
        self.DATEFORMAT = "%Y-%m-%d %H:%M:%S"
        self.window = 3 # hours
        self.is_up_to_date_window = self.window + 2 # hours
        self.table = table
        self.initial_date = datetime.strptime("2018-04-24 00:00:00",self.DATEFORMAT)
        # manage size of warehouse
        self.df_size_lst = []
        self.df_size_threshold = {
            'upper': 60000,
            'lower': 40000
        }
        # manage checkpoints
        self.checkpoint_column = 'block_timestamp'
        self.checkpoint_key = table
        self.checkpoint_dict = None
        if self.table != 'external_daily':
            self.dct = checkpoint_dict[self.checkpoint_key]
        else:
            self.dct = None
        self.key_params = 'checkpoint:' + self.checkpoint_key

    def int_to_date(self, x):
        return datetime.utcfromtimestamp(x).strftime(self.DATEFORMAT)

    def str_to_date(self,x):
        if isinstance(x,str):
            return datetime.strptime(x,self.DATEFORMAT)
        return x

    def datetime_to_date(self,x):
        if isinstance(x,str):
            x = datetime.strptime(x,self.DATEFORMAT)
        if isinstance(x, datetime):
            return datetime.combine(x.date(), datetime.min.time())
        if isinstance(x, date):
            return datetime.combine(x, datetime.min.time())


    def load_df(self, start_date, end_date, cols, table, storage_medium):
        try:
            start_date = self.str_to_date(start_date)
            end_date = self.str_to_date(end_date)
            #start_date = datetime.combine(start_date, datetime.min.time())
            if storage_medium == 'mysql':
                df = self.my.load_data(table=table, cols=cols,
                                       start_date=start_date, end_date=end_date)
            elif storage_medium == 'clickhouse':
                df = self.cl.load_data(table=table, cols=cols,
                                       start_date=start_date, end_date=end_date)
            # logger.warning('line 157, load:%s',df['block_timestamp'])
            # convert to datetime to date

            return df
        except Exception:
            logger.warning('load_df', exc_info=True)

    def save_checkpoint(self):
        try:
            self.redis.save(self.checkpoint_dict, self.key_params, "", "", type='checkpoint')
            # logger.warning('CHECKPOINT SAVED TO REDIS:%s', self.key_params)
        except Exception:
            logger.error("Construct table query", exc_info=True)


    def get_offset(self):
        try:
            if self.table == 'external_daily': # scrapers, github
                # handle reset or initialization
                offset = self.get_value_from_mongo(self.table, min_max='MAX')
                if offset is None:
                    offset = self.initial_date

                # convert offset to datetime if needed
                if isinstance(offset, str):
                    offset = datetime.strptime(offset,'%Y-%m-%d %H:%M:%S')

                # SET DATETIME TO DATE WITH MIN TIME
                # ensure date fits mongo scheme
                if isinstance(offset, date):
                    offset = datetime.combine(offset,datetime.min.time())
                if isinstance(self.checkpoint_dict['offset'], datetime):
                    self.checkpoint_dict['offset'] = datetime.combine(self.checkpoint_dict['offset'].timestamp(),
                                                                      datetime.min.time())
            else: # aion etls
                # handle reset or initialization
                offset = self.get_value_from_clickhouse(self.table, min_max='MAX')
                if offset is None:
                    offset = self.initial_date

                # convert offset to datetime if needed
                if isinstance(offset, str):
                    offset = datetime.strptime(offset,self.DATEFORMAT)

            return offset

        except Exception:
            logger.error('get offset', exc_info=True)

    def reset_offset(self, reset_value):
        try:
            self.get_checkpoint_dict()
            if isinstance(reset_value, datetime) or isinstance(reset_value, date):
                reset_value = datetime.strftime(reset_value, self.DATEFORMAT)
            self.checkpoint_dict['offset'] = reset_value
            self.checkpoint_dict['timestamp'] = datetime.now().strftime(self.DATEFORMAT)
            if 'items_updated' in self.checkpoint_dict.keys():
                self.checkpoint_dict['items_updated'] = []
            self.save_checkpoint()
            logger.warning("CHECKPOINT reset:%s", self.checkpoint_dict)
        except Exception:
            logger.error('reset checkpoint :%s', exc_info=True)

    def update_checkpoint_dict(self, offset):
        try:
            logger.warning('update checkpoint:%s',offset)
            if isinstance(offset,str):
                offset = datetime.strptime(offset,self.DATEFORMAT)
                offset = offset + timedelta(seconds=1)
            # update checkpoint
            self.checkpoint_dict['offset'] = datetime.strftime(offset, self.DATEFORMAT)
            self.checkpoint_dict['timestamp'] = datetime.now().strftime(self.DATEFORMAT)
        except Exception:
            logger.error("make warehouse", exc_info=True)

    def save_df(self,df,columns,table,timestamp_col):
        try:

            self.cl.upsert_df(df,columns,table,timestamp_col)
            # logger.warning("DF with offset %s SAVED TO CLICKHOUSE,dict save to REDIS:%s",
            # self.checkpoint_dict['offset'],
            # self.checkpoint_dict['timestamp'])
        except:
            logger.error("save dataframe to clickhouse", exc_info=True)

    def get_value_from_clickhouse(self, table, column='timestamp', min_max='MAX',db='aion'):
        try:
            qry = "select count() from {}.{}".format(db, table)
            numrows = self.cl.client.execute(qry)
            if numrows[0][0] >= 1:
                qry = """select {}({}) from {}.{} AS result LIMIT 1""" \
                    .format(min_max, self.checkpoint_column, db, table)
                result = self.cl.client.execute(qry)
                #logger.warning('%s value from clickhouse:%s',min_max,result[0][0])
                return result[0][0]
            return self.initial_date  # if block_tx_warehouse is empty
        except Exception:
            #logger.error("get value from clickhouse", exc_info=True)
            return self.initial_date

    def get_value_from_mysql(self, table, column='block_timestamp',min_max='MAX',db='aion'):
        try:
            qry = """select {}({}) AS result from {}.{}  LIMIT 1""" \
                .format(min_max, column, db, table)
            result = self.my.conn.execute(qry)
            row = self.my.conn.fetchone()
            result = row[0]
            #logger.warning('%s value from mysql %s:%s', min_max, table.upper(), result)
            if result is not None:
                return result
            return self.initial_date
        except Exception:
            logger.error("get value from mysql", exc_info=True)

    def get_value_from_mongo(self, table, column='timestamp',min_max='MAX',db='aion'):
        try:
            self.pym = PythonMongo(db)
            if min_max == 'MAX':
                result = self.pym.db[table].find(
                    {column:{'$exists':True}}).sort(column, -1).limit(1)
            else:
                result = self.pym.db[table].find(
                    {column:{'$exists':True}}).sort(column, 1).limit(1)

            if result.count() > 0:
                for res in result:
                    #logger.warning('%s value from mongo %s:%s', min_max, table.upper(), res['timestamp'])
                    return res['timestamp']
            else:
                return self.initial_date
        except Exception:
            logger.error("get value from mongo", exc_info=True)

    def window_adjuster(self):
        if len(self.df_size_lst) > 5:
            if mean(self.df_size_lst) >= self.df_size_threshold['upper']:
                self.window = round(self.window * .75)
                logger.warning("WINDOW ADJUSTED DOWNWARDS FROM: %s hours", self.window)
            elif mean(self.df_size_lst) <= self.df_size_threshold['lower']:
                self.window = round(self.window * 1.25)
                logger.warning("WINDOW ADJUSTED UPWARDS FROM: %s hours",  self.window)
            self.is_up_to_date_window = self.window + 2
            self.df_size_lst = []

        # check max timestamp in a construction table

    def is_up_to_date(self, construct_table,storage_medium,window_hours,db):
        try:
            offset = self.get_offset()
            if self.table in ['external_daily']:
                today = datetime.combine(datetime.today().date(), datetime.min.time())
                if offset >= today - timedelta(hours=window_hours):
                    return True
                return False
            else:
                if storage_medium == 'mysql':
                    construct_max_val = self.get_value_from_mysql(construct_table,
                                                                  column='block_timestamp',
                                                                  min_max='MAX',db=db)

                elif storage_medium == 'clickhouse':
                    try:
                        construct_max_val = self.get_value_from_clickhouse(construct_table,column='timestamp',
                                                                           min_max='MAX',db=db)
                    except: #table does not exist
                        logger.warning('clickhouse table does not exist')
                        return False

                elif storage_medium == 'mongo':
                    construct_max_val = self.get_value_from_mongo(construct_table,column='timestamp',
                                                                  min_max='MAX',db=db)

                if isinstance(construct_max_val,int):
                    construct_max_val = datetime.fromtimestamp(construct_max_val)
                if isinstance(construct_max_val, str):
                    construct_max_val = datetime.strptime(construct_max_val, self.DATEFORMAT)
                    construct_max_val = construct_max_val.date()

                logger.warning('self.table:%s',self.table)
                logger.warning("offset:construct max=%s:%s",offset,(construct_max_val - timedelta(hours=window_hours)))
                if offset >= construct_max_val - timedelta(hours=window_hours):
                    # logger.warning("CHECKPOINT:UP TO DATE")
                    return True
                # logger.warning("NETWORK ACTIVITY CHECKPOINT:NOT UP TO DATE")

                return False
        except Exception:
            logger.error("is_up_to_date", exc_info=True)
            return False



