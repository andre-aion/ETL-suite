import datetime

from tornado.gen import coroutine

from scripts.utils.mylogger import mylogger
from concurrent.futures import ThreadPoolExecutor
from clickhouse_driver import Client as Clickhouse_Client

import pandas as pd
import dask as dd
import numpy as np
from datetime import datetime
import sqlalchemy as sa
import pandahouse
import MySQLdb
from pymongo import MongoClient
from pprint import pprint

logger = mylogger(__file__)

class PythonMongo:
    # port = '9000'
    # ch = sa.create_engine('clickhouse://default:@127.0.0.1:8123/aion')
    def __init__(self, db):
        self.client = MongoClient('localhost', 27017)
        self.db = self.client['aion']
        collection = 'external'

    def load_data(self, table,start_date,end_date):

        try:
            #logger.warning('load date range %s:%s',start_date,end_date)
            df = pd.DataFrame(list(self.db.external.find(
                {'date': {'$lte': end_date, '$gte': start_date}},{'_id':False}
            )))
            if df is not None:
                if len(df) > 0:
                    #logger.warning('external:%s',df.head(5))
                    # add month, day, year columns
                    df['block_month'] = df['date'].dt.month
                    df['block_day'] = df.date.dt.day
                    df['block_year'] = df.date.dt.year

                    df = dd.dataframe.from_pandas(df, npartitions=1)

            return df
        except Exception:
            logger.error('load data',exc_info=True)
