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

executor = ThreadPoolExecutor(max_workers=20)
logger = mylogger(__file__)

class PythonMysql:
    # port = '9000'
    ch = sa.create_engine('clickhouse://default:@127.0.0.1:8123/aion')
    def __init__(self,db):
        self.schema = db
        self.connection = MySQLdb.connect(user='clickhouse', password='1233tka061',
                                    database=db, host='40.113.226.240')
        self.conn = self.connection.cursor()
        self.DATEFORMAT = "%Y-%m-%d %H:%M:%S"

    def date_to_int(self, x):
        return int(x.timestamp())

    def int_to_date(self, x):
        return datetime.utcfromtimestamp(x).strftime(self.DATEFORMAT)

    def construct_read_query(self, table, cols, startdate, enddate):
        qry = 'SELECT '
        if len(cols) >= 1:
            for pos, col in enumerate(cols):
                qry += col
                if pos < len(cols)-1:
                    qry += ','
        else:
            qry += '*'
        if table == 'token_transfers':
            qry += """ FROM {}.{} WHERE transfer_timestamp >= {} AND 
                               transfer_timestamp <= {} ORDER BY transfer_timestamp""" \
                .format(self.schema, table, startdate, enddate)
        else:
            qry += """ FROM {}.{} WHERE block_timestamp >= {} AND 
                   block_timestamp <= {} ORDER BY block_timestamp""" \
                .format(self.schema, table, startdate, enddate)

        # logger.warning('query:%s', qry)
        return qry

    def load_data(self,table,cols,start_date,end_date):
        logger.warning('%s load data start_date,%s:%s',table,start_date, end_date)

        start_date = self.date_to_int(start_date)
        end_date = self.date_to_int(end_date)
        # logger.warning('table:cols=%s:%s', table, cols)

        if start_date > end_date:
            logger.warning("END DATE IS GREATER THAN START DATE")
            logger.warning("BOTH DATES SET TO START DATE")
            start_date = end_date
        sql = self.construct_read_query(table, cols, start_date, end_date)
        try:

            df = pd.read_sql(sql,self.connection)
            if df is not None:
                if len(df)>0:
                    # do some renaming

                    if table in ['token_transfers']:
                        rename = {"transfer_timestamp": "block_timestamp",
                                  "approx_value":"value"}

                    elif table in ['internal_transfer']:
                        rename = {"approx_value":"value"}

                    elif table in ['transaction']:
                        rename = {"approx_value":"value"}

                    elif table in ['block']:
                        rename = {
                            'nrg_consumed':'block_nrg_consumed',
                            'month':'block_month',
                            'day':'block_day',
                            'year':'block_year',
                            'approx_nrg_reward':'nrg_reward'
                        }
                    df = df.rename(index=str, columns=rename)

                    # convert to dask
                    df = dd.dataframe.from_pandas(df, npartitions=5)
                    #logger.warning("%s data loaded from mysql:%s",table.upper(),df.columns.tolist())
                    if table not in ['transaction']:
                        df['block_timestamp'] = df['block_timestamp'].map(self.int_to_date)
                    #logger.warning("%s data loaded from mysql:%s",table.upper(),df.head(10))
            return df

        except Exception:
            logger.error('mysql load data :%s', exc_info=True)