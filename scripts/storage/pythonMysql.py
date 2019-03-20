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


credentials = {}
credentials['dennis'] = {
    'user':'clickhouse',
    'host':'192.168.1.7',
    'db':'aion_analytics',
    'password': 'lDhtoP1xcDVaQalqhiWmgNOw5'
}
credentials['office'] = {
    'user':'clickhouse',
    'host':'104.200.110.155',
    'db':'aion_analytics',
    'password': 'lDhtoP1xcDVaQalqhiWmgNOw5'
}
credentials['staging'] = {
    'user':'clickhouse',
    'host':'104.208.35.165',
    'db':'aion',
    'password': '1233tka061'
}

credentials['localhost'] = {
    'user':'admin',
    'host':'127.0.0.1',
    'db':'aion_analytics',
    'password': 'password'
}

class PythonMysql:
    # port = '9000'
    #ch = sa.create_engine('clickhouse://default:@127.0.0.1:8123/aion')
    def __init__(self,credential='localhost'):
        tmp = credentials[credential]
        self.schema = tmp['db']
        self.connection = MySQLdb.connect(user=tmp['user'],
                                          password=tmp['password'],
                                          database=tmp['db'],
                                          host=tmp['host'])

        self.conn = self.connection.cursor()
        self.DATEFORMAT = "%Y-%m-%d %H:%M:%S"

    def date_to_int(self, x):
        return int(x.timestamp())

    def int_to_date(self, x):
        if isinstance(x,int):
            return datetime.fromtimestamp(x).strftime(self.DATEFORMAT)
        return x

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

        #logger.warning('query:%s', qry)
        return qry

    def load_data(self,table,cols,start_date,end_date,type='dask'):
        #logger.warning('%s load data start_date,%s:%s',table,start_date, end_date)

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
                    rename = {}
                    if table in ['token_transfers']:
                        if 'transfer_timestamp' in df.columns.tolist():
                            rename['transfer_timestamp'] = 'block_timestamp'
                        if 'approx_value' in df.columns.tolist():
                            rename['approx_value'] = 'value'
                    elif table in ['internal_transfer']:
                        if 'approx_value' in df.columns.tolist():
                            rename['approx_value'] = 'value'
                    elif table in ['block']:
                        if 'nrg_consumed' in df.columns.tolist():
                            rename['nrg_consumed'] = 'block_nrg_consumed'
                        if 'month' in df.columns.tolist():
                            rename['month'] = 'block_month'
                        if 'day' in df.columns.tolist():
                            rename['day'] = 'block_day'
                        if 'year' in df.columns.tolist():
                            rename['year'] = 'block_year'
                        if 'approx_nrg_reward' in df.columns.tolist():
                            rename['approx_nrg_reward'] = 'nrg_reward'
                    elif table in ['transaction']:
                        if 'nrg_consumed' in df.columns.tolist():
                            rename['nrg_consumed'] = 'transaction_nrg_consumed'
                        if 'approx_value' in df.columns.tolist():
                            rename['approx_value'] = 'value'

                    df = df.rename(index=str, columns=rename)
                    if 'block_timestamp' in df.columns.tolist():
                        min = df.block_timestamp.min()
                        max = df.block_timestamp.max()
                        min = datetime.fromtimestamp(min)
                        max = datetime.fromtimestamp(max)
                        #logger.warning('data loaded from mysql start:end=%s:%s',min,max)
                    # convert to dask
                    #logger.warning("%s data loaded from mysql:%s",table.upper(),df.columns.tolist())

                    if type == 'dask':
                        df = dd.dataframe.from_pandas(df, npartitions=5)
                        if 'block_timestamp' in df.columns.tolist():
                            df['block_timestamp'] = df['block_timestamp'].map(self.int_to_date)
                    #logger.warning("%s length data loaded from mysql:%s",table,len(df))
            return df

        except Exception:
            logger.error('mysql load data :%s', exc_info=True)