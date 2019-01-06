from scripts.utils.mylogger import mylogger
from scripts.utils.pythonRedis import LoadType, RedisStorage
from scripts.streaming.streamingDataframe import StreamingDataframe as SD
from config import columns, dedup_cols

import pandas as pd
from os.path import join, dirname
from pandas.api.types import is_string_dtype
from datetime import datetime, date
import dask as dd
from bokeh.models import Panel
from bokeh.models.widgets import Div
import numpy as np
from tornado.gen import coroutine
import gc
import calendar
from time import mktime

logger = mylogger(__file__)


def mem_usage(pandas_obj):
    if isinstance(pandas_obj,pd.DataFrame):
        usage_b = pandas_obj.memory_usage(deep=True).sum()
    else: # we assume if not a df it's a series
        usage_b = pandas_obj.memory_usage(deep=True)
    usage_mb = usage_b / 1024 ** 2 # convert bytes to megabytes
    return "{:03.2f} MB".format(usage_mb)

def optimize_dataframe(df,timestamp_col='block_timestamp'):
    dtypes = df.drop(timestamp_col, axis=1).dtypes
    dtypes_col = dtypes.index
    dtypes_type = [i.name for i in dtypes.values]
    column_types = dict(zip(dtypes_col, dtypes_type))

    df_read_and_optimized = pd.read_csv(join(dirname(__file__),
                                             '../../data', 'blockdetails.csv'),
                                             dtype=column_types, parse_dates=['block_timestamp'],
                                             infer_datetime_format=True)

    return df_read_and_optimized


def convert_block_timestamp_from_string(df,col):
    if is_string_dtype(df[col]):
        df[col] = df[col].apply(int)
        df[col] = pd.Timestamp(df[col])
    return df


def setdatetimeindex(df):
    # set timestamp as index
    meta = ('block_timestamp', 'datetime64[ns]')
    df['block_timestamp'] = df['block_timestamp']
    df['block_timestamp'] = df.block_timestamp\
        .map_partitions(pd.to_datetime, unit='s',
                                        format="%Y-%m-%d %H:%M:%S",
                                        meta=meta)
    df = df.set_index('block_timestamp')
    return df


def get_breakdown_from_timestamp(ts):
    ns = 1e-6
    mydate = datetime.fromtimestamp(ts).date()
    return mydate

def get_initial_blocks(pc):
    try:
        to_check = tuple(range(0, 50000))
        qry ="""SELECT block_number, difficulty, block_date, 
            block_time, miner_addr FROM block_old
            WHERE block_number in """+str(to_check)

        df = pd.DataFrame(list(pc.session.execute(qry)))
        df = dd.dataframe.from_pandas(df, npartitions=15)
        #logger.warning('from get initial block: %s',df.head(5))
        return df
    except Exception:
        logger.error('get initial blocks',exc_info=True)


def timestamp_to_datetime(ts):
    return datetime.fromtimestamp(ts)


# when a tab does not work
def tab_error_flag(tabname):
    print('IN POOLMINER')

    # Make a tab with the layout
    div = Div(text="""ERROR CREATING POOLMINER TAB, 
    CHECK THE LOGS""",
              width=200, height=100)

    tab = Panel(child=div, title=tabname)

    return tab


# convert dates from timestamp[ms] to datetime[ns]
def ms_to_date(ts):
    try:
        if isinstance(ts, int) == True:
            # change milli to seconds
            if ts > 16307632000:
                ts = ts // 1000
            ts = datetime.utcfromtimestamp(ts)
            # convert to nanosecond representation
            ts = np.datetime64(ts).astype(datetime)
            ts = pd.Timestamp(datetime.date(ts))

            logger.warning('from ms_to_date: %s',ts)
        return ts
    except Exception:
        logger.error('ms_to_date', exc_info=True)
        return ts


# nano_secs_to_date
def ns_to_date(ts):
    ns = 1e-9
    try:
        ts = datetime.utcfromtimestamp(ts * ns)
        ts = pd.Timestamp(datetime.date(ts))
        return ts
    except Exception:
        logger.error('ns_to_date', exc_info=True)
        return ts

# date time to ms
def date_to_ms(ts):
    if isinstance(ts, str):
        ts = datetime.strptime(ts, '%Y-%m-%d')

    ts = int(ts.timestamp())
    return ts

# convert date format for building cassandra queries
def date_to_cass_ts(ts):
    logger.warning('date_to_cass_ts:%s', ts)
    if isinstance(ts, str):
        ts = datetime.strptime(ts,'%Y-%m-%d')
        ts = int(ts.timestamp()*1000)
    elif isinstance(ts,datetime):
        #ts = pd.Timestamp(ts, unit='ns')
        ts = int(mktime(ts.timetuple()) * 1000)
    logger.warning('date_to_cass_ts:%s', ts)
    return ts


#convert ms to string date
def slider_ts_to_str(ts):
    # convert to datetime if necessary
    if isinstance(ts,int) == True:
        ts = ms_to_date(ts)

    ts = datetime.strftime(ts,'%Y-%m-%d')
    return ts


# cols are a list
def construct_read_query(table, cols, startdate, enddate):
    qry = 'select '
    if len(cols) >= 1:
        for pos, col in enumerate(cols):
            if pos > 0:
                qry += ','
            qry += col
    else:
        qry += '*'

    qry += """ from {} where block_timestamp >={} and 
        block_timestamp <={} ALLOW FILTERING"""\
        .format(table, startdate, enddate)

    logger.warning('query:%s',qry)
    return qry

# dates are in milliseconds from sliders
def cass_load_from_daterange(pc, table, cols, from_date, to_date):
    logger.warning('cass load from_date:%s', from_date)
    logger.warning('cass load to_date:%s', to_date)

    try:
        if isinstance(from_date,int) == True:
            # convert ms from slider to nano for cassandra
            if from_date < 16307632000:
                from_date = from_date * 1000
                to_date = to_date * 1000
        elif isinstance(from_date,str) == True:
            # convert from datetime to ns
            from_date = date_to_cass_ts(from_date)
            to_date = date_to_cass_ts(to_date)

        # construct query
        qry = construct_read_query(table, cols,
                                   from_date,
                                   to_date)
        df = pd.DataFrame(list(pc.session.execute(qry)))
        df = dd.dataframe.from_pandas(df, npartitions=15)
        logger.warning('data loaded from daterange :%s', df.tail(5))
        return df

    except Exception:
        logger.error('cass load from daterange:%s', exc_info=True)

# check to see if the current data is within the active dataset
def set_params_to_load(df, start_date, end_date):
    try:
        params = dict()
        params['start'] = False
        params['min_date'] = None
        params['end'] = False
        params['max_date'] = None
        # convert dates from ms to datetime
        # start_date = ms_to_date(start_date)
        #end_date = ms_to_date(end_date)

        if len(df) > 0:
            params['min_date'], params['max_date'] = \
                dd.compute(df.block_date.min(), df.block_date.max())
            # check start
            logger.warning('start_date from compute:%s', params['min_date'])
            logger.warning('start from slider:%s', start_date)
            if isinstance(start_date, int):
                start_date = ms_to_date(start_date)
                end_date = ms_to_date(end_date)

            if start_date > params['min_date']:
                    params['start'] = True
            if end_date > params['max_date']:
                    params['end'] = True

            logger.warning('set_params_to_load:%s', params)

        else:
            # if no df in memory set start date and end_date far in the past
            # this will trigger full cassandra load
            params['min_date'] = datetime.strptime('2010-01-01','%Y-%m-%d')
            params['max_date'] = datetime.strptime('2010-01-02','%Y-%m-%d')
            params['start'] = True
            params['end'] = True

        return params
    except Exception:
        logger.error('set_params_loaded_params', exc_info=True)
        return params

# delta is integer: +-
def get_relative_day(day,delta):
    if isinstance(day,str):
        day = datetime.strptime('%Y-%m-%d')
    elif isinstance(day,int):
        day = ms_to_date()
    day = day + datetime.timedelta(days=delta)
    day = datetime.strftime(day, '%Y-%m-%d')
    return day


# get the data differential from the required start range
def construct_df_upon_load(pc, df, table, cols, req_start_date,
                           req_end_date, load_params):
    if len(df) > 0:
        logger.warning("df original, HEAD:%s", df.head())
        logger.warning("df original, TAIL:%s", df.tail())

    try:
        redis = RedisStorage()
        # get the data parameters to determine from whence to load
        params = redis.set_load_params(table, req_start_date,
                                       req_end_date, load_params)
        # load all from redis
        logger.warning('construct df, params:%s', params)
        if params['load_type'] & LoadType.REDIS_FULL.value == LoadType.REDIS_FULL.value:
            lst = params['redis_key_full'].split(':')
            sdate = date_to_ms(lst[1])
            edate = date_to_ms(lst[2])
            df = redis.load_df(params['redis_key_full'], table, sdate, edate)
            logger.warning('')
        # load all from cassandra
        elif params['load_type'] & LoadType.CASS_FULL.value == LoadType.CASS_FULL.value:
            sdate = date_to_cass_ts(req_start_date)
            edate = date_to_cass_ts(req_end_date)
            logger.warning('construct_df, in cass load, sdate:%s',sdate)
            df = cass_load_from_daterange(pc, table, cols, sdate, edate)

        # load from both cassandra and redis
        else:
            # load start
            streaming_dataframe = SD(table, columns, dedup_cols)
            df_start = streaming_dataframe.get_df()
            df_end = streaming_dataframe.get_df()
            df_temp = None

            # add cass if needed, then add redis if needed
            if params['load_type'] & LoadType.START_CASS.value == LoadType.START_CASS.value:
                lst = params['cass_start_range']
                sdate = date_to_ms(lst[0])
                edate = date_to_ms(lst[1])

                df_temp = cass_load_from_daterange(pc, table, cols, sdate, edate)
                df_start = df_start.append(df_temp)

            if params['load_type'] & LoadType.REDIS_START.value == LoadType.REDIS_START.value:
                lst = params['redis_start_range']
                sdate = date_to_ms(lst[0])
                edate = date_to_ms(lst[1])
                df_temp = redis.load_df(params['redis_key_start'], table, sdate, edate)
                df_start = df_start.append(df_temp)


            # load end, add redis df, then cass df if needed
            if params['load_type'] & LoadType.REDIS_END.value == LoadType.REDIS_END.value:
                lst = params['redis_end_range']
                sdate = date_to_ms(lst[0])
                edate = date_to_ms(lst[1])
                df_temp = redis.load_df(params['redis_key_end'], table, sdate, edate)
                df_end = df_end.append(df_temp)

            if params['load_type'] & LoadType.CASS_END.value == LoadType.CASS_END.value:
                lst = params['cass_end_range']
                sdate = date_to_ms(lst[0])
                edate = date_to_ms(lst[1])
                df_temp = cass_load_from_daterange(pc, table, cols, sdate, edate)
                df_end = df_end.append(df_temp)

            # concatenate end and start to original df
            if len(df_start>0):
                df = df_start.append(df).reset_index()
            if len(df_end>0):
                df = df.append(df_end).reset_index()

            del df_temp
            del df_start
            del df_end

            gc.collect()

        logger.warning("df constructed, HEAD:%s", df.head())
        logger.warning("df constructed, TAIL:%s", df.tail())

        # save df to  redis
        # clean up by deleting any dfs in redis smaller than the one we just saved
        """
        redis_df      || ---------------- ||
        required  |---------------------------- |

        """
        for key in params['redis_keys_to_delete']:
            redis.conn.delete(key)
            logger.warning('bigger df added so deleted key:%s',
                           str(key, 'utf-8'))

        # save (including overwrite to redis)
        redis.save_df(df, table, req_start_date, req_end_date)

        gc.collect()
        return df

    except Exception:
        logger.error('construct df from load', exc_info=True)