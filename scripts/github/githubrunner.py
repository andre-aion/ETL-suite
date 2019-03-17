import asyncio
import gc
import json
import ujson
import gzip
import urllib
from pprint import pprint
import yaml

from config.checkpoint import checkpoint_dict
from scripts.utils.mylogger import mylogger
from scripts.ETL.checkpoint import Checkpoint
import pandas as pd
from pandas.io.json import json_normalize
from requests import request
from ast import literal_eval
from itertools import chain
from datetime import datetime, timedelta, date
import concurrent.futures, threading
import os

logger = mylogger(__file__)

class GithubLoader(Checkpoint):
    month_end_day_dict = {
        '30' : ['09','04','06','11'],
        '28' : ['02'],
        '31':['01','03','05','07','08','10','12']
    }
    SLEEP = 300
    def __init__(self):
        Checkpoint.__init__(self,'external')
        self.data = None
        self.cryptos = ['aion','cardano','bitcoin','ethereum']
        self.events = ['ReleaseEvent','PushEvent','WatchEvent','ForkEvent','IssueEvent']
        self.cryptos_pattern = '|'.join(self.cryptos)
        self.events_pattern = '|'.join(self.events)
        self.initial_date = datetime.strptime("2018-04-23 00:00:00",self.DATEFORMAT)
        self.is_up_to_date_window = 24  # hours to sleep to give reorg time to happen
        self.window = 24
        # save daily data
        self.item = {}
        self.item_tracker = []

        # checkpointing
        self.checkpoint_column = 'aion_fork'
        self.checkpoint_key = 'githubloader'
        self.key_params = 'checkpoint:' + self.checkpoint_key
        self.dct = checkpoint_dict[self.checkpoint_key]


    # /////////////////////////////////////////////////////////////////////
    # ////////////          UTILS
    # function to return key for any value
    def get_key(self,val):
        for key, value in self.month_end_day_dict.items():
            if val in value:
                return key
        return None

    def get_offset(self):
        try:
            self.get_checkpoint_dict()
            # handle reset or initialization
            if self.checkpoint_dict['offset'] is None:
                self.checkpoint_dict['offset'] = self.get_value_from_mongo(self.table, min_max='MAX')
                if self.checkpoint_dict['offset'] is None:
                    self.checkpoint_dict['offset'] = self.initial_date

            # convert offset to datetime if needed
            if isinstance(self.checkpoint_dict['offset'], str):
                self.checkpoint_dict['offset'] = datetime.strptime(self.checkpoint_dict['offset'],
                                                                   self.DATEFORMAT)

            # SET THE DATE
            # ensure date fits mongo scheme
            if isinstance(self.checkpoint_dict['offset'], date):
                self.checkpoint_dict['offset'] = datetime.combine(self.checkpoint_dict['offset'], datetime.min.time())
            if isinstance(self.checkpoint_dict['offset'], datetime):
                self.checkpoint_dict['offset'] = datetime.combine(self.checkpoint_dict['offset'].date(), datetime.min.time())

            return self.checkpoint_dict['offset']

        except Exception:
            logger.error('get offset', exc_info=True)

    # ///////////////////    UTILS END       /////////////////////////////////////////////////

    def decompress(self,response):
        try:
            data = gzip.decompress(response.read())
            data = data.decode('utf8').replace("'{", '{')
            data = data.replace("}'", '}')

            data = data.splitlines()
            json_data = []
            for item in data:
                if item[0] == '{' and item[-1] == '}':
                    json_data.append(json.loads(item))
            del data
            gc.collect()
            df = json_normalize(json_data)
            # Load the JSON to a Python list & dump it back out as formatted JSON
            df = df[['repo.name', 'repo.url', 'type','created_at']]
            logger.warning('flattened columns:%s', df.columns.tolist())
            #logger.warning('df:%s', df.head(30))
            return df
        except Exception:
            logger.error('decompress',exc_info=True)

    def filter_df(self, df):
        try:
            if df is not None:
                if len(df) > 0:
                    timestamp = df['created_at'].min()
                    DATEFORMAT = "%Y-%m-%dT%H:%M:%SZ"

                    hour = datetime.strptime(timestamp,DATEFORMAT).hour
                    df = df.drop(columns=['created_at'])
                    to_lower_lst = df.columns.tolist()
                    for col in to_lower_lst:
                        df[col].str.lower()
                    df = df[(df['repo.name'].str.contains(self.cryptos_pattern)) |
                            (df['repo.url'].str.contains(self.cryptos_pattern))]
                    if len(df) > 0:
                        df = df[df.type.str.contains(self.events_pattern)]
                    #logger.warning('FILTERING COMPLETE')
                    gc.collect()
                    return df,hour
        except Exception:
            logger.error('filter df',exc_info=True)

    def log_occurrences(self, df, date, hour):
        try:
            if df is not None:
                if len(df) > 0:
                    # make item
                    item = {
                        'date':date
                    }
                    for crypto in self.cryptos:
                        for event in self.events:
                            df_temp = df[(df.type.str.contains(event)) & (df['repo.name'].str.contains(crypto))]
                            event_truncated = event[:-5]
                            column_name = '{}_{}'.format(crypto,event_truncated.lower())
                            if len(df_temp)>0:
                                item[column_name] = len(df_temp)
                            else:
                                item[column_name] = 0
                    self.aggregate_data(item,hour)
                    #logger.warning('hour %s item to aggregate:%s',hour,item)

        except Exception:
            logger.error('count occurrences', exc_info=True)

    # aggregate 24 hours of data,
    def aggregate_data(self,item, hour):
        try:
            # ensure that 24 hours are logged uniquely
            if hour not in self.item_tracker:
                # process the hour
                if len(self.item_tracker) == 0:
                    self.item = item
                    logger.warning('SELF.ITEM SET TO ITEM:%s', self.item)
                    logger.warning('new day aggregate started')
                else:
                    for col in item.keys():
                        if col != 'date':
                            self.item[col] = self.item[col] + item[col]
                    logger.warning('aggregate updated, hour %s', hour)

                self.item_tracker.append(hour)
                if len(self.item_tracker) >= 24:
                    self.item_tracker = [] # reset the tracker
                    self.process_item(self.item) # save the data
                    self.item = {}
            else:
                logger.warning('HOUR %s ALREADY AGGREGATED',hour)
        except Exception:
            logger.error('count occurrences', exc_info=True)

    def process_item(self, item):
        try:
            for col in item.keys():
                if col != 'date':
                    self.pym.db[self.table].update_one(
                        {'date': item['date']},
                        {'$set':
                            {
                                col: item[col],
                            }
                        },
                        upsert=True)
            self.save_checkpoint()
            logger.warning("%s item added to MongoDB database!",format(item))
        except Exception:
            logger.error('process item', exc_info=True)

    def load_url(self,url):
        try:
            request = urllib.request.Request(
                url,
                headers={
                    "Accept-Encoding": "gzip",
                    "User-Agent": "Mozilla/5.0 (X11; U; Linux i686) Gecko/20071127 Firefox/2.0.0.11",
                })
            response = urllib.request.urlopen(request)
            return response
        except Exception:
            logger.error('read requests', exc_info=True)

    def run_process(self,url):
        data = self.load_url(url)
        df = self.decompress(data)
        df, hour = self.filter_df(df)
        return df, hour

    async def update(self):
        try:
            offset = self.get_offset()
            this_date = offset+timedelta(days=1)
            logger.warning("OFFSET INCREASED:%s",this_date)

            self.update_checkpoint_dict(this_date)
            month = str(this_date.month).zfill(2)
            year = this_date.year
            day = str(this_date.day).zfill(2)
            # make four threads
            start = 0
            url_parallel_count = 3
            while start + url_parallel_count <= 24:
                URLS = []
                for hour in list(range(start,start+url_parallel_count+1)):
                    hour_str = str(hour)
                    search_str = 'http://data.gharchive.org/{}-{}-{}-{}.json.gz'\
                        .format(year,month,day,hour_str)
                    URLS.append(search_str)
                start = start+ url_parallel_count + 1
                logger.warning(URLS)
                try:
                    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
                        # Start the load operations and mark each future with its
                        '''
                        future_to_url = {executor.submit(self.load_url, url): url for url in URLS}
                        
                        for future in concurrent.futures.as_completed(future_to_url):
                            url = future_to_url[future]
                            try:
                                data = future.result()
                                
                                df = self.decompress(data)
                                if len(df) > 0:
                                    df,hour = self.filter_df(df)
                                if len(df) > 0:
                                    self.log_occurrences(df, this_date, hour)
                                

                            except Exception as exc:
                                logger.warning('%r generated an exception: %s' % (url, exc))
                            else:
                                logger.warning('%r page is %d bytes' % (url, len(data.read())))
                        '''
                        future_to_url = {executor.submit(self.run_process, url): url for url in URLS}
                        for future in concurrent.futures.as_completed(future_to_url):
                            url = future_to_url[future]
                            try:
                                df, hour = future.result()
                                self.log_occurrences(df, this_date, hour)
                            except Exception as exc:
                                logger.warning('%r generated an exception: %s' % (url, exc))
                            else:
                                logger.warning('%r hour completed' % (url))

                except Exception:
                    logger.error('batch load failed',exc_info=True)

        except Exception:
            logger.error('github interface run',exc_info=True)

    async def run(self):
        # create warehouse table in clickhouse if needed
        # self.create_table_in_clickhouse()
        while True:
            await self.update()
            if self.is_up_to_date(construct_table='external',
                                  storage_medium='mongo',
                                  window_hours=self.window):
                logger.warning("ACCOUNT ACTIVITY SLEEPING FOR 24 hours:UP TO DATE")
                await asyncio.sleep(self.window*60*60)  # sleep
            else:
                await asyncio.sleep(1)