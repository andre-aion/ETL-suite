import asyncio
import gc
import json
import ujson
import gzip
import urllib

from config.checkpoint import checkpoint_dict
from scripts.scrapers.beautiful_soup.bs_scraper_interface import Scraper
from scripts.utils.mylogger import mylogger
from scripts.ETL.checkpoint import Checkpoint
import pandas as pd
from pandas.io.json import json_normalize
from datetime import datetime, timedelta, date
import concurrent.futures, threading

logger = mylogger(__file__)

class GithubLoader(Scraper):

    def __init__(self, items):
        Scraper.__init__(self)
        self.data = None
        self.items = items
        self.events = ['ReleaseEvent', 'PushEvent', 'WatchEvent', 'ForkEvent', 'IssueEvent']
        self.cryptos_pattern = '|'.join(self.items)
        self.events_pattern = '|'.join(self.events)
        self.initial_date = datetime.strptime("2018-04-25 00:00:00", self.DATEFORMAT)
        self.is_up_to_date_window = 24  # hours to sleep to give reorg time to happen
        self.window = 24

        # checkpointing
        self.checkpoint_column = 'aion_fork'
        self.checkpoint_append = '_fork'
        self.checkpoint_key = 'githubloader'
        self.key_params = 'checkpoint:' + self.checkpoint_key
        self.dct = checkpoint_dict[self.checkpoint_key]
        self.offset = self.initial_date

        self.scraper_name = 'githubloader'
        self.item_name = 'aion'

        self.item_aggregator = {} # aggregate data over a day

    # /////////////////////////////////////////////////////////////////////
    # ////////////          UTILS
    # function to return key for any value
    def get_key(self, val):
        for key, value in self.month_end_day_dict.items():
            if val in value:
                return key
        return None

    # ///////////////////    UTILS END       /////////////////////////////////////////////////

    def decompress(self, response):
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
            df = df[['repo.name', 'repo.url', 'type', 'created_at']]
            logger.warning('flattened columns:%s', df.columns.tolist())
            #logger.warning('df:%s', df.head(30))
            return df
        except Exception:
            logger.error('decompress', exc_info=True)

    def filter_df(self, df):
        try:
            #logger.warning(self.cryptos_pattern)
            if df is not None:
                if len(df) > 0:
                    timestamp = df['created_at'].min()
                    DATEFORMAT = "%Y-%m-%dT%H:%M:%SZ"

                    hour = datetime.strptime(timestamp, DATEFORMAT).hour
                    df = df.drop(columns=['created_at'])
                    to_lower_lst = df.columns.tolist()
                    for col in to_lower_lst:
                        df[col].str.lower()
                    df = df[(df['repo.name'].str.contains(self.cryptos_pattern)) |
                            (df['repo.url'].str.contains(self.cryptos_pattern))]

                    if len(df) > 0:
                        df = df[df.type.str.contains(self.events_pattern)]

                    logger.warning('FILTERING COMPLETE')
                    gc.collect()
                    return df, hour
        except Exception:
            logger.error('filter df', exc_info=True)

    def log_occurrences(self, df, date, hour):
        try:
            if df is not None:
                if len(df) > 0:
                    # make item
                    item = {
                        'date': datetime.strftime(date,self.DATEFORMAT)
                    }
                    self.get_checkpoint_dict()

                    for item_name in self.items:
                        # do not run loop if the offset date is later than current date
                        item_offset = datetime.strptime(self.checkpoint_dict['items'][item_name]['offset'],self.DATEFORMAT)
                        if item_offset <= date and hour not in self.checkpoint_dict['items'][item_name]['processed_hours']:
                            for event in self.events:
                                df_temp = df[(df.type.str.contains(event)) &
                                             ((df['repo.name'].str.contains(item_name)) |
                                              (df['repo.url'].str.contains(item_name)))]
                                event_truncated = event[:-5]
                                column_name = '{}_{}'.format(item_name, event_truncated.lower())
                                if len(df_temp) > 0:
                                    item[column_name] = len(df_temp)
                                else:
                                    item[column_name] = 0
                            item['processed_hours'] = self.checkpoint_dict['items'][item_name]['processed_hours']
                            logger.warning(item)
                            self.aggregate_data(item, date, hour, item_name)
                        else:
                            logger.warning('ALREADY LOGGED - %s, hour:%s',item_name,hour)

        except Exception:
            logger.error('count occurrences', exc_info=True)


    # aggregate 24 hours of data,
    def aggregate_data(self, item, date, hour, item_name):
        try:
            # find if item exists in mongo for a particular date
            res = self.item_in_mongo(item_name,date)
            if res is not None:
                processed_hours = res['processed_hours']
                if len(processed_hours) < 24:
                    temp_item = {'date': item['date']}
                    processed_hours.append(hour)
                    self.checkpoint_dict['items'][item_name]['processed_hours'] = processed_hours
                    temp_item['processed_hours'] = processed_hours
                    for col in item.keys():
                        if col not in ['date', 'processed_hours']:
                            temp_item[col] = temp_item[col] + item[col]
                    logger.warning('aggregate updated, hour %s', hour)
            else:
                temp_item = item
                temp_item['processed_hours']=[hour]
                self.checkpoint_dict['items'][item_name]['processed_hours'] = [hour]
                logger.warning('new aggregator started for %s',item_name)

            self.process_item(temp_item, item_name)  # save the data
            logger.warning('%s hour:%s saved',item_name,hour)
            self.save_checkpoint()
        except Exception:
            logger.error('aggregate data', exc_info=True)


    def load_url(self, url):
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

    def run_process(self, url):
        data = self.load_url(url)
        df = self.decompress(data)
        df, hour = self.filter_df(df)
        return df, hour


    async def update(self):
        try:
            offset = datetime.strptime(self.checkpoint_dict['items']['aion']['offset'],self.DATEFORMAT)
            # reset the date and processed hour tracker in redis if all items, all hours are don

            counter = 0
            # only pogress updater if all hours have been completed
            min_hours = []
            for item_name in self.items:
                hours_completed = len(self.checkpoint_dict['items'][item_name]['processed_hours'])
                if hours_completed > 0:
                    min_hours.append(min(self.checkpoint_dict['items'][item_name]['processed_hours']))
                else:
                    break

                if hours_completed >= 24:
                    counter += 1
                else:
                    break

            start = 0
            this_date = offset
            if counter == len(self.items): # if all hours at offset have been completed
                this_date = offset + timedelta(days=1)
                logger.warning("OFFSET INCREASED:%s", this_date)
                for item_name in self.items: # reset the hour tracker
                    self.checkpoint_dict['items'][item_name]['processed_hours'] = []
            else:
                if len(min_hours) > 0:
                    start = min(min_hours)


            month = str(this_date.month).zfill(2)
            year = this_date.year
            day = str(this_date.day).zfill(2)
            # make four threads
            url_parallel_count = 0

            while start + url_parallel_count <= 24:
                URLS = []
                for hour in list(range(start, start + url_parallel_count + 1)):
                    if hour < 23:
                        hour_str = str(hour)
                        search_str = 'http://data.gharchive.org/{}-{}-{}-{}.json.gz' \
                            .format(year, month, day, hour_str)
                        URLS.append(search_str)
                start = start + url_parallel_count + 3
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
                    logger.error('batch load failed', exc_info=True)

        except Exception:
            logger.error('github interface run', exc_info=True)
