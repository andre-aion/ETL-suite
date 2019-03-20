import gc
import json
import gzip
import urllib

from config.checkpoint import checkpoint_dict
from scripts.github_and_bsscraper_interface import Scraper
from scripts.utils.mylogger import mylogger
from pandas.io.json import json_normalize
from datetime import datetime, timedelta
from iteration_utilities import deepflatten

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

        self.day_data = {} # aggregate data over a day

    # /////////////////////////////////////////////////////////////////////
    # ////////////          UTILS
    # function to return key for any value
    def get_key(self, val):
        for key, value in self.month_end_day_dict.items():
            if val in value:
                return key
        return None

    def get_date_data_from_mongo(self,date):
        try:
            if isinstance(date,str):
                date = datetime.strptime(date, self.DATEFORMAT)

            result = self.pym.db[self.table].find(
                {
                    'date': {'$eq':date},
                }
            ).limit(1)
            if result.count() > 0:
                for res in result:
                    return res
            else:
                return None
        except Exception:
            logger.error("processed hours", exc_info=True)

    def process_item(self, item, item_name):
        try:
            # logger.warning('item before save:%s',item)
            for col in list(item.keys()):
                # logger.warning('col:%s', col)
                if col not in ['date', 'processed_hours']:
                    #logger.warning('col:%s',col)
                    nested_search = item_name+'.'+col
                    self.pym.db[self.collection].update_one(
                        {'date': item['date']},
                        {'$set':
                            {
                                nested_search: item[col]
                            }
                        },
                        upsert=True)
            nested_search = item_name+'.processed_hours'
            self.pym.db[self.collection].update(
                {'date': item['date']},
                {'$addToSet':
                    {
                        nested_search: item['processed_hours']
                    }
                })

            #logger.warning("%s item added to MongoDB database!", format(self.item_name))
        except Exception:
            logger.error('process item', exc_info=True)

    # ///////////////////    UTILS END       /////////////////////////////////////////////////
    def decompress(self, response):
        try:
            data = gzip.decompress(response.read())
            data = data.decode('utf8').replace("'{", '{')
            data = data.replace("}'", '}')

            data = data.splitlines()
            json_data = []
            for item in data:
                try:
                    if item[0] == '{' and item[-1] == '}':
                        json_data.append(json.loads(item))
                except:
                    logger.warning('string index out of range')

            df = json_normalize(json_data)
            del data
            del json_data
            gc.collect()
            # Load the JSON to a Python list & dump it back out as formatted JSON
            df = df[['repo.name', 'repo.url', 'type']]
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
                    DATEFORMAT_created_at = "%Y-%m-%dT%H:%M:%SZ"

                    to_lower_lst = df.columns.tolist()
                    for col in to_lower_lst:
                        df[col].str.lower()
                    df = df[(df['repo.name'].str.contains(self.cryptos_pattern)) |
                            (df['repo.url'].str.contains(self.cryptos_pattern))]

                    if len(df) > 0:
                        df = df[df.type.str.contains(self.events_pattern)]

                    logger.warning('FILTERING COMPLETE')
                    gc.collect()
                    return df
        except Exception:
            logger.error('filter df', exc_info=True)

    def log_occurrences(self, df, date, hour):
        try:
            if df is not None:
                if len(df) > 0:
                    # make item

                    self.get_checkpoint_dict()
                    for item_name in self.items:
                        item = {
                            'date': date,
                        }
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
                                #logger.warning("coin:event=%s:%s",item_name,column_name)
                            item['processed_hours'] = self.checkpoint_dict['items'][item_name]['processed_hours']
                            self.aggregate_data(item, date, hour, item_name)

                        else:
                            logger.warning('ALREADY LOGGED - %s, hour:%s',item_name,hour)
                    del df
                    gc.collect()

        except Exception:
            logger.error('count occurrences', exc_info=True)


    # aggregate 24 hours of data,
    def aggregate_data(self, item, date, hour, item_name):
        try:
            # find if item exists in mongo for a particular date
            res = self.item_in_mongo(item_name,date)
            if res is not None:
                processed_hours = list(deepflatten(res['processed_hours'],depth=1))
                # do not duplicate
                if len(processed_hours) < 24 and hour not in processed_hours:
                    temp_item = {'date': item['date']}
                    processed_hours.append(hour)
                    self.checkpoint_dict['items'][item_name]['processed_hours'] = processed_hours
                    temp_item['processed_hours'] = hour
                    for col in item.keys():
                        if col not in ['date', 'processed_hours']:
                            temp_item[col] = res[col] + item[col]
                    logger.warning('aggregate updated, hour %s', hour)
            else:
                temp_item = item
                temp_item['processed_hours']= hour
                self.checkpoint_dict['items'][item_name]['processed_hours'] = [hour]

            #logger.warning('%s hour:%s saved, %s',item_name,hour, temp_item)
            self.process_item(temp_item, item_name)  # save the data
            self.save_checkpoint()
            del res
            del item
            del temp_item
            gc.collect()
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


    def hour_to_process(self,offset):
        try:
            result = self.get_date_data_from_mongo(offset)
            if result is not None:
                #logger.warning('res:%s',result)
                max_processed_hours = []
                for item_name in self.items:
                    if len(result[item_name]['processed_hours']) == 0:
                        return -1
                    max_processed_hours.append(max(list(deepflatten(result[item_name]['processed_hours']))))
                    max_processed_hours = list(set(max_processed_hours))

                # find mininum of max loaded numbers
                if len(max_processed_hours) > 0:
                    return min(max_processed_hours)
                else:
                    return -1
            else: # if no data for date in table
                return -1
        except Exception:
            logger.warning('decide start hour',exc_info=True)

    def determine_url(self, offset):
        try:
            # determine the start hour, and the start date
            offset_increment_tracker = 0
            max_processed_hour = self.hour_to_process(offset) # determine which hour to start
            logger.warning('max processed hour:%s',max_processed_hour)

            # RESET REDIS CHECKPOINT IF NEEDED
            hour = max_processed_hour + 1 # increment hour to consider
            if max_processed_hour >= 23: # all 24 hours for all coins have been loaded
                offset = offset + timedelta(days=1)
                hour = 0
                # reset self.checkpoint dict for new day
                for item_name in self.items:
                    self.checkpoint_dict['items'][item_name]['offset'] = datetime.strftime(offset, self.DATEFORMAT)
                    self.checkpoint_dict['items'][item_name]['timestamp'] = datetime.now().strftime(self.DATEFORMAT)
                    self.checkpoint_dict['items'][item_name]['processed_hours'] = []
                self.save_checkpoint()

            logger.warning('hour tracker:offset_increment_tracker= %s:%s',hour,offset)


            # MAKE URL
            month = str(offset.month).zfill(2)
            year = offset.year
            day = str(offset.day).zfill(2)
            hour_str = str(hour)
            url = 'http://data.gharchive.org/{}-{}-{}-{}.json.gz'.format(year, month, day, hour_str)
            logger.warning('URL: %s',url)

            return url, offset, hour
        except Exception:
            logger.warning('',exc_info=True)

    def run_process(self, url):
        try:
            data = self.load_url(url)
            df = self.decompress(data)
            df = self.filter_df(df)
            del data
            gc.collect()
            return df
        except Exception:
            logger.warning('run process',exc_info=True)


    async def update(self):
        try:
            offset = datetime.strptime(self.checkpoint_dict['items']['aion']['offset'],self.DATEFORMAT)
            logger.warning('offset:%s',offset)
            # reset the date and processed hour tracker in redis if all items, all hours are don
            url, offset, hour = self.determine_url(offset)
            df = self.run_process(url)
            self.log_occurrences(df, offset, hour)
            del df
            gc.collect()
        except Exception:
            logger.error('github interface run', exc_info=True)
