import asyncio

from selenium import webdriver

from scripts.utils.mylogger import mylogger
from scripts.storage.pythonMongo import PythonMongo
from selenium.webdriver.common.proxy import Proxy, ProxyType
from scripts.ETL.checkpoint import Checkpoint
from datetime import datetime, date, timedelta
from scripts.scrapers.utils import get_random_scraper_data
from config.checkpoint import checkpoint_dict


logger = mylogger(__file__)
WEBDRIVER_PATH='/usr/local/bin/chromedriver'
DBUS_SESSION_BUS_ADDRESS= '/dev/null'
WINDOW_SIZE = "1920,1080"


class Scraper(Checkpoint):
    def __init__(self,collection='external_daily'):
        Checkpoint.__init__(self,collection)
        data = get_random_scraper_data()
        self.proxy = Proxy({
            'proxyType': ProxyType.MANUAL,
            'httpProxy': data['proxy'],
            'ftpProxy': data['proxy'],
            'sslProxy': data['proxy'],
            'noProxy': ''  # set this value as desired
        })
        self.url = ''
        self.scrape_period = 'daily'
        self.item_name = 'aion'
        self.window = 24
        self.is_up_to_date_window = self.window
        self.items_updated = []
        self.offset = self.initial_date
        self.pym = PythonMongo('aion')

        # create a new firefox session
        self.options = webdriver.FirefoxOptions()
        self.options.add_argument('-headless')
        self.firefox_profile = webdriver.FirefoxProfile()
        self.firefox_profile.set_preference('permissions.default.image', 2)
        self.firefox_profile.set_preference('dom.ipc.plugins.enabled.libflashplayer.so', False)
        self.firefox_profile.set_preference("network.cookie.cookieBehavior", 2)
        self.driver = webdriver.Firefox(proxy=self.proxy, firefox_profile=self.firefox_profile,
                                        firefox_options=self.options)
        self.scraper = ''
        self.collection = collection
        self.table = collection
        self.items = []

    def process_item(self,item, item_name):
        try:
            if isinstance(item['timestamp'],str):
                item['timestamp'] = datetime.strptime(item['timestamp'], self.DATEFORMAT)
            #logger.warning('item before save:%s',item)
            for col in list(item.keys()):
                #logger.warning('col:%s', col)
                if col != 'timestamp':
                    if col in ['month','day','year','hour']:
                        nested_search = col
                    else:
                        nested_search = item_name+'.'+col
                    self.pym.db[self.collection].update_one(
                        {'timestamp': item['timestamp']},
                        {'$set':
                             {
                               nested_search:item[col]
                             }
                        },
                        upsert=True)

            #logger.warning("%s item added to MongoDB database!",format(self.item_name))
        except Exception:
            logger.error('process item', exc_info=True)

    def start_firefox_driver(self):
        try:
            self.driver = webdriver.Firefox(proxy=self.proxy, firefox_profile=self.firefox_profile,
                                            firefox_options=self.options)
        except Exception:
            logger.error('start firefox driver',exc_info=True)

    def set_scrape_period(self,offset):
        try:
            today = datetime.combine(datetime.today().date(),datetime.min.time())
            if abs((today-offset).days > 1):
                self.scrape_period = 'history'
            else:
                self.scrape_period = 'daily'
        except Exception:
            logger.error('set scrape period', exc_info=True)

    def update_proxy(self):
        try:
            data = get_random_scraper_data()
            self.proxy = Proxy({
                'proxyType': ProxyType.MANUAL,
                'httpProxy': data['proxy'],
                'ftpProxy': data['proxy'],
                'sslProxy': data['proxy'],
                'noProxy': ''  # set this value as desired
            })
            self.driver = webdriver.Firefox(proxy=self.proxy, firefox_profile=self.firefox_profile,
                                            firefox_options=self.options)
        except Exception:
            logger.error('update period', exc_info=True)

    def get_item_offset(self, item_name, min_max='MAX'):
        try:
            nested_field = item_name
            if min_max == 'MAX':
                result = self.pym.db[self.table].find(
                    {nested_field: {'$exists': True}}).sort('timestamp', -1).limit(1)
            else:
                result = self.pym.db[self.table].find(
                    {nested_field: {'$exists': True}}).sort('timestamp', 1).limit(1)

            offset = self.initial_date
            if result.count() > 0:
                for res in result:
                    offset = res['timestamp']
            # SET DATETIME TO DATE WITH MIN TIME
            # ensure timestamp fits mongo scheme
            if 'daily' in self.table:
                offset = datetime.combine(offset.date(),datetime.min.time())

            #logger.warning('%s offset from mongo:%s', item_name, offset)
            return offset
        except Exception:
            logger.error("get item offset", exc_info=True)

    def get_processed_hours(self,item_name,timestamp):
        try:
            if isinstance(timestamp,str):
                timestamp = datetime.strptime(timestamp, self.DATEFORMAT)
            self.pym = PythonMongo('aion')
            nested_field = item_name+"."+"processed_hours"
            result = self.pym.db[self.table].find(
                {
                    'timestamp': {'$eq':timestamp},
                    nested_field:{'exists':True}
                }
            ).limit(1)
            if result.count() > 0:
                for res in result:
                    logger.warning('result: %s',res)
                    return res['processed_hours']
            else:
                return []
        except Exception:
            logger.error("processed hours", exc_info=True)

    def item_in_mongo(self, item_name, timestamp):
        try:
            nested_field = item_name
            result = self.pym.db[self.table].find(
                {
                    'timestamp': {'$eq': timestamp},
                    nested_field: {'exists': True}
                }
            ).limit(1)
            if result.count() > 0:
                for res in result:
                    logger.warning(res)
                    return res
            else:
                return None
        except Exception:
            logger.error('item in mongo', exc_info=True)

    def item_is_up_to_date(self, item_name):
        try:
            offset = self.get_item_offset(item_name)
            yesterday =  datetime.combine(datetime.today().date(),datetime.min.time()) - timedelta(days=1)
            if self.scraper_name == 'financial indexes':
                # if yesterday is a weekend day, adjust to friday
                #logger.warning('yesterday.weekday: %s',yesterday.weekday())
                if yesterday.weekday() in [5,6]:
                    yesterday = yesterday - timedelta(days=abs(yesterday.weekday() - 4))

            if offset >= yesterday:
                logger.warning('%s up to timestamp offset:yesterday=%s:%s',item_name,offset,yesterday)
                return True
            elif offset >= yesterday - timedelta(days=1):
                logger.warning('%s daily offset:yesterday=%s:%s',item_name,offset,yesterday-timedelta(days=1))
                self.scrape_period = 'daily'
            else:
                #logger.warning('%s history offset:yesterday=%s:%s',item_name,offset,yesterday)
                self.scrape_period = 'history'
            return False
        except Exception:
            logger.error("item is_up_to_date", exc_info=True)

    def get_date_data_from_mongo(self, timestamp):
        try:
            if isinstance(timestamp, str):
                timestamp = datetime.strptime(timestamp, self.DATEFORMAT)

            result = self.pym.db[self.table].find(
                {
                    'timestamp': {'$eq': timestamp},
                }
            ).limit(1)
            if result.count() > 0:
                for res in result:
                    return res
            else:
                return None
        except Exception:
            logger.error("processed hours", exc_info=True)

    # check if all coins under inspection have been loaded up to yesterday
    def is_up_to_date(self):
        try:
            timestamp = datetime.now() - timedelta(hours=self.window)
            timestamp = datetime(timestamp.year,timestamp.month, timestamp.day,timestamp.hour,0,0)
            if self.scraper_name == 'financial indexes':
                # if yesterday is a weekend day, adjust to friday
                if timestamp.weekday() in [5, 6]:
                    logger.warning('DATE ADJUSTED CAUSE YESTERDAY IS A WEEKEND')
                    timestamp = timestamp - timedelta(days=abs(timestamp.weekday() - 4))

            counter = 0
            for item in self.items:
                #logger.warning('scraper:%s, self.items:%s', self.scraper_name, self.items)
                if self.item_is_up_to_date(item_name=item):
                    counter += 1
                    logger.warning('items:%s',item)

            if counter >= len(self.items):
                return True
            return False

        except Exception:
            logger.error("is_up_to_date", exc_info=True)


    def get_checkpoint_dict(self, col='block_timestamp', db='aion'):
        # try to load from , then redis, then clickhouse
        try:
            key = self.key_params
            temp_dct = self.redis.load([], '', '', key=key, item_type='checkpoint')
            if temp_dct is not None:
                self.checkpoint_dict = temp_dct
            else: # set if from config

                self.checkpoint_dict = self.dct
                # refill checkpoint dict with last update from database
                for item_name in self.items:
                    nested_field = item_name + "." + self.checkpoint_column
                    result = self.pym.db[self.table].find(
                        {nested_field: {'$exists': True}}).sort('timestamp', -1).limit(1)

                    offset = self.initial_date
                    processed_hours = []
                    if result.count() > 0:
                        for res in result:
                            logger.warning('res:%s',res)
                            offset = res['timestamp']
                            if self.scraper_name == 'github':
                                processed_hours = res['processed_hours']

                    self.checkpoint_dict['items'][item_name] = {
                        'offset': datetime.strftime(offset,self.DATEFORMAT),
                        'timestamp': datetime.now().strftime(self.DATEFORMAT),
                    }

                    if self.scraper_name == 'github':
                        self.checkpoint_dict['items'][item_name]['processed_hours'] = processed_hours

                    self.save_checkpoint()
        except Exception:
            logger.error("get checkpoint dict", exc_info=True)


    async def run(self):
        # create warehouse table in clickhouse if needed
        # self.create_table_in_clickhouse()
        while True:
            if self.is_up_to_date():
                logger.warning("%s SCRAPER SLEEPING FOR 24 hours:UP TO DATE", self.scraper_name)
                self.driver.quit()
                await asyncio.sleep(self.window * 60 * 60)  # sleep
                self.start_firefox_driver()
            else:
                await asyncio.sleep(1)
            await self.update()