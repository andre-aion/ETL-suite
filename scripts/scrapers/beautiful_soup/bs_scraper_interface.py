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
    collection = 'external'
    pym = PythonMongo('aion')
    def __init__(self):
        Checkpoint.__init__(self,'external')
        self.cols = []
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

        # launch url
        # create a new chrome session
        chrome_options = webdriver.ChromeOptions()
        prefs = {'profile.managed_default_content_settings.images': 2}
        chrome_options.add_experimental_option("prefs", prefs)
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--window-size=%s" % WINDOW_SIZE)
        # driver = webdriver.Chrome(executable_path=WEBDRIVER_PATH)

        # create a new firefox session
        self.options = webdriver.FirefoxOptions()
        self.options.add_argument('-headless')
        self.firefox_profile = webdriver.FirefoxProfile()
        self.firefox_profile.set_preference('permissions.default.image', 2)
        self.firefox_profile.set_preference('dom.ipc.plugins.enabled.libflashplayer.so', False)
        self.firefox_profile.set_preference("network.cookie.cookieBehavior", 2)
        self.driver = webdriver.Firefox(proxy=self.proxy, firefox_profile=self.firefox_profile,
                                        firefox_options=self.options)


    def process_item(self, item):
        try:
            for col in self.cols:
                self.pym.db[self.collection].update_one(
                    {'date': item['date']},
                    {'$set':
                        {
                            col: item[col],
                        }
                    },
                    upsert=True)
            self.update_checkpoint_dict()
            self.save_checkpoint()
            logger.warning("%s item added to MongoDB database!",format(self.item_name))
        except Exception:
            logger.error('process item', exc_info=True)

    def update_items(self):
        try:
            if self.offset == self.checkpoint_dict['offset']:
                if self.items not in self.checkpoint_dict['items_updated']:
                    self.checkpoint_dict['items_updated'].append(self.items)

        except Exception:
            logger.error('update item', exc_info=True)



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

    def get_item_offset(self, checkpoint_column, min_max='MAX'):
        try:
            self.pym = PythonMongo('aion')
            # first check self.checkpoint_dict
            self.get_checkpoint_dict()
            if self.item_name in self.items_updated:
                offset = self.checkpoint_dict['offset']
            else:
                if min_max == 'MAX':
                    result = self.pym.db[self.table].find(
                        {checkpoint_column: {'$exists': True}}).sort('date', -1).limit(1)
                else:
                    result = self.pym.db[self.table].find(
                        {checkpoint_column: {'$exists': True}}).sort('date', 1).limit(1)

                offset = self.initial_date
                if result.count() > 0:
                    for res in result:
                        offset = res['date']
                # SET DATETIME TO DATE WITH MIN TIME
                # ensure date fits mongo scheme
                offset = datetime.combine(offset.date(),datetime.min.time())
                logger.warning('%s offset from mongo:%s', self.item_name, offset)
            return offset
        except Exception:
            logger.error("get item offset", exc_info=True)

    def item_is_up_to_date(self):
        try:
            checkpoint_column = self.item_name+'_fork'
            self.offset = self.get_item_offset(self.table,checkpoint_column)
            today = datetime.combine(datetime.today().date(), datetime.min.time())
            if self.offset >= today - timedelta(hours=self.is_up_to_date_window):
                return True
            elif self.offset >= today - timedelta(hours=self.is_up_to_date_window*2):
                self.scrape_period = 'daily'
            else:
                self.scrape_period = 'history'
            return False
        except Exception:
            logger.error("item is_up_to_date", exc_info=True)

    # check if all coins under inspection have been loaded up to yesterday
    def is_up_to_date(self):
        try:
            self.get_checkpoint_dict()
            if self.checkpoint_dict['offset'] is None:
                return False
            today = datetime.combine(datetime.today().date(), datetime.min.time())
            offset = datetime.strptime(self.checkpoint_dict['offset'],self.DATEFORMAT)
            if offset >= today - timedelta(hours=self.window):
                if len(self.checkpoint_dict['items_updated']) == len(self.items):
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
            logger.warning(" %s CHECKPOINT dictionary (re)set or retrieved:%s", self.checkpoint_key, self.checkpoint_dict)

        except Exception:
            logger.error("get checkpoint dict", exc_info=True)


    def update_checkpoint_dict(self):
        try:
            self.update_items()
            # update checkpoint
            self.checkpoint_dict['offset'] = datetime.strftime(self.offset,self.DATEFORMAT)
            self.checkpoint_dict['timestamp'] = datetime.now().strftime(self.DATEFORMAT)
        except Exception:
            logger.error("make warehouse", exc_info=True)


    async def run(self):
        # create warehouse table in clickhouse if needed
        # self.create_table_in_clickhouse()
        while True:
            if self.is_up_to_date():
                logger.warning("SCRAPER SLEEPING FOR 24 hours:UP TO DATE")
                await asyncio.sleep(self.window*60*60)  # sleep
            else:
                await asyncio.sleep(1)
            await self.update()


