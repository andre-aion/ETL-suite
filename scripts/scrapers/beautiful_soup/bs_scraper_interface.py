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
    def __init__(self):
        Checkpoint.__init__(self,'external')
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


    def process_item(self,item, item_name):
        try:
            #logger.warning('item before save:%s',item)
            for col in list(item.keys()):
                #logger.warning('col:%s', col)
                if col != 'date':
                    nested_search = item_name+'.'+col
                    self.pym.db[self.collection].update_one(
                        {'date': item['date']},
                        {'$set':
                             {
                               nested_search:item[col]
                             }
                        },
                        upsert=True)

            logger.warning("%s item added to MongoDB database!",format(self.item_name))
        except Exception:
            logger.error('process item', exc_info=True)

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

    def get_item_offset(self, checkpoint_column, item_name, min_max='MAX'):
        try:
            # first check self.checkpoint_dict
            self.get_checkpoint_dict()
            if item_name in self.checkpoint_dict['items'].keys():
                offset = self.checkpoint_dict['items'][item_name]['offset']
                offset = datetime.strptime(offset,self.DATEFORMAT)
            else:
                nested_field = self.item_name+"."+checkpoint_column
                if min_max == 'MAX':
                    result = self.pym.db[self.table].find(
                        {nested_field: {'$exists': True}}).sort('date', -1).limit(1)
                else:
                    result = self.pym.db[self.table].find(
                        {nested_field: {'$exists': True}}).sort('date', 1).limit(1)

                offset = self.initial_date
                if result.count() > 0:
                    for res in result:
                        offset = res['date']
                # SET DATETIME TO DATE WITH MIN TIME
                # ensure date fits mongo scheme
                offset = datetime.combine(offset.date(),datetime.min.time())
                logger.warning('%s offset from mongo:%s', item_name, offset)
            return offset
        except Exception:
            logger.error("get item offset", exc_info=True)

    def get_processed_hours(self,item_name,date):
        try:
            if isinstance(date,str):
                date = datetime.strptime(date, self.DATEFORMAT)
            self.pym = PythonMongo('aion')
            nested_field = item_name+"."+"processed_hours"
            result = self.pym.db[self.table].find(
                {
                    'date': {'$eq':date},
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

    def item_in_mongo(self, item_name, date):
        try:
            nested_field = item_name
            result = self.pym.db[self.table].find(
                {
                    'date': {'$eq': date},
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


    def item_is_up_to_date(self, checkpoint_column, item_name):
        try:
            self.offset = self.get_item_offset(checkpoint_column,item_name)
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
            counter = 0
            today = datetime.combine(datetime.today().date(), datetime.min.time())
            for item in self.items:
                if item in self.checkpoint_dict['items'].keys():
                    offset = datetime.strptime(self.checkpoint_dict['items'][item]['offset'],self.DATEFORMAT)
                    if self.scraper_name == 'github':
                        hours_completed = len(self.checkpoint_dict['items'][item]['processed_hours'])
                        if offset >= today - timedelta(hours=self.window) and hours_completed >= 24:
                            counter += 1
                    else:
                        if offset >= today - timedelta(hours=self.window):
                            counter += 1
                else:
                    return False
            if counter == len(self.items):
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
                        {nested_field: {'$exists': True}}).sort('date', -1).limit(1)

                    offset = self.initial_date
                    processed_hours = []
                    if result.count() > 0:
                        for res in result:
                            logger.warning('res:%s',res)
                            offset = res['date']
                            if self.scraper_name == 'githubloader':
                                processed_hours = res['processed_hours']

                    self.checkpoint_dict['items'][item_name] = {
                        'offset': datetime.strftime(offset,self.DATEFORMAT),
                        'timestamp': datetime.now().strftime(self.DATEFORMAT),
                    }

                    if self.scraper_name == 'githubloader':
                        self.checkpoint_dict['items'][item_name]['processed_hours'] = processed_hours

                    self.save_checkpoint()
        except Exception:
            logger.error("get checkpoint dict", exc_info=True)

    async def run(self):
        # create warehouse table in clickhouse if needed
        # self.create_table_in_clickhouse()
        while True:
            if self.is_up_to_date():
                logger.warning("%s SCRAPER SLEEPING FOR 24 hours:UP TO DATE",self.scraper_name)
                await asyncio.sleep(self.window*60*60)  # sleep
            else:
                await asyncio.sleep(1)
            await self.update()


