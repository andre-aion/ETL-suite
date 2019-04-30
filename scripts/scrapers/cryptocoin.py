
import asyncio
from datetime import datetime, timedelta

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.expected_conditions import visibility_of_element_located
from bs4 import BeautifulSoup

from config.checkpoint import checkpoint_dict
from scripts.utils.mylogger import mylogger
from scripts.storage.pythonMongo import PythonMongo
from scripts.scraper_interface import Scraper


logger = mylogger(__file__)

class Cryptocoin(Scraper):
    def __init__(self, items):
        Scraper.__init__(self, collection='external_daily')
        self.item_name = 'aion'
        self.items = items.copy()
        self.items.sort()
        self.DATEFORMAT_coinmarket = "%b %d, %Y"
        self.volume = 'volume'
        self.close = 'close'
        self.open = 'open'
        self.high = 'high'
        self.low = 'low'
        self.cap = 'market_cap'
        self.url = 'https://coinmarketcap.com/currencies/{}/historical-data/'\
            .format(self.item_name)
        # checkpointing
        self.checkpoint_key = 'coinscraper'
        self.key_params = 'checkpoint:'+self.checkpoint_key
        self.checkpoint_column = 'close'
        self.dct = checkpoint_dict[self.checkpoint_key]
        self.offset = self.initial_date

        self.scraper_name = 'crytpo coin'


    async def update(self):
        try:
            for self.item_name in self.items:
                item_name = self.item_name+'.'+self.checkpoint_column
                if self.item_is_up_to_date(item_name):
                    logger.warning('%s financial index is up to date',self.item_name)
                else:
                    yesterday = datetime.combine(datetime.today().date(), datetime.min.time()) - timedelta(days=1)

                    self.offset = self.offset + timedelta(days=1)
                    url = 'https://coinmarketcap.com/currencies/{}/historical-data/'\
                        .format(self.item_name)
                    # launch url
                    self.driver['firefox'].implicitly_wait(30)
                    self.driver['firefox'].get(url)
                    logger.warning('url loaded:%s',url)
                    await asyncio.sleep(6)
                    if self.scrape_period == 'history':
                        # click on the dropdown list to expose it
                        dropdown = self.driver['firefox'].find_element_by_id('reportrange')
                        self.driver['firefox'].execute_script("arguments[0].click();", dropdown)
                        await asyncio.sleep(2)

                        # click on the exposed link
                        wait = WebDriverWait(self.driver['firefox'], 3)
                        link = wait.until(visibility_of_element_located(
                            (By.CSS_SELECTOR, '.ranges li:nth-child(6)')))
                        print('LINK:',link)

                        link.click()
                        await asyncio.sleep(6)

                    # get soup
                    soup = BeautifulSoup(self.driver['firefox'].page_source, 'html.parser')
                    table = soup.find('table', attrs={'class':'table'})
                    # parse table and write to database
                    count = 0
                    rows = table.find('tbody').findAll('tr')
                    for row in rows:
                        item = {}
                        item['timestamp'] = datetime.strptime(row.findAll('td')[0].contents[0],self.DATEFORMAT_coinmarket)
                        item[self.open] = float(row.findAll('td')[1].contents[0].replace(',', ''))
                        item[self.high] = float(row.findAll('td')[2].contents[0].replace(',', ''))
                        item[self.low] = float(row.findAll('td')[3].contents[0].replace(',', ''))
                        item[self.close] = float(row.findAll('td')[4].contents[0].replace(',', ''))
                        item['month'] = item['timestamp'].month
                        item['day'] = item['timestamp'].day
                        item['year'] = item['timestamp'].year
                        item['hour'] = item['timestamp'].hour
                        try:
                            item[self.volume] = float(row.findAll('td')[5].contents[0].replace(',', ''))
                        except:
                            item[self.volume] = 0
                        try:
                            item[self.cap] = float(row.findAll('td')[6].contents[0].replace(',', ''))
                        except:
                            item[self.cap] = 0

                        if count <= 1:
                            self.cols = list(item)
                            self.cols.remove('timestamp')


                        #print('{} {} data added'.format(self.coin,item['timestamp']))
                        self.process_item(item,self.item_name)

                        if self.scrape_period != 'history':
                            if count >= 1:
                                break
                        count += 1
                    self.driver['firefox'].close() # close currently open browsers
                    # PAUSE THE LOADER, SWITCH THE USER AGENT, SWITCH THE IP ADDRESS
                    self.update_proxy()

                    logger.warning('%s SCRAPER %s COMPLETED', self.item_name.upper(),self.scrape_period)



        except Exception:
            logger.error('BS4: crytocoin run:',exc_info=True)

    async def run(self):
        # create warehouse table in clickhouse if needed
        # self.create_table_in_clickhouse()
        while True:
            if self.is_up_to_date():
                logger.warning("%s SCRAPER SLEEPING FOR 24 hours:UP TO DATE", self.scraper_name)
                await asyncio.sleep(self.window * 60 * 60)  # sleep
            else:
                await asyncio.sleep(1)
            await self.update()