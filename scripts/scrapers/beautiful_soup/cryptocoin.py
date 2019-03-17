import asyncio
from datetime import datetime, timedelta

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.expected_conditions import visibility_of_element_located
from bs4 import BeautifulSoup

from config.checkpoint import checkpoint_dict
from scripts.utils.mylogger import mylogger
from scripts.storage.pythonMongo import PythonMongo
from scripts.scrapers.beautiful_soup.bs_scraper_interface import Scraper


logger = mylogger(__file__)

class Cryptocoin(Scraper):
    collection = 'external'
    pym = PythonMongo('aion')
    def __init__(self, items):
        Scraper.__init__(self)
        self.item_name = 'aion'
        self.items = items
        self.DATEFORMAT_coinmarket = "%b %d, %Y"
        self.volume = self.item_name+'_coin_volume'
        self.close = self.item_name+'_coin_close'
        self.open = self.item_name+'_coin_open'
        self.high = self.item_name+'_coin_high'
        self.low = self.item_name+'_coin_low'
        self.cap = self.item_name+'_coin_marketcap'
        self.url = 'https://coinmarketcap.com/currencies/{}/historical-data/'\
            .format(self.item_name)
        # checkpointing
        self.checkpoint_key = 'coinscraper'
        self.key_params = 'checkpoint:'+self.checkpoint_key
        self.checkpoint_column = 'aion_coin_close'
        self.dct = checkpoint_dict[self.checkpoint_key]
        self.offset = self.initial_date

    async def update(self):
        try:
            for self.item_name in self.items:
                if self.item_is_up_to_date():
                    pass
                else:
                    self.offset = self.offset + timedelta(days=1)
                    logger.warning("OFFSET:%s", self.offset)
                    url = 'https://coinmarketcap.com/currencies/{}/historical-data/'\
                        .format(self.item_name)
                    # launch url
                    self.driver.implicitly_wait(30)
                    self.driver.get(url)
                    logger.warning('url loaded:%s',url)
                    await asyncio.sleep(6)
                    if self.scrape_period == 'history':
                        # click on the dropdown list to expose it
                        dropdown = self.driver.find_element_by_id('reportrange')
                        self.driver.execute_script("arguments[0].click();", dropdown)
                        await asyncio.sleep(2)

                        # click on the exposed link
                        wait = WebDriverWait(self.driver, 3)
                        link = wait.until(visibility_of_element_located(
                            (By.CSS_SELECTOR, '.ranges li:nth-child(4)')))
                        print('LINK:',link)

                        link.click()
                        await asyncio.sleep(6)

                    # get soup
                    soup = BeautifulSoup(self.driver.page_source, 'html.parser')
                    table = soup.find('table', attrs={'class':'table'})
                    # parse table and write to database
                    count = 0
                    rows = table.find('tbody').findAll('tr')
                    for row in rows:
                        item = {}
                        item['date'] = datetime.strptime(row.findAll('td')[0].contents[0],self.DATEFORMAT_coinmarket)
                        item[self.open] = float(row.findAll('td')[1].contents[0].replace(',', ''))
                        item[self.high] = float(row.findAll('td')[2].contents[0].replace(',', ''))
                        item[self.low] = float(row.findAll('td')[3].contents[0].replace(',', ''))
                        item[self.close] = float(row.findAll('td')[4].contents[0].replace(',', ''))
                        item[self.volume] = float(row.findAll('td')[5].contents[0].replace(',', ''))
                        item[self.cap] = float(row.findAll('td')[6].contents[0].replace(',', ''))
                        if count <= 1:
                            self.cols = list(item)
                            self.cols.remove('date')

                        #print('{} {} data added'.format(self.coin,item['date']))
                        self.process_item(item)
                        if self.scrape_period != 'history':
                            if count >= 1:
                                break
                        count += 1

                    logger.warning('%s SCRAPER %s COMPLETED', self.item_name.upper(),self.scrape_period)

                    # PAUSE THE LOADER, SWITCH THE USER AGENT, SWITCH THE IP ADDRESS
                    self.update_proxy()

        except Exception:
            logger.error('BS4: crytocoin run:',exc_info=True)

