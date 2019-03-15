import asyncio
from datetime import datetime

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.expected_conditions import visibility_of_element_located
from bs4 import BeautifulSoup
from scripts.utils.mylogger import mylogger
from scripts.storage.pythonMongo import PythonMongo
from scripts.scrapers.beautiful_soup.scraper_interface import Scraper


logger = mylogger(__file__)

class Cryptocoin(Scraper):
    collection = 'external'
    pym = PythonMongo('aion')
    def __init__(self, item_name, data, scrape_period):
        Scraper.__init__(self,item_name,data,scrape_period)
        logger.warning('%s SCRAPER STARTED',item_name.upper())
        self.DATEFORMAT = "%b %d, %Y"
        self.volume = item_name+'_coin_volume'
        self.close = item_name+'_coin_close'
        self.open = item_name+'_coin_open'
        self.high = item_name+'_coin_high'
        self.low = item_name+'_coin_low'
        self.cap = item_name+'_coin_marketcap'
        self.url = 'https://coinmarketcap.com/currencies/{}/historical-data/'.format(self.item_name)
    async def run(self):
        try:
            # launch url
            self.driver.implicitly_wait(30)
            self.driver.get(self.url)
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
                print('XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')
                print(row)
                print('XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')

                item = {}
                item['date'] = datetime.strptime(row.findAll('td')[0].contents[0],self.DATEFORMAT)
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

            logger.warning('%s SCRAPER BULK COMPLETED', self.item_name.upper())
        except Exception:
            logger.error('BS4: aioncon run:',exc_info=True)

