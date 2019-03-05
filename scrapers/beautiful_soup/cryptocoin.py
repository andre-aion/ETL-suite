import asyncio
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support.expected_conditions import visibility_of_element_located
from bs4 import BeautifulSoup
import re
import pandas as pd
import os
from scripts.utils.mylogger import mylogger
from scripts.storage.pythonMongo import PythonMongo

logger = mylogger(__file__)

class Cryptocoin():
    collection = 'external'
    pym = PythonMongo('aion')
    def __init__(self,coin):
        logger.warning('{} SCRAPER STARTED:%s',coin.upper())
        self.DATEFORMAT = "%b %d, %Y"
        self.volume = coin+'_coin_volume'
        self.close = coin+'_coin_close'
        self.open = coin+'_coin_open'
        self.high = coin+'_coin_high'
        self.low = coin+'_coin_low'
        self.cap = coin+'_coin_marketcap'
        self.coin = coin

    async def run(self):
        try:
            # launch url
            company = 'aion'
            url = 'https://coinmarketcap.com/currencies/{}/historical-data/'.format(company)

            # create a new Firefox session
            driver = webdriver.Chrome('/usr/local/bin/chromedriver')
            driver.implicitly_wait(30)
            driver.get(url)
            await asyncio.sleep(6)
            #link = driver.find_element_by_css_selector('.ranges li:nth-child(4)')
            # click on the dropdown list to expose it
            dropdown = driver.find_element_by_id('reportrange')
            driver.execute_script("arguments[0].click();", dropdown)
            await asyncio.sleep(2)

            # click on the exposed link
            wait = WebDriverWait(driver, 3)
            link = wait.until(visibility_of_element_located(
                (By.CSS_SELECTOR, '.ranges li:nth-child(4)')))
            print('LINK:',link)

            link.click()
            await asyncio.sleep(6)
            # get soup
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            table = soup.find('table', attrs={'class':'table'})

            # parse table and write to database
            table_body = table.find('tbody')
            for row in table_body.findAll('tr'):
                item = {}
                item['date'] = datetime.strptime(row.findAll('td')[0].contents[0],self.DATEFORMAT)
                item[self.open] = float(row.findAll('td')[1].contents[0].replace(',', ''))
                item[self.high] = float(row.findAll('td')[2].contents[0].replace(',', ''))
                item[self.low] = float(row.findAll('td')[3].contents[0].replace(',', ''))
                item[self.close] = float(row.findAll('td')[4].contents[0].replace(',', ''))
                item[self.volume] = float(row.findAll('td')[5].contents[0].replace(',', ''))
                item[self.cap] = float(row.findAll('td')[6].contents[0].replace(',', ''))

                print('{} {} data added'.format(self.coin,item['date']))
                self.process_item(item)
            logger.warning('{} SCRAPER BULK COMPLETED:%s', self.coin.upper())
        except Exception:
            logger.error('BS4: aioncon run:',exc_info=True)

    def process_item(self, item):
        try:
            self.pym.db[self.collection].update_one(
                {'date': item['date']},
                {'$set':
                    {
                        self.close: item[self.close],
                        self.volume: item[self.volume],
                        self.high: item[self.high],
                        self.low: item[self.low],
                        self.open: item[self.open],
                        self.cap: item[self.cap]
                    }
                },
                upsert=True)

            logger.warning("aion coin item added to MongoDB database!")
            return item
        except Exception:
            logger.warning('process item', exc_info=True)
