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
from selenium.webdriver.common.proxy import Proxy, ProxyType
from scrapers.beautiful_soup.scraper import Scraper

logger = mylogger(__file__)

class FinancialIndicies(Scraper):
    coin_abbr = {
        'nasdaq':'ndaq',
        'sp':'sp',
        'russell':'iwv'
    }
    DATEFORMAT = "%m/%d/%Y"

    def __init__(self,item_name, data, scrape_period):
        Scraper.__init__(self,item_name,data,scrape_period)
        logger.warning('%s SCRAPER STARTED',item_name.upper())
        self.close = item_name+'_close'
        self.volume = item_name+'_volume'
        self.cols.append(self.close)
        self.cols.append(self.volume)
        self.url = 'https://www.nasdaq.com/symbol/{}/historical'.format(self.coin_abbr[item_name])

    async def run(self):
        try:
            self.driver.implicitly_wait(10)
            self.driver.get(self.url)
            await asyncio.sleep(5)
            logger.warning('LINE 45')
            if self.scrape_period == 'history':
                # click on the dropdown list to expose it
                dropdown = self.driver.find_element_by_id('ddlTimeFrame')
                logger.warning('DROPDOWN:%s', dropdown)
                self.driver.execute_script("arguments[0].click();", dropdown)
                await asyncio.sleep(3)

                # click on the exposed link
                link = self.driver.find_element_by_xpath("//select[@id='ddlTimeFrame']/option[@value='1y']")
                logger.warning('LINK:%s', link)

                link.click()
                await asyncio.sleep(5)

            count = 0
            # get soup
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            div = soup.find('div', attrs={'id': 'quotes_content_left_pnlAJAX'})
            rows = div.find('tbody').findAll('tr')
            for row in rows:
                if count >= 1:
                    # logger.warning('row:%s',row)
                    item = {}
                    item['date'] = datetime.strptime(row.findAll('td')[0].contents[0].strip(), self.DATEFORMAT)
                    item[self.close] = float(row.findAll('td')[4].contents[0].strip().replace(',', ''))
                    item[self.volume] = float(row.findAll('td')[5].contents[0].strip().replace(',', ''))

                    logger.warning('%s: %s %s data added', self.item_name, item['date'], item[self.volume])
                    self.process_item(item)
                if self.scrape_period != 'history':
                    if count > 1:
                        break
                count += 1

            logger.warning('%s SCRAPER BULK COMPLETED:', self.item_name.upper())

        except Exception:
            logger.error('financial indicies:', exc_info=True)

