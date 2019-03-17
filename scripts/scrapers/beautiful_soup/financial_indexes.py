import asyncio
from datetime import datetime, timedelta

from bs4 import BeautifulSoup

from config.checkpoint import checkpoint_dict
from scripts.scrapers.utils import get_random_scraper_data
from scripts.utils.mylogger import mylogger
from scripts.scrapers.beautiful_soup.bs_scraper_interface import Scraper

logger = mylogger(__file__)

class FinancialIndexes(Scraper):
    coin_abbr = {
        'nasdaq':'ndaq',
        'sp':'sp',
        'russell':'iwv'
    }
    DATEFORMAT = "%m/%d/%Y"

    def __init__(self, items):
        Scraper.__init__(self)
        self.items = items
        self.item_name = 'russell'
        self.close = self.item_name+'_close'
        self.volume = self.item_name + '_volume'
        self.cols.append(self.close)
        self.cols.append(self.volume)
        self.url = 'https://www.nasdaq.com/symbol/{}/historical'\
            .format(self.coin_abbr[self.item_name])

        # checkpointing
        self.checkpoint_key = 'indexscraper'
        self.key_params = 'checkpoint:' + self.checkpoint_key
        self.checkpoint_column = 'russell_close'
        self.dct = checkpoint_dict[self.checkpoint_key]
        self.DATEFORMAT = '%m/%d/%Y'

    async def update(self):
        try:
            # set load date
            offset = self.get_offset()
            this_date = offset + timedelta(days=1)
            logger.warning("OFFSET INCREASED:%s", this_date)
            self.set_scrape_period(offset)

            for self.item_name in self.items:
                url = 'https://www.nasdaq.com/symbol/{}/historical'\
                    .format(self.coin_abbr[self.item_name])
                self.driver.implicitly_wait(10)
                self.driver.get(url)
                await asyncio.sleep(5)
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

                logger.warning('%s SCRAPER %s COMPLETED:', self.item_name.upper(), self.scrape_period)

                # PAUSE THE LOADER, SWITCH THE USER AGENT, SWITCH THE IP ADDRESS
                self.update_proxy()

        except Exception:
            logger.error('financial indicies:', exc_info=True)

