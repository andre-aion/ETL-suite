
import asyncio
from datetime import datetime, timedelta, date

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.expected_conditions import visibility_of_element_located
from bs4 import BeautifulSoup

from config.checkpoint import checkpoint_dict
from config.indicators import country_indicators
from scripts.utils.mylogger import mylogger
from scripts.storage.pythonMongo import PythonMongo
from scripts.scraper_interface import Scraper


logger = mylogger(__file__)

class CountryEconomicIndicators(Scraper):
    def __init__(self):
        Scraper.__init__(self, collection='country_daily')
        self.item_name = 'united_states'
        self.window = 24
        self.countries = country_indicators['countries']
        self.indicators = country_indicators['scrape_cols']
        self.url = 'https://tradingeconomics.com'
        # checkpointing
        #self.table = 'country_economic_indicators'
        self.key_params = 'checkpoint:'+self.checkpoint_key
        cols = sorted(list(self.indicators.keys()))
        self.checkpoint_column = cols[0]
        self.scraper_name = 'country_economic_indicators'
        self.checkpoint_key = self.scraper_name
        self.dct = checkpoint_dict[self.scraper_name]
        self.offset = self.initial_date
        self.country = 'barbados'

    def fix_country_names(self,countries):
        countries_fixed = []
        for country in countries:
            country = country.lower()
            countries_fixed.append(country.replace(' ','_'))

    def am_i_up_to_date(self, offset_update=None):
        try:

            today = datetime.combine(datetime.today().date(), datetime.min.time())
            yesterday = today - timedelta(days=1)
            self.offset_update = offset_update
            if offset_update is not None:
                self.offset, self.offset_update = self.reset_offset(offset_update)
            else:
                self.offset = self.get_value_from_clickhouse(self.table)

            # first check if the ETL table is up to timestamp
            logger.warning('my max date:%s',self.offset)
            offset = self.offset
            if isinstance(self.offset,date):
                offset = datetime.combine(self.offset,datetime.min.time())
            stop_date = yesterday
            if self.offset_update is not None:
                stop_date = datetime.strptime(self.offset_update['end'], self.DATEFORMAT)
            if offset < stop_date:
                return False
            else:
                return True
        except Exception:
            logger.error('am i up to timestamp', exc_info=True)


    async def update(self):
        try:
            yesterday = datetime.combine(datetime.today().date(), datetime.min.time()) - timedelta(days=1)
            offset = self.offset
            stop_date = yesterday
            if self.offset_update is not None:
                stop_date = datetime.strptime(self.offset_update['end'],self.DATEFORMAT)

            logger.warning('offset:%s',offset)
            url = self.url
            # launch url
            self.driver.implicitly_wait(30)
            logger.warning('url loaded:%s',url)

            self.driver.get(url)
            await asyncio.sleep(10)
            # click on the box to expose all the countries
            script = "__doPostBack('ctl00$ContentPlaceHolder1$defaultUC1$CurrencyMatrixAllCountries1$LinkButton1','')"
            self.driver.execute_script(script)
            await asyncio.sleep(2)

            # get soup
            table_id ="ctl00_ContentPlaceHolder1_defaultUC1_CurrencyMatrixAllCountries1_GridView1"
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            table = soup.find('table', attrs={'id':table_id})
            # parse table and write to database
            count = 0
            rows = table.find('tbody').findAll('tr')
            # save for each date
            tmp_offset = offset
            while tmp_offset < stop_date:
                tmp_offset = tmp_offset + timedelta(days=1)
                for row in rows:
                    if count == 0: # skip table headers
                        count += 1
                    else:
                        item = {
                            'timestamp':tmp_offset
                        }
                        for a in row.findAll('a',href=True):
                            if a.text:
                                tmp = a['href'][1:] # strip the first '/'
                                tmp_vec = tmp.split('/')
                                if 'indicators' in tmp:
                                    # prep country name
                                    item_name = a.text.strip()
                                    item_name = item_name.lower()
                                    item_name = item_name.replace(' ', '_')
                                else:
                                    # extract the column names
                                    colname = tmp_vec[-1]
                                    # extract the content
                                    txt = a.text.replace('%','')
                                    try:
                                        x = float(txt)
                                    except Exception:
                                        x = 0
                                    item[colname] = x

                            self.process_item(item, item_name=item_name) # save
                print('economic data loaded for %s',tmp_offset)
                # update checkpoint
                self.update_checkpoint_dict(tmp_offset)
                self.save_checkpoint()
                tmp_offset, self.offset_update = self.reset_offset(self.offset_update)


        except Exception:
            logger.error('country economic indicators',exc_info=True)

    async def run(self,offset_update):
        #self.initialize_table()
        """
        --offset up_date takes the form
        offset_update = {
            'start': datetime.strptime('2018-06-20 00:00:00,self.DATEFORMAT'),
            'end': datetime.strptime('2018-08-12 00:00:00,self.DATEFORMAT')
        }
        """
        while True:
            if self.am_i_up_to_date(offset_update):
                logger.warning("%s UP TO DATE- WENT TO SLEEP FOR %s HOURS",self.table,self.window)
                await asyncio.sleep(self.window*60*60)
            else:
                await  asyncio.sleep(1)
            await self.update()


