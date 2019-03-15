import asyncio

from scripts.scrapers.beautiful_soup.cryptocoin import Cryptocoin
from scripts.scrapers.beautiful_soup.financial_indicies import FinancialIndicies
from scripts.utils.mylogger import mylogger
from scripts.scrapers.utils import get_random_scraper_data

logger = mylogger(__file__)

scrape_range = 'history'
# input list of crypto currencies to be scraped
async def crypto(items,scrape_period):
    try:
        scrapers= {}
        for item in items:
            data = get_random_scraper_data()
            logger.warning('proxy chosen for %s:%s',item,data)
            scrapers[item] = Cryptocoin(item, data,scrape_period)
            await scrapers[item].run()
            await asyncio.sleep(20)
    except Exception:
        logger.error('crypto',exc_info=True)

async def indicies(items, scrape_period):
    try:
        scrapers= {}
        for item in items:
            data = get_random_scraper_data()
            logger.warning('proxy chosen for %s:%s',item,data)
            scrapers[item] = FinancialIndicies(item, data,scrape_period)
            await scrapers[item].run()
            await asyncio.sleep(20)
    except Exception:
        logger.error('indicies',exc_info=True)

async def run_scrapers(cryptocurrencies=[],
                       fin_indicies=[],
                       scrape_period='daily'):
    while True:
        await crypto(cryptocurrencies, scrape_period)
        await indicies(fin_indicies, scrape_period)
        logger.warning('SCRAPERS GOING TO NAP for half a day')
        await asyncio.sleep(86400/2)
