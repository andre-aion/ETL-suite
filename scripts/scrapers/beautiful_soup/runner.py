import asyncio

from scripts.scrapers.beautiful_soup.cryptocoin import Cryptocoin
from scripts.scrapers.beautiful_soup.financial_indexes import FinancialIndexes
from scripts.utils.mylogger import mylogger
from scripts.scrapers.utils import get_random_scraper_data

logger = mylogger(__file__)

scrape_range = 'history'
# input list of crypto currencies to be scraped
async def crypto(items):
    try:
        scrapers= {}
        for item in items:
            data = get_random_scraper_data()
            logger.warning('proxy chosen for %s:%s',item,data)
            scrapers[item] = Cryptocoin(item, data)
            await scrapers[item].run()
            await asyncio.sleep(20)
    except Exception:
        logger.error('crypto',exc_info=True)

async def indicies(items):
    try:
        scrapers= {}
        for item in items:
            data = get_random_scraper_data()
            logger.warning('proxy chosen for %s:%s',item,data)
            scrapers[item] = FinancialIndexes(item, data)
            await scrapers[item].run()
            await asyncio.sleep(10)
    except Exception:
        logger.error('indicies',exc_info=True)

async def run_scrapers(cryptocurrencies=[],
                       fin_indicies=[]):
    while True:
        await crypto(cryptocurrencies)
        await indicies(fin_indicies)
        logger.warning('SCRAPERS GOING TO NAP for a day')
        await asyncio.sleep(86400)
