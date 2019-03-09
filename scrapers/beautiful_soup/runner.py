import asyncio

from scrapers.beautiful_soup.cryptocoin import Cryptocoin
from scrapers.beautiful_soup.financial_indicies import FinancialIndicies
from scripts.utils.mylogger import mylogger
from scrapers.utils import get_random_scraper_data

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
    except Exception:
        logger.error('crypto',exc_info=True)

async def indicies(items, scrape_period):
    try:
        scrapers= {}
        for item in items:
            data = get_random_scraper_data()
            logger.warning('proxy chosen for %s:%s',item,data)
            scrapers[item] = FinancialIndicies(item, data,scrape_range, scrape_period)
            await scrapers[item].run()
    except Exception:
        logger.error('indicies',exc_info=True)

async def run_scrapers(cryptocurrencies=[],
                       indicies=[],
                       scrape_period='daily'):
    await crypto(cryptocurrencies, scrape_period)
    #await indicies(fin_indicies, scrape_period)
