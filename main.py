from scripts.ETL.crypto_daily import CryptoDaily
from scripts.utils.mylogger import mylogger
import asyncio
from scripts.scrapers.financial_indexes import FinancialIndexes
from scripts.scrapers.cryptocoin import Cryptocoin
from scripts.scrapers.country_economic_indicators import CountryEconomicIndicators
from scripts.github.github_loader import GithubLoader
from scripts.ETL.account_ext_warehouse import AccountExternalWarehouse
from scripts.utils.myutils import load_cryptos
from scripts.storage.backup.mongo_backup import MongoBackup

'''
from scripts.tablemanager.table import Table
table = 'crypto_daily'
tb = Table(table,table,'create','timestamp')
'''

loop = asyncio.get_event_loop()

logger = mylogger(__file__)

# ETLS
#warehouse_etl = BlockTxWarehouse('block_tx_warehouse')
# backup
mongo_backup = MongoBackup(['external_daily','github'])

# scrapers
cryptocurrencies = load_cryptos()
financial_indicies = ['russell','sp']

indexes_scraper = FinancialIndexes(financial_indicies)
cryptos_scraper = Cryptocoin(cryptocurrencies)
github_loader = GithubLoader(cryptocurrencies)
economic_indicators = CountryEconomicIndicators()

#cryptos_scraper.reset_offset('2018-04-24 00:00:00')
logger.warning(cryptocurrencies)

table = 'account_ext_warehouse'

account_ext_warehouse = AccountExternalWarehouse(table='account_ext_warehouse',
                                                 mysql_credentials='staging',
                                                 items=cryptocurrencies)

crytpo_daily = CryptoDaily(table='crypto_daily',
                           items=cryptocurrencies)
reset_offset = {'start':'2018-04-23 00:00:00', 'end':'2019-04-17 00:00:00'}

async def run_etls():
    tasks = [
        #asyncio.ensure_future(indexes_scraper.run()),
        #asyncio.ensure_future(cryptos_scraper.run()),
        #asyncio.ensure_future(github_loader.run()),
        #asyncio.ensure_future(mongo_backup.run()),
        #asyncio.ensure_future(account_ext_warehouse.run(None)),
        #asyncio.ensure_future(crytpo_daily.run(None)),
        #asyncio.ensure_future(economic_indicators.run(reset_offset)),
    ]
    await asyncio.wait(tasks)


if __name__ == '__main__':
    try:
        loop.run_until_complete(asyncio.ensure_future(run_etls()))
    except Exception:
        logger.error('',exc_info=True)
    finally:
        loop.close()
