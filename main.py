from scripts.utils.mylogger import mylogger
import asyncio
from scripts.scrapers.financial_indexes import FinancialIndexes
from scripts.scrapers.cryptocoin import Cryptocoin
from scripts.github.github_loader import GithubLoader
from scripts.ETL.account_ext_warehouse import AccountExternalWarehouse
from scripts.utils.myutils import load_cryptos
#from scripts.ETL.blocktxwarehouse import BlockTxWarehouse
#warehouse_etl = BlockTxWarehouse('block_tx_warehouse')

loop = asyncio.get_event_loop()

logger = mylogger(__file__)

"""
#tb = Table(table,table,'create')
account_activity_etl = AccountActivity('account_activity')
account_activity_churn_etl = AccountActivityChurn('account_activity_churn')

#account_activity_etl.reset_offset('2018-07-01 01:00:00')
"""
#account_activity_warehouse_etl = AccountActivityWarehouse('account_activity_warehouse')

# ETLS
#tb = Table(table,table,'create')
#warehouse_etl = BlockTxWarehouse('block_tx_warehouse')

# scrapers
cryptocurrencies = load_cryptos()
financial_indicies = ['russell','sp']

indexes_scraper = FinancialIndexes(financial_indicies)
cryptos_scraper = Cryptocoin(cryptocurrencies)
github_loader = GithubLoader(cryptocurrencies)

#cryptos_scraper.reset_offset('2018-04-24 00:00:00')
logger.warning(cryptocurrencies)

table = 'account_ext_warehouse'

account_ext_warehouse = AccountExternalWarehouse(table='account_ext_warehouse',
                                                      mysql_credentials='staging',
                                                      items=cryptocurrencies)
reset_offset = {'start':'2018-04-25 00:00:00', 'end':'2018-07-05 00:00:00'}

async def run_etls():

    tasks = [
        #asyncio.ensure_future(account_ext_warehouse.run(reset_offset)),
        #asyncio.ensure_future(warehouse_etl.run()),
        #asyncio.ensure_future(indexes_scraper.run()),
        asyncio.ensure_future(cryptos_scraper.run()),
        #asyncio.ensure_future(github_loader.run()),
        #asyncio.ensure_future(account_activity_etl.run()),
        #asyncio.ensure_future(account_activity_churn_etl.run()),
        #asyncio.ensure_future(account_activity_warehouse_etl.run()),
    ]
    await asyncio.wait(tasks)


if __name__ == '__main__':
    try:
        loop.run_until_complete(asyncio.ensure_future(run_etls()))
    except Exception:
        logger.error('',exc_info=True)
    finally:
        loop.close()
