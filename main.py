"""
from scripts.ETL.account_activity_churn import AccountActivityChurn
from scripts.ETL.account_activity import AccountActivity
"""
from scripts.ETL.account_activity_warehouse import AccountActivityWarehouse
from scripts.utils.mylogger import mylogger
import asyncio
from scrapers.scrapy.runner import RunSpiders
from scrapers.beautiful_soup.runner import run_scrapers
#warehouse_etl = Warehouse('block_tx_warehouse')

loop = asyncio.get_event_loop()

logger = mylogger(__file__)

"""
#table = 'account_activity'
#tb = Table(table,table,'create')
account_activity_etl = AccountActivity('account_activity')
account_activity_churn_etl = AccountActivityChurn('account_activity_churn')

#account_activity_etl.reset_offset('2018-07-01 01:00:00')


"""
account_activity_warehouse_etl = AccountActivityWarehouse('account_activity_warehouse')

# scrapers
cryptocurrencies = ['aion','bitcoin']
async def run_etls():

    tasks = [
        asyncio.ensure_future(run_scrapers(cryptocurrencies)),
        #asyncio.ensure_future(warehouse_etl.run()),
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
