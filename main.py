from scripts.ETL.account_activity_churn import AccountActivityChurn
from scripts.ETL.account_activity_warehouse import AccountActivityWarehouse
from scripts.ETL.warehouse import Warehouse
from scripts.ETL.account_activity_warehouse import AccountActivityWarehouse
from scripts.tablemanager.Table import Table
from scripts.utils.mylogger import mylogger
import asyncio

loop = asyncio.get_event_loop()

logger = mylogger(__file__)
'''
table = 'network_activity'
#network_activity_etl = NetworkTxActivity(table)
#network_activity_etl.reset_offset('2019-01-23 00:00:00')

table = 'block_tx_warehouse'
'''
#table = 'account_activity'
#tb = Table(table,table,'create')
#warehouse_etl.reset_offset('2018-04-23 05:00:00')
#warehouse_etl = Warehouse('block_tx_warehouse')
#account_activity_etl = AccountActivity('account_activity')
#account_activity_churn_etl = AccountActivityChurn('account_activity_churn')

'''
table = 'account_activity_churn'
#tb = Table(table,table,'create')

table = 'account_value_churn'
#tb = Table(table,table,'create')
account_value_churn_etl = AccountValueChurn(table)
'''

table = 'account_activity_warehouse'
account_activity_warehouse_etl = AccountActivityWarehouse(table)


async def run_etls():

    tasks = [
        #asyncio.ensure_future(network_activity_etl.run()),
        #asyncio.ensure_future(warehouse_etl.run()),
        #asyncio.ensure_future(account_activity_etl.run()),
        #asyncio.ensure_future(account_activity_churn_etl.run()),
        asyncio.ensure_future(account_activity_warehouse_etl.run()),


    ]
    await asyncio.wait(tasks)


if __name__ == '__main__':
    try:
        loop.run_until_complete(asyncio.ensure_future(run_etls()))
    except Exception:
        logger.error('',exc_info=True)
    finally:
        loop.close()

