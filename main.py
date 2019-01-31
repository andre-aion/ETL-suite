import signal
import sys
from concurrent.futures import ThreadPoolExecutor
import threading

# IMPORT HELPERS
from bokeh.document import without_document_lock
from bokeh.models import Div, Tabs
from tornado import gen
from bokeh.server.server import Server
from tornado.gen import coroutine

from scripts.ETL.account_activity import AccountActivity
from scripts.ETL.account_transactions import AccountTransactions
from scripts.ETL.warehouse import Warehouse
from scripts.tablemanager.Table import Table
from scripts.utils.mylogger import mylogger
from scripts.ETL.network_tx_activity import NetworkTxActivity
import asyncio

loop = asyncio.get_event_loop()

logger = mylogger(__file__)

table = 'network_activity'
network_activity_etl = NetworkTxActivity(table)
#network_activity_etl.reset_offset('2019-01-23 00:00:00')

table = 'block_tx_warehouse'
warehouse_etl = Warehouse(table)

table = 'account_activity'
# tb = Table('account_activity','account_activity','create')
account_activity_etl = AccountActivity(table)
#account_activity_etl.reset_offset('2018-04-25 00:00:00')

async def run_etls():

    tasks = [
        #asyncio.ensure_future(warehouse_etl.run()),
        #asyncio.ensure_future(network_activity_etl.run()),
        asyncio.ensure_future(account_activity_etl.run())
    ]
    await asyncio.wait(tasks)

if __name__ == '__main__':

    try:
        loop.run_until_complete(asyncio.ensure_future(run_etls()))
    except Exception:
        logger.error('',exc_info=True)
    finally:
        loop.close()

