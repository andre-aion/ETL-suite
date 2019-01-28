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

from scripts.ETL.warehouse import Warehouse
from scripts.tablemanager.Table import Table
from scripts.utils.mylogger import mylogger
from scripts.ETL.network_activity import NetworkActivity
import asyncio

loop = asyncio.get_event_loop()

logger = mylogger(__file__)

table = 'network_activity'
network_activity_etl = NetworkActivity(table)
#network_activity_etl.reset_offset('2019-01-13 05:00:00')

table = 'block_tx_warehouse'
warehouse_etl = Warehouse(table)

async def run_etls():
    tasks = [
        asyncio.ensure_future(network_activity_etl.run()),
        asyncio.ensure_future(warehouse_etl.run()),

    ]
    await asyncio.wait(tasks)

if __name__ == '__main__':

    try:
        loop.run_until_complete(asyncio.ensure_future(run_etls()))
    except Exception:
        logger.error('',exc_info=True)
    finally:
        loop.close()


