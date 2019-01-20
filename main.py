from concurrent.futures import ThreadPoolExecutor
import threading

# IMPORT HELPERS
from bokeh.document import without_document_lock
from bokeh.models import Div, Tabs
from tornado import gen
from bokeh.server.server import Server
from tornado.gen import coroutine

from scripts.tablemanager.Table import Table
from scripts.utils.mylogger import mylogger
from scripts.ETL.miner_activity import MinerActivity
logger = mylogger(__file__)


table = 'miner_activity'
miner_activity = MinerActivity(table)

@coroutine
def kafka_spark_streamer(doc):
    try:

        yield miner_activity.run()

        #t = Table('transaction_delete','transaction', 'create')


        tabs = Tabs(tabs=[])
        doc.add_root(tabs)
    except Exception:
        logger.error("TABS:", exc_info=True)


# Setting num_procs here means we can't touch the IOLoop before now, we must
# let Server handle that. If you need to explicitly handle IOLoops then you
# will need to use the lower level BaseServer class.
@coroutine
@without_document_lock
def launch_server():
    try:
        server = Server({'/':  kafka_spark_streamer}, num_procs=1, port=5007)
        server.start()
        server.io_loop.add_callback(server.show, "/")
        server.io_loop.start()

    except Exception:
        logger.error("WEBSERVER LAUNCH:", exc_info=True)


if __name__ == '__main__':
    print('Opening Bokeh application on http://localhost:5007/')
    launch_server()

