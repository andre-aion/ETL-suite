from concurrent.futures import ThreadPoolExecutor
import threading

# IMPORT HELPERS
from bokeh.document import without_document_lock
from bokeh.models import Div, Tabs
from tornado import gen
from bokeh.server.server import Server
from scripts.tablemanager.Table import Table
from scripts.utils.mylogger import mylogger

logger = mylogger(__file__)

@gen.coroutine
def kafka_spark_streamer(doc):
    try:
        t = Table('transaction_delete','transaction', 'create')

        tabs = Tabs(tabs=[t])
        doc.add_root(tabs)
    except Exception:
        logger.error("TABS:", exc_info=True)


# Setting num_procs here means we can't touch the IOLoop before now, we must
# let Server handle that. If you need to explicitly handle IOLoops then you
# will need to use the lower level BaseServer class.
@gen.coroutine
@without_document_lock
def launch_server():
    try:
        server = Server({'/': kafka_spark_streamer}, num_procs=1, port=5007)
        server.start()
        server.io_loop.add_callback(server.show, "/")
        server.io_loop.start()

    except Exception:
        logger.error("WEBSERVER LAUNCH:", exc_info=True)


if __name__ == '__main__':
    print('Opening Bokeh application on http://localhost:5007/')
    launch_server()

