#--run a crawler in a script stuff
from scripts.utils.mylogger import mylogger
from twisted.internet import reactor, defer
from scrapy.crawler import CrawlerRunner
from scrapy.settings import Settings
from config.scrapy import scrapy_settings

#--the spiders

from scripts.scrapers import NasdaqSpider
#--the spiders
from scrapy.utils.log import configure_logging
from twisted.logger import Logger
from scripts.scrapers.utils import get_proxies

log = Logger()
logger = mylogger(__file__)

scrapy_settings['ROTATING_PROXY_LIST'] = get_proxies()

class RunSpiders:
    def __init__(self):
        pass

    def onErrorfunc(self, failure):
        print('Error: {0}'.format(failure.value))

    async def run(self):
        configure_logging({'LOG_FORMAT': '%(name)s - %(levelname)s: %(message)s'})
        '''
        logging.basicConfig(
            filename='log.txt',
            format='%(name)s %(levelname)s: %(message)s',
            level=logging.INFO
        )
        '''
        runner = CrawlerRunner(Settings(scrapy_settings))

        dfs = set()
        #a = runner.crawl(SpSpider)
        #b = runner.crawl(RussellSpider)
        #c = runner.crawl(NasdaqSpider)
        try:
            ac = runner.crawl(NasdaqSpider)
            #dfs.add(a)
            #dfs.add(b)
            dfs.add(ac)
            defer.DeferredList(dfs).addBoth(lambda _: reactor.callFromThread(reactor.stop))
            reactor.run()  # the script will block here until the crawling is finished
            ac.addErrback(self.onErrorfunc)

        except Exception:
            logger.error('run',exc_info=True)



