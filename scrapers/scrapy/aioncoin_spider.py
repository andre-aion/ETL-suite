import pymongo
from scrapy.utils.log import configure_logging
from scripts.utils.mylogger import mylogger
import scrapy
from scrapy.http import request
from scrapy.crawler import CrawlerProcess
from scrapy_splash import SplashRequest
from scrapy.spiders import CrawlSpider

from scrapy.selector import Selector
import logging
from datetime import date, datetime, time

from scrapy.item import Item, Field

logging.getLogger('scrapy').setLevel(logging.DEBUG)
logger = mylogger(__file__)


class AionCoinItem(Item):
    _id = Field()
    aion_open = Field()
    aion_close = Field()
    aion_high = Field()
    aion_low = Field()
    aion_volume = Field()
    aion_marketcap = Field()
    date = Field()

class AioncoinSpider(scrapy.Spider):
    name = 'aioncoin'

    client = pymongo.MongoClient('localhost', 27017)
    db = client['aion']
    collection_name = 'external'
    allowed_domains = ['coinmarketcap.com']
    start_urls = ['https://coinmarketcap.com/currencies/aion/historical-data/']
    volume = 'aion_coin_volume'
    close = 'aion_coin_close'
    open = 'aion_coin_open'
    high = 'aion_coin_high'
    low = 'aion_coin_low'
    cap = 'aion_coin_marketcap'
    configure_logging({'LOG_FORMAT': '%(levelname)s: %(message)s'})

    """
    def parse_old(self, response):
        sel = Selector(response)
        pattern = '[ ]?nasdaqHomeIndexChart\.storeIndexInfo\(\"NASDAQ\",[0-9|\"|,|\.|\-]*\)'
        data = sel.xpath('//table[@id="indexTable"]').re(pattern)
        lst = data[0].split(",")
        lst[1] = lst[1].replace('"', ' ').strip()
        lst[2] = lst[2].replace('"', ' ').strip()


        item = NasdaqItem()
        item['nasdaq_close'] = float(lst[1])
        item['nasdaq_volume'] = float(lst[2])
        item['date'] = float(lst[3])
        print('nasdaq',item['nasdaq'])

        yield item


    """

    def start_requests(self):
        configure_logging({'LOG_FORMAT': '%(name)s - %(levelname)s: %(message)s'})
        logging.log(logging.DEBUG, "Loading requests")
        script = """
               function main(splash)
                  local url = 'https://coinmarketcap.com/currencies/aion/historical-data/'
                  assert(splash:go{
                      url,
                    })
                
                  assert(splash:wait(8))
                  assert(splash:runjs("$('.ranges li:nth-child(4)').click()"))
                  assert(splash:wait(8))
                
                  local getTable = splash:jsfunc([[
                    function(){
                    return $(".table tbody").html();
                  }]])
                  return getTable()
                end
               """
        yield SplashRequest(
            url=self.start_urls[0], callback=self.parse,
            endpoint='execute',
            args={
                'http_method': 'POST',
                'lua_source': script,
                'wait':1.0
            }

        )

    def parse(self, response):
        print("PROCESSING:")

        logging.log(logging.ERROR, "parsing response")
        from scrapy.shell import inspect_response
        inspect_response(response, self)
        DATEFORMAT = "%m/%d/%Y"
        DATEFORMAT = "%b %d, %Y"
        sel = Selector(response)
        rows = response.xpath('//tr')
        count = 0
        for row in rows:
            if count > 0:
                item = AionCoinItem()
                print(row)
                tmp2 = row.xpath('td[2]//text()').extract_first().strip()
                tmp3 = row.xpath('td[3]//text()').extract_first().strip()
                tmp4 = row.xpath('td[4]//text()').extract_first().strip()
                tmp5 = row.xpath('td[5]//text()').extract_first().strip()
                tmp6 = row.xpath('td[6]//text()').extract_first().strip()
                tmp7 = row.xpath('td[7]//text()').extract_first().strip()

                item[self.open] = float(tmp2.replace(',', ''))
                item[self.high] = float(tmp3.replace(',', ''))
                item[self.low] = float(tmp4.replace(',', ''))
                item[self.close] = float(tmp5.replace(',', ''))
                item[self.volume] = float(tmp6.replace(',', ''))
                item[self.cap] = float(tmp7.replace(',', ''))

                mydate = row.xpath('td[1]//text()').extract_first().strip()
                mydate = datetime.strptime(mydate, DATEFORMAT)
                item['date'] = mydate
                yield item
                self.process_item(item)
            count += 1

    def process_item(self, item):
        logger.warning('self.db:%s', self.db)
        try:
            self.db[self.collection_name].update_one(
                {'date': item['date']},
                {'$set':
                    {
                        self.close: item[self.close],
                        self.volume: item[self.volume],
                        self.high: item[self.high],
                        self.low: item[self.low],
                        self.open: item[self.open],
                        self.cap: item[self.cap]
                    }
                },
                upsert=True)

            logger.warning("aion coin item added to MongoDB database!")
            return item
        except Exception:
            logger.warning('process item', exc_info=True)
