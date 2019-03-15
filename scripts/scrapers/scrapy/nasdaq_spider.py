import pymongo
from scripts.utils.mylogger import mylogger
import scrapy
from scrapy.http import request
from scrapy.crawler import CrawlerProcess
from scrapy_splash import SplashRequest
from scrapy.linkextractors import LinkExtractor

from scrapy.selector import Selector
import logging
from datetime import date, datetime, time

from scrapy.item import Item, Field
logging.getLogger('scrapy').setLevel(logging.WARNING)
logger = mylogger(__file__)

class NasdaqItem(Item):
    _id = Field()
    nasdaq_close = Field()
    nasdaq_volume = Field()
    date = Field()

class NasdaqSpider(scrapy.Spider):
    name = 'nasdaq'
    client = pymongo.MongoClient('localhost', 27017)
    db = client['aion']
    collection_name = 'external'
    allowed_domains = ['nasdaq.com']
    start_urls = ['https://www.nasdaq.com/symbol/ndaq/historical']
    close = "nasdaq_close"
    volume = "nasdaq_volume"

    def start_requests(self):
        script = """
               function main(splash)
                  url = splash.args.url

                  assert(splash:go{
                      url,
                      http_method='POST',
                    })

                  assert(splash:wait(3))
                  assert(splash:runjs("$('#ddlTimeFrame').val('1y').change()"))
                  assert(splash:wait(4))

                  local getTable = splash:jsfunc([[
                    function(){
                     return $("#quotes_content_left_pnlAJAX tbody").html();
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
                'select_id': '#ddlTimeFrame'
            })

    def parse(self, response):
        DATEFORMAT = "%m/%d/%Y"
        sel = Selector(response)
        print("PROCESSING:" + response.url)
        count = 0
        rows = sel.xpath('//tr')
        for row in rows:
            if count > 0:
                item = NasdaqItem()
                item['nasdaq_volume'] = float(row.xpath('td[6]//text()').extract_first().strip())
                item['nasdaq_close'] = float(row.xpath('td[5]//text()').extract_first().strip())
                mydate = row.xpath('td[1]//text()').extract_first().strip()
                mydate = datetime.strptime(mydate,DATEFORMAT)
                item['date'] = mydate
                yield item
                self.process_item(item)
            count +=1

    def process_item(self,item,name='nasdaq'):
        logger.warning('db:%s', self.db)
        close = name+"_close"
        volume = name+"_volume"
        try:
            self.db[self.collection_name].update_one(
                {'date': item['date']},
                {'$set':
                    {
                        close: item[close],
                        volume: item[volume]
                    }
                },
                upsert=True)

            logger.warning("nasdaq item added to MongoDB database!")
            return item
        except Exception:
            logger.warning('process item', exc_info=True)