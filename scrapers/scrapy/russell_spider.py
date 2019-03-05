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

class RussellItem(Item):
    _id = Field()
    russell_close = Field()
    russell_volume = Field()
    date = Field()

class RussellSpider(scrapy.Spider):
    client = pymongo.MongoClient('localhost', 27017)
    db = client['aion']
    collection_name = 'external'
    name = 'russell'
    allowed_domains = ['nasdaq.com']
    start_urls = ['https://www.nasdaq.com/symbol/iwv/historical']

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
                'http_method':'POST',
                'lua_source': script
            }

        )

    def parse(self, response):
        DATEFORMAT = "%m/%d/%Y"
        sel = Selector(response)
        print("PROCESSING:" + response.url)
        rows = sel.xpath('//tr')
        count = 0
        for row in rows:
            if count > 0:
                item = RussellItem()
                print(row)
                tmp1 = row.xpath('td[6]//text()').extract_first().strip()
                tmp2 = row.xpath('td[5]//text()').extract_first().strip()
                item['russell_volume'] = float(tmp1.replace(',',''))
                item['russell_close'] = float(tmp2.replace(',',''))
                mydate = row.xpath('td[1]//text()').extract_first().strip()
                mydate = datetime.strptime(mydate,DATEFORMAT)
                item['date'] = mydate
                yield item
                self.process_item(item)
            count +=1

    def process_item(self,item,name='russell'):
        logger.warning('self.db:%s', self.db)
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

            logger.warning("russell item added to MongoDB database!")
            return item
        except Exception:
            logger.warning('process item', exc_info=True)
