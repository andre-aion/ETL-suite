import pymongo
from scrapy_splash import SplashRequest

from scripts.utils.mylogger import mylogger
import scrapy
from scrapy.crawler import CrawlerProcess

from scrapy.selector import Selector
import logging
from datetime import date, datetime, time

from scrapy.item import Item, Field
logging.getLogger('scrapy').setLevel(logging.WARNING)
logger = mylogger(__file__)

class SpItem(Item):
    _id = Field()
    sp_close = Field()
    sp_volume = Field()
    date = Field()


class SpSpider(scrapy.Spider):
    client = pymongo.MongoClient('localhost', 27017)
    db = client['aion']
    collection_name = 'external'
    name = 'sp'
    allowed_domains = ['nasdaq.com']
    start_urls = ['https://www.nasdaq.com/symbol/spy/historical']

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
                'http_method': 'POST',
                'lua_source': script,
                'select_id': '#ddlTimeFrame'
            }

        )

    def parse(self, response):
        DATEFORMAT = "%m/%d/%Y"
        sel = Selector(response)
        print("PROCESSING:" + response.url)
        count = 0
        rows = sel.xpath('//tr')
        for row in rows:
            if count > 0:
                item = SpItem()
                tmp1 = row.xpath('td[6]//text()').extract_first().strip()
                tmp2 = row.xpath('td[5]//text()').extract_first().strip()
                item['sp_volume'] = float(tmp1.replace(',', ''))
                item['sp_close'] = float(tmp2.replace(',', ''))
                mydate = row.xpath('td[1]//text()').extract_first().strip()
                mydate = datetime.strptime(mydate,DATEFORMAT)
                item['date'] = mydate
                yield item
                self.process_item(item)
            count +=1


    def process_item(self,item,name='sp'):

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

            logger.warning("sp item added to MongoDB database!")
            return item
        except Exception:
            logger.warning('process item', exc_info=True)
