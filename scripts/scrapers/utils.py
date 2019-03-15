import requests
from lxml.html import fromstring
import random
from scripts.utils.mylogger import mylogger
from config.scrapy import scrapy_settings

logger = mylogger(__file__)

def get_proxies():
    url = 'https://free-proxy-list.net/'
    response = requests.get(url)
    parser = fromstring(response.text)
    proxies = set()
    for i in parser.xpath('//tbody/tr')[:20]:
        if i.xpath('.//td[7][contains(text(),"yes")]'):
            #Grabbing IP and corresponding PORT
            proxy = ":".join([i.xpath('.//td[1]/text()')[0], i.xpath('.//td[2]/text()')[0]])
            proxies.add(proxy)
    #logger.warning('%s proxies scraped!',len(proxies))
    return list(proxies)

def get_random_scraper_data():
    proxies = get_proxies()
    data = {
        'proxy':random.choice(proxies),
        'user_agent':random.choice(scrapy_settings['USER_AGENTS'])
    }
    return data