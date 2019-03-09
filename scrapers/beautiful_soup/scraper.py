from selenium import webdriver

from scripts.utils.mylogger import mylogger
from scripts.storage.pythonMongo import PythonMongo
from selenium.webdriver.common.proxy import Proxy, ProxyType

logger = mylogger(__file__)
WEBDRIVER_PATH='/usr/local/bin/chromedriver'
DBUS_SESSION_BUS_ADDRESS= '/dev/null'

class Scraper():
    collection = 'external'
    pym = PythonMongo('aion')
    def __init__(self,item_name, data, scrape_period):
        self.cols = []
        self.proxy = Proxy({
            'proxyType': ProxyType.MANUAL,
            'httpProxy': data['proxy'],
            'ftpProxy': data['proxy'],
            'sslProxy': data['proxy'],
            'noProxy': ''  # set this value as desired
        })
        self.url = ''
        self.scrape_period = scrape_period
        self.item_name = item_name

        # launch url
        # create a new chrome session
        chromeOptions = webdriver.ChromeOptions()
        prefs = {'profile.managed_default_content_settings.images': 2}
        chromeOptions.add_experimental_option("prefs", prefs)
        # driver = webdriver.Chrome(executable_path=WEBDRIVER_PATH)

        # create a new firefox session
        firefox_profile = webdriver.FirefoxProfile()
        firefox_profile.set_preference('permissions.default.image', 2)
        firefox_profile.set_preference('dom.ipc.plugins.enabled.libflashplayer.so', False)
        self.driver = webdriver.Firefox(proxy=self.proxy, firefox_profile=firefox_profile)

    def process_item(self, item):
        try:
            for col in self.cols:
                self.pym.db[self.collection].update_one(
                    {'date': item['date']},
                    {'$set':
                        {
                            col: item[col],
                        }
                    },
                    upsert=True)

                logger.warning("%s item added to MongoDB database!",format(self.item_name))
        except Exception:
            logger.error('process item', exc_info=True)