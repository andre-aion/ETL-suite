scrapy_settings = {
    'USER_AGENT1': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)',
    'USER_AGENT':'User-Agent:Mozilla/5.0 (iPhone; CPU iPhone OS 7_1_2 like Mac OS X) \
                    AppleWebKit/537.51.2 (KHTML, like Gecko) Version/7.0 Mobile/11D257 Safari/9537.53',
    'SPLASH_URL': 'http://localhost:8050',
    'DOWNLOADER_MIDDLEWARES': {
        'scrapy_splash.SplashCookiesMiddleware': 723,
        'scrapy_splash.SplashMiddleware': 725,
        'scrapy.downloadermiddlewares.httpcompression.HttpCompressionMiddleware': 810,
    },
    'SPIDER_MIDDLEWARES': {
        'scrapy_splash.SplashDeduplicateArgsMiddleware': 100,
    },
    'DUPEFILTER_CLASS': 'scrapy_splash.SplashAwareDupeFilter',
    'LOG_FILE': '/tmp/spider/log.txt',
    'ROBOTSTXT_OBEY': False,
    'LOG_LEVEL':'INFO',
}