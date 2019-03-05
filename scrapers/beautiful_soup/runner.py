from scrapers.beautiful_soup.cryptocoin import Cryptocoin

# input list of crypto currencies to be scraped
async def run_scrapers(cryptocurrencies):
    scrapers = {}
    for coin in cryptocurrencies:
        scrapers[coin] = Cryptocoin('aion')
        await scrapers[coin].run()