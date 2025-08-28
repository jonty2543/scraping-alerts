import time
import asyncio
import traceback

from loguru                       import logger
from random                       import randrange
from playwright_stealth           import stealth_async
from playwright.async_api         import async_playwright
 
class PBSportsScraper:
    def __init__(self, url, chosen_date):
        """
        PointsBet Scraper initialisation function.

        :param: url str: Link to Sportsbet page that is being scraped.
        :param: race_code str: Race code that is being scraped. Options include "Racing", "Greyhound", "Harness", "International".
        :param: chosen_date str: The date of the race. Should be in this format: YYYY-mm-dd
        """
        self.url = url
        self.chosen_date = chosen_date
        # self.jurisdiction = self.map_jurisdiction(jurisdiction)


    async def POINTSBET_scrape_union(self, market_type):
        """
        Union Pointsbet Scraper.
        """
        # Input checking

        async with async_playwright() as p:
            # Stealth Browser Set Up to Access Sportsbet API (Not Needed but just copied over from TAB)
            browser = await p.chromium.launch(headless=True)
            ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36 Edg/116.0.1938.81"
            page = await browser.new_page(user_agent=ua)

            await page.goto(self.url)

            all_markets = await page.evaluate(f"() => fetch('{self.url}').then(response => response.json())")
            if not all_markets:
                logger.error("Failed to fetch markets")
                await browser.close()
                            
            win_market = {}
            
            events = all_markets['events']
                
            for event in events:
                markets = event['specialFixedOddsMarkets']
                
                for market in markets:
                    
                    if market['eventName'] != market_type:
                        continue
                
                    outcomes = market['outcomes']
                    market_name = event['name']
                                    
                    prices = []
                    results = []
                    
                    for outcome in outcomes:
                        result = outcome['name']
                        price = outcome['price']
                        results.append(result)
                        prices.append(price)
                    
                    win_market[market_name] = {
                        result: price for result, price in zip(results, prices)
                    }
                
        return win_market
    
    async def POINTSBET_scrape_nrl(self, market_type):
        """
        Nrl Pointsbet Scraper.
        """
        # Input checking

        async with async_playwright() as p:
            # Stealth Browser Set Up to Access Sportsbet API (Not Needed but just copied over from TAB)
            browser = await p.chromium.launch(headless=True)
            ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36 Edg/116.0.1938.81"
            page = await browser.new_page(user_agent=ua)

            await page.goto(self.url)

            all_markets = await page.evaluate(f"() => fetch('{self.url}').then(response => response.json())")
            if not all_markets:
                logger.error("Failed to fetch markets")
                await browser.close()
                
            win_market = {}
            
            events = all_markets['events']
                
            for event in events:
                markets = event['specialFixedOddsMarkets']
                
                for market in markets:
                    
                    if market['eventClass'] != market_type:
                        continue
                
                    outcomes = market['outcomes']
                    market_name = event['name']
                                    
                    prices = []
                    results = []
                    
                    for outcome in outcomes:
                        result = outcome['name']
                        price = outcome['price']
                        results.append(result)
                        prices.append(price)
                    
                    win_market[market_name] = {
                        results[0]: prices[0],
                        results[1]: prices[1]
                    }
                
        return win_market
    
    async def POINTSBET_scrape_sport(self, market_type):
        """
        Football Pointsbet Scraper.
        """
        # Input checking

        async with async_playwright() as p:
            # Stealth Browser Set Up to Access Sportsbet API (Not Needed but just copied over from TAB)
            browser = await p.chromium.launch(headless=True)
            ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36 Edg/116.0.1938.81"
            page = await browser.new_page(user_agent=ua)

            await page.goto(self.url)

            league = await page.evaluate(f"() => fetch('{self.url}').then(response => response.json())")
            if not league:
                logger.error("Failed to fetch markets")
                await browser.close()
                            
            win_market = {}
                
            for event in league.get('events'):
                markets = event['fixedOddsMarkets']
                
                prices = []
                results = []
                
                for market in markets:
                    
                    if market['eventClass'] != market_type:
                        continue
                
                    outcomes = market['outcomes']
                    market_name = event['name']
                                    
                    prices = []
                    results = []
                    
                    for outcome in outcomes:
                        result = outcome['name']
                        price = outcome['price']
                        results.append(result)
                        prices.append(price)
                    
                    win_market[market_name] = {
                        result: price for result, price in zip(results, prices)
                    }
                
        return win_market