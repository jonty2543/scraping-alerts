import time
import asyncio
import traceback

from loguru                       import logger
from random                       import randrange
from playwright_stealth           import stealth_async
from playwright.async_api         import async_playwright
 
class SurgeSportsScraper:
    def __init__(self, url, chosen_date):
        """
        SurgeBet Scraper initialisation function.

        :param: url str: Link to Sportsbet page that is being scraped.
        :param: race_code str: Race code that is being scraped. Options include "Racing", "Greyhound", "Harness", "International".
        :param: chosen_date str: The date of the race. Should be in this format: YYYY-mm-dd
        """
        self.url = url
        self.chosen_date = chosen_date
        # self.jurisdiction = self.map_jurisdiction(jurisdiction)


    async def Surge_scrape(self, comp=None, sport=None, market=None):
        """
        SurgeBet Scraper.
        """
        # Input checking
        
        win_market = {}

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
                
        if not all_markets:
            logger.error("Failed to fetch markets")
            return win_market
    

        # Navigate to matches.groups
        try:
            matches = all_markets["data"]["events"]
        except (KeyError, IndexError, TypeError):
            logger.error("Unexpected JSON structure from palmerbet")
            return win_market
        
        for game in matches:
            
            match = game.get("name")
            
            
            prices = []
            results = []
            
            if market and game.get("markets")[0].get("name") != market:
                continue
            
            outcomes = game.get("markets")[0]['outcomes']
            
            if any(outcome.get("isOpenForBetting") != True for outcome in outcomes):
                continue
            
            for outcome in outcomes:
                
                name = outcome.get('name')
                price = outcome.get('price')
                
                results.append(name)
                prices.append(price)
            
            win_market[match] = {res: price for res, price in zip(results, prices)}
            
        
        return win_market

            
            
            
            
            
            
            
            