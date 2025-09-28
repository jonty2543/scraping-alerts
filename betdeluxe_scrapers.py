import time
import asyncio
import traceback

from loguru                       import logger
from random                       import randrange
from playwright_stealth           import stealth_async
from playwright.async_api         import async_playwright
 
class BDSportsScraper:
    def __init__(self, url, chosen_date):
        """
        Betdeluxe Scraper initialisation function.

        :param: url str: Link to Sportsbet page that is being scraped.
        :param: race_code str: Race code that is being scraped. Options include "Racing", "Greyhound", "Harness", "International".
        :param: chosen_date str: The date of the race. Should be in this format: YYYY-mm-dd
        """
        self.url = url
        self.chosen_date = chosen_date
        # self.jurisdiction = self.map_jurisdiction(jurisdiction)


    async def BETDELUXE_scraper(self, sport, market_name='Match Result'):
        win_market = {}
    
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            ua = (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/116.0.0.0 Safari/537.36 Edg/116.0.1938.81"
            )
            page = await browser.new_page(user_agent=ua)
    
            await page.goto(self.url)
    
            all_comps = await page.evaluate(f"() => fetch('{self.url}').then(r => r.json())")
            if not all_comps:
                logger.error("Failed to fetch markets")
                await browser.close()
                return win_market
    
            comp_ids = [
                comp.get("id")
                for region in all_comps["data"]["regions"]
                for comp in region["competitions"]
            ]
    
            for comp_id in comp_ids:
                url = f"https://api.blackstream.com.au/api/sports/v1/sports/{sport}/competitions/{comp_id}/events"
                events = await page.evaluate(f"() => fetch('{url}').then(r => r.json())")
                if not events:
                    logger.warning("No event found for comp")
                    continue
    
                for event in events["data"]["events"]:
                    event_name = event.get("name")
    
                    prices = []
                    results = []
    
                    for market in event.get("markets", []):
                        if market.get("name") == market_name:
                            for outcome in market.get("outcomes", []):
                                prices.append(outcome.get("price"))
                                results.append(outcome.get("name"))
    
                    win_market[event_name] = {
                        result: price for result, price in zip(results, prices)
                    }
    
            await browser.close()
    
        return win_market
                
            
                
                
                
    
