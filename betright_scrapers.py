import time
import asyncio
import traceback
import json

from loguru                       import logger
from random                       import randrange
from playwright_stealth           import stealth_async
from playwright.async_api         import async_playwright

class BRSportsScraper:
    def __init__(self, url, chosen_date):
        """
        Betright Scraper initialisation function.

        :param: url str: Link to Sportsbet page that is being scraped.
        :param: race_code str: Race code that is being scraped. Options include "Racing", "Greyhound", "Harness", "International".
        :param: chosen_date str: The date of the race. Should be in this format: YYYY-mm-dd
        """
        self.url = url
        self.chosen_date = chosen_date
        # self.jurisdiction = self.map_jurisdiction(jurisdiction)
        
    
    async def BETRIGHT_scraper(self, competition_id='none', retries=3, delay=2):
        """
        Union Sportsbet Scraper.
        """
        # Input checking

        async with async_playwright() as p:
            # Stealth Browser Set Up to Access Sportsbet API (Not Needed but just copied over from TAB)
            browser = await p.chromium.launch(headless=True)
            ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36 Edg/116.0.1938.81"
            page = await browser.new_page(user_agent=ua)

            await page.goto(self.url)

            all_categories = await page.evaluate(f"() => fetch('{self.url}').then(response => response.json())")
            if not all_categories:
                logger.error("Failed to fetch markets")
                await browser.close()
                                        
            win_market = {}
                
            for category in all_categories.get("masterCategories"):
                
                if competition_id != 'none':
                    if category.get("masterCategoryId") != competition_id:
                        continue
                    
                for league in category.get("categories"):
                    
                    league_id = league.get("categoryId")
                    league_url = f'https://next-api.betright.com.au/Sports/Category?categoryId={league_id}'
                    league_json = await page.evaluate(f"() => fetch('{league_url}').then(response => response.json())")

                    for mc in league_json.get("masterCategories", []):
                        for cat in mc.get("categories", []):
                            for event in cat.get("masterEvents", []):                        
                                match = event.get("masterEventName")
                                
                                prices = []
                                results = []
                                
                                for market in event.get("markets"):
                                    if market.get("eventName", "").lower() != "match result":
                                        continue
                                    
                                    result = market.get("outcomeName")
                                    price = market.get("price")
                                    results.append(result)
                                    prices.append(price)
                                
                                win_market[match] = {
                                   result: price for result, price in zip(results, prices)
                                }                     

                
        return win_market

        
    
    async def BETRIGHT_scrape_football(self, competition_id='none', retries=3, delay=2):
        """
        Union Sportsbet Scraper.
        """
        # Input checking

        async with async_playwright() as p:
            # Stealth Browser Set Up to Access Sportsbet API (Not Needed but just copied over from TAB)
            browser = await p.chromium.launch(headless=True)
            ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36 Edg/116.0.1938.81"
            page = await browser.new_page(user_agent=ua)

            await page.goto(self.url)

            all_categories = await page.evaluate(f"() => fetch('{self.url}').then(response => response.json())")
            if not all_categories:
                logger.error("Failed to fetch markets")
                await browser.close()
                                        
            win_market = {}
                
            for category in all_categories.get("masterCategories"):
                
                if competition_id != 'none':
                    if category.get("masterCategoryId") != competition_id:
                        continue
                    
                for league in category.get("categories"):
                    
                    league_id = league.get("categoryId")
                    league_url = f'https://next-api.betright.com.au/Sports/Category?categoryId={league_id}'
                    league_json = await page.evaluate(f"() => fetch('{league_url}').then(response => response.json())")

                    for mc in league_json.get("masterCategories", []):
                        for cat in mc.get("categories", []):
                            for event in cat.get("masterEvents", []):                        
                                match = event.get("masterEventName")
                                
                                prices = []
                                results = []
                                
                                for market in event.get("markets"):
                                    if market.get("eventName", "").lower() != "match result":
                                        continue
                                    
                                    result = market.get("outcomeName")
                                    price = market.get("price")
                                    results.append(result)
                                    prices.append(price)
                                
                                win_market[match] = {
                                   result: price for result, price in zip(results, prices)
                                }                     

                
        return win_market
    
