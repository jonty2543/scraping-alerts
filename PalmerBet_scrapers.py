import time
import asyncio
import traceback

from loguru                       import logger
from random                       import randrange
from playwright_stealth           import stealth_async
from playwright.async_api         import async_playwright
from datetime import datetime
from zoneinfo import ZoneInfo

class PalmerBetSportsScraper:
    def __init__(self, url, chosen_date):
        """
        PalmerBet Scraper initialisation function.

        :param: url str: Link to Sportsbet page that is being scraped.
        :param: race_code str: Race code that is being scraped. Options include "Racing", "Greyhound", "Harness", "International".
        :param: chosen_date str: The date of the race. Should be in this format: YYYY-mm-dd
        """
        self.url = url
        self.chosen_date = chosen_date
        # self.jurisdiction = self.map_jurisdiction(jurisdiction)


    async def PalmerBet_scrape(self, comp=None, sport=None):
        """
        NPC PalmerBet Scraper.
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
            matches = all_markets["matches"]
        except (KeyError, IndexError, TypeError):
            logger.error("Unexpected JSON structure from Unibet")
            return win_market

        for game in matches:
            
            if game.get("status") != 'NotStarted':
                continue
            
            if comp:
                if game['paths'][2]['title'] != comp:
                    continue
                
            date = game.get("startTime")
            dt_utc = datetime.fromisoformat(date.replace("Z", "+00:00"))
            brisbane_dt = dt_utc.astimezone(ZoneInfo("Australia/Brisbane"))
            brisbane_date = brisbane_dt.date().strftime("%Y-%m-%d")
                    
            prices = []
            results = []

                              
            for team_key in ["homeTeam", "awayTeam"]:
                team = game.get(team_key, {})
                name = team.get("title")
                price = team.get("win", {}).get("price")
                
                results.append(name)
                prices.append(price)
            
            if sport == 'football':
                draw_data = game.get("draw")
                if draw_data:  # only add if it exists
                    results.append("draw")
                    prices.append(draw_data.get("price"))
                
            match = " vs ".join(results[:2])
    
            # Store outcome-price pairs in dict
            win_market[match, brisbane_date] = {res: price for res, price in zip(results, prices)}
                  
        return win_market
    
    