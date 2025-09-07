import time
import asyncio
import traceback

from loguru                       import logger
from random                       import randrange
from playwright_stealth           import stealth_async
from playwright.async_api         import async_playwright
 
class UBSportsScraper:
    def __init__(self, url, chosen_date):
        """
        Unibet Scraper initialisation function.

        :param: url str: Link to Sportsbet page that is being scraped.
        :param: race_code str: Race code that is being scraped. Options include "Racing", "Greyhound", "Harness", "International".
        :param: chosen_date str: The date of the race. Should be in this format: YYYY-mm-dd
        """
        self.url = url
        self.chosen_date = chosen_date
        # self.jurisdiction = self.map_jurisdiction(jurisdiction)


    async def UNIBET_scrape_tennis(self):
        """
        Union Unibet Scraper.
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
    

        try:
            groups = all_markets["layout"]["sections"][1]["widgets"][0]["matches"]["groups"]
        except (KeyError, IndexError, TypeError) as e:
            logger.error(f"Unexpected JSON structure from Unibet: {e}")
            return win_market

        for group in groups:
            for division in group.get("subGroups", []):
                for comp in division.get("events", []):
                    event = comp.get("event", {})
                    match_name = event.get("englishName") or event.get("name")

                    if not match_name:
                        continue

                    win_market.setdefault(match_name, {})

                    for bet_offer in comp.get("betOffers", []):
                        bet_type = bet_offer.get("betOfferType", {}).get("englishName", "")
                        if bet_type.lower() == "match":  # Match odds only
                            for outcome in bet_offer.get("outcomes", []):
                                player = outcome.get("englishLabel")
                                odds = outcome.get("oddsDecimal")
                                try:
                                    win_market[match_name][player] = float(odds)
                                except (TypeError, ValueError):
                                    win_market[match_name][player] = None

        return win_market
    
    async def UNIBET_scrape_football(self):
        """
        football Unibet Scraper.
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
    

        try:
            groups = all_markets["layout"]["sections"][1]["widgets"][0]["matches"]["groups"]
        except (KeyError, IndexError, TypeError) as e:
            logger.error(f"Unexpected JSON structure from Unibet: {e}")
            return win_market

        for group in groups:
            for division in group.get("subGroups", []):
                for comp in division.get("events", []):
                    event = comp.get("event", {})
                    match_name = event.get("englishName") or event.get("name")

                    if not match_name:
                        continue

                    win_market.setdefault(match_name, {})

                    for bet_offer in comp.get("betOffers", []):
                        bet_type = bet_offer.get("betOfferType", {}).get("englishName", "")
                        if bet_type.lower() == "match":  # Match odds only
                            for outcome in bet_offer.get("outcomes", []):
                                player = outcome.get("participant")
                                odds = outcome.get("oddsDecimal")
                                try:
                                    win_market[match_name][player] = float(odds)
                                except (TypeError, ValueError):
                                    win_market[match_name][player] = None

        return win_market
    
    async def UNIBET_scrape_union(self, comp=False):
        """
        football Unibet Scraper.
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
    

        try:
            groups = all_markets["layout"]["sections"][1]["widgets"][0]["matches"]["groups"]
        except (KeyError, IndexError, TypeError) as e:
            logger.error(f"Unexpected JSON structure from Unibet: {e}")
            return win_market

        for group in groups:
            
            if comp:
                if group['name'] != comp:
                    continue
            
            for game in (group.get("events") or []):
                event = game.get("event", {})
                match_name = event.get("englishName") or event.get("name")

                if not match_name:
                    continue

                win_market.setdefault(match_name, {})

                for bet_offer in game.get("betOffers", []):
                    bet_type = bet_offer.get("betOfferType", {}).get("englishName", "")
                    if bet_type.lower() == "match":  # Match odds only
                        for outcome in bet_offer.get("outcomes", []):
                            team = outcome.get("englishLabel")
                            odds = outcome.get("oddsDecimal")
                            try:
                                win_market[match_name][team] = float(odds)
                            except (TypeError, ValueError):
                                win_market[match_name][team] = None

        return win_market
    
    async def UNIBET_scrape_sport(self, comp=None):
        """
        General sport Unibet Scraper.
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
    

        try:
            groups = all_markets["layout"]["sections"][1]["widgets"][0]["matches"]["groups"]
        except (KeyError, IndexError, TypeError) as e:
            logger.error(f"Unexpected JSON structure from Unibet: {e}")
            return win_market

        def process_events(events, win_market):
            for comp in events:
                event = comp.get("event", {})
                match_name = event.get("englishName") or event.get("name")
        
                if not match_name:
                    continue
        
                win_market.setdefault(match_name, {})
        
                for bet_offer in comp.get("betOffers", []):
                    bet_type = bet_offer.get("betOfferType", {}).get("englishName", "")
                    if bet_type.lower() == "match":  # Match odds only
                        for outcome in bet_offer.get("outcomes", []):
                            team = outcome.get("participant") or outcome.get("englishLabel") or ""
                            odds = outcome.get("oddsDecimal")
                            try:
                                win_market[match_name][team] = float(odds)
                            except (TypeError, ValueError):
                                win_market[match_name][team] = None
        
        
        for group in groups:
            if comp and group.get("name") != comp:
                continue
        
            # Case 1: group has subGroups
            if group.get("subGroups"):
                for subgroup in group["subGroups"]:
                    process_events(subgroup.get("events", []), win_market)
        
            # Case 2: group has no subGroups
            else:
                process_events(group.get("events", []), win_market)

        return win_market
    
    
    
    