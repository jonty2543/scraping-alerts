import time
import asyncio
import traceback
import re

from loguru                       import logger
from random                       import randrange
from playwright_stealth           import stealth_async
from playwright.async_api         import async_playwright
 
class BetrSportsScraper:
    def __init__(self, url, chosen_date):
        """
        Betr Scraper initialisation function.

        :param: url str: Link to Sportsbet page that is being scraped.
        :param: race_code str: Race code that is being scraped. Options include "Racing", "Greyhound", "Harness", "International".
        :param: chosen_date str: The date of the race. Should be in this format: YYYY-mm-dd
        """
        self.url = url
        self.chosen_date = chosen_date
        # self.jurisdiction = self.map_jurisdiction(jurisdiction)


    async def Betr_scrape_union(self, comp=None):
        """
        NPC BlueBet Win Market Scraper for Rugby Union.
        Skips markets with line bets (e.g., outcomes containing +/- numbers).
        """
        win_market = {}

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36 Edg/116.0.1938.81"
            context = await browser.new_context(user_agent=ua)
            page = await context.new_page()

            response = await page.request.get(
                self.url,
                headers={"User-Agent": ua, "Accept": "application/json"}
            )

            if response.status != 200:
                logger.error(f"Failed to fetch markets, status: {response.status}")
                await browser.close()
                return win_market

            all_markets = await response.json()
            await browser.close()

        # Regex to detect lines like "-3.5" or "+2"
        line_regex = re.compile(r'[-+]\d+(\.\d+)?')

        try:
            for master in all_markets.get("MasterCategories", []):
                for cat in master.get("Categories", []):
                    if comp and cat.get("CategoryName") != comp:
                        continue

                    for event in cat.get("MasterEvents", []):
                        match_name = event.get("MasterEventName")
                        skip_match = False

                        for mkt in event.get("Markets", []):
                            if mkt.get("MarketTypeCode") == "WIN":
                                outcome_name = mkt.get("OutcomeName")

                                # Skip if outcome has + or - number
                                if line_regex.search(outcome_name):
                                    skip_match = True
                                    break

                        if skip_match:
                            continue  # skip entire match

                        # Add valid WIN outcomes
                        for mkt in event.get("Markets", []):
                            if mkt.get("MarketTypeCode") == "WIN":
                                outcome_name = mkt.get("OutcomeName")
                                price = mkt.get("Price")
                                if match_name not in win_market:
                                    win_market[match_name] = {}
                                win_market[match_name][outcome_name] = price

        except (KeyError, TypeError) as e:
            logger.error(f"Unexpected JSON structure: {e}")
            return win_market

        return win_market
            