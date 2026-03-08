import time
import asyncio
import traceback
import json
import re

from loguru                       import logger
from random                       import randrange
from playwright_stealth           import stealth_async
from playwright.async_api         import async_playwright
from datetime import datetime
from zoneinfo import ZoneInfo

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
        
    
    async def BETRIGHT_scraper(self, market_type=None, competition_id='none', market_kind='h2h', retries=3, delay=2):
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
                                
                                date = event.get("maxAdvertisedStartTimeUtc")
                                dt_utc = datetime.fromisoformat(date.replace("Z", "+00:00"))
                                brisbane_dt = dt_utc.astimezone(ZoneInfo("Australia/Brisbane"))
                                brisbane_date = brisbane_dt.date().strftime("%Y-%m-%d")
                                
                                prices = []
                                results = []
                                
                                for market in event.get("markets"):
                                    if market_type != None:
                                        if market.get("eventName") != market_type:
                                            continue

                                    result = market.get("outcomeName")
                                    price = market.get("price")
                                    points = market.get("points")

                                    outcome_text = str(result or "").lower()
                                    is_total = "over" in outcome_text or "under" in outcome_text or "total" in str(market.get("eventName", "")).lower()
                                    is_line = points not in (None, 0, 0.0)

                                    if market_kind == 'h2h' and is_line:
                                        continue
                                    if market_kind == 'line':
                                        if not is_line or is_total:
                                            continue
                                        result = f"{result} {float(points):+.1f}"
                                    if market_kind == 'total':
                                        if not is_total:
                                            continue
                                        if points not in (None, 0, 0.0) and not re.search(r'[-+]?\d+(?:\.\d+)?', str(result)):
                                            result = f"{result} {abs(float(points)):.1f}"

                                    results.append(result)
                                    prices.append(price)
                                
                                win_market[match, brisbane_date] = {
                                   result: price for result, price in zip(results, prices)
                                }                     

                
        return win_market

    async def BETRIGHT_scraper_masterevent(self, market_kind='h2h', category_name='NRL', competition_id='none'):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36 Edg/116.0.1938.81"
            page = await browser.new_page(user_agent=ua)

            await page.goto(self.url)

            all_categories = await page.evaluate(f"() => fetch('{self.url}').then(response => response.json())")
            if not all_categories:
                logger.error("Failed to fetch markets")
                await browser.close()
                return {}

            win_market = {}

            for category in all_categories.get("masterCategories", []):
                if competition_id != 'none' and category.get("masterCategoryId") != competition_id:
                    continue

                for league in category.get("categories", []):
                    if category_name and str(league.get("categoryName", "")).lower() != str(category_name).lower():
                        continue

                    league_id = league.get("categoryId")
                    if not league_id:
                        continue

                    league_url = f'https://next-api.betright.com.au/Sports/Category?categoryId={league_id}'
                    league_json = await page.evaluate(f"() => fetch('{league_url}').then(response => response.json())")

                    for mc in league_json.get("masterCategories", []):
                        for cat in mc.get("categories", []):
                            for master_event in cat.get("masterEvents", []):
                                master_event_id = master_event.get("masterEventId")
                                match = master_event.get("masterEventName")
                                if not master_event_id or not match or " v " not in match:
                                    continue

                                date = master_event.get("maxAdvertisedStartTimeUtc")
                                if not date:
                                    continue
                                dt_utc = datetime.fromisoformat(date.replace("Z", "+00:00"))
                                brisbane_dt = dt_utc.astimezone(ZoneInfo("Australia/Brisbane"))
                                brisbane_date = brisbane_dt.date().strftime("%Y-%m-%d")

                                details_url = f'https://next-api.betright.com.au/Sports/MasterEventEvents?masterEventId={master_event_id}'
                                details_json = await page.evaluate(f"() => fetch('{details_url}').then(response => response.json())")
                                events = details_json.get("events", []) if isinstance(details_json, dict) else []
                                if not events:
                                    continue

                                participants = [t.strip() for t in str(match).split(" v ") if t.strip()]
                                prices_map = {}

                                if market_kind == 'h2h':
                                    for event in events:
                                        event_name_l = str(event.get("eventName", "")).lower()
                                        if event_name_l not in {"match result", "match winner", "money line"}:
                                            continue
                                        for outcome in event.get("outcomes", []):
                                            outcome_name = outcome.get("outcomeName")
                                            price = outcome.get("price")
                                            points = outcome.get("points")
                                            code = str(outcome.get("marketTypeCode", "")).upper()
                                            if outcome_name not in participants:
                                                continue
                                            if points not in (None, 0, 0.0):
                                                continue
                                            if code and code != "WIN":
                                                continue
                                            prices_map[outcome_name] = price
                                        if len(prices_map) >= 2:
                                            break

                                elif market_kind == 'line':
                                    for event in events:
                                        if str(event.get("eventName", "")).lower() != "match result":
                                            continue
                                        # Anchor each team's "natural" handicap direction from the core handicap market.
                                        base_sign_by_team = {}
                                        for outcome in event.get("outcomes", []):
                                            header = str(outcome.get("groupByHeader", "")).lower()
                                            code = str(outcome.get("marketTypeCode", "")).upper()
                                            if header != "handicap" and code not in {"HCWEST", "HCAU"}:
                                                continue
                                            outcome_name = outcome.get("outcomeName")
                                            points = outcome.get("points")
                                            if not outcome_name or points in (None, 0, 0.0):
                                                continue
                                            sign = 1 if float(points) > 0 else -1
                                            base_sign_by_team[outcome_name] = sign

                                        for outcome in event.get("outcomes", []):
                                            points = outcome.get("points")
                                            if points in (None, 0, 0.0):
                                                continue
                                            header = str(outcome.get("groupByHeader", "")).lower()
                                            code = str(outcome.get("marketTypeCode", "")).upper()
                                            if header not in {"handicap", "pick your own line"} and code not in {"HCWEST", "HCAU", "HCPYOL"}:
                                                continue
                                            outcome_name = outcome.get("outcomeName")
                                            price = outcome.get("price")
                                            if not outcome_name or price is None:
                                                continue
                                            sign = 1 if float(points) > 0 else -1
                                            if outcome_name in base_sign_by_team and sign != base_sign_by_team[outcome_name]:
                                                continue
                                            prices_map[f"{outcome_name} {float(points):+.1f}"] = price

                                elif market_kind == 'total':
                                    for event in events:
                                        event_name = str(event.get("eventName", ""))
                                        event_name_l = event_name.lower()
                                        if "total match points over/under" not in event_name_l:
                                            continue
                                        if "first half" in event_name_l or "second half" in event_name_l:
                                            continue
                                        for outcome in event.get("outcomes", []):
                                            outcome_name = str(outcome.get("outcomeName", ""))
                                            outcome_name_l = outcome_name.lower()
                                            if "over" not in outcome_name_l and "under" not in outcome_name_l:
                                                continue
                                            price = outcome.get("price")
                                            if price is None:
                                                continue

                                            if not re.search(r'[-+]?\d+(?:\.\d+)?', outcome_name):
                                                mtch = re.search(r'([+-]?\d+(?:\.\d+)?)', event_name)
                                                if mtch:
                                                    outcome_name = f"{outcome_name.strip()} {abs(float(mtch.group(1))):.1f}"
                                            prices_map[outcome_name] = price

                                if len(prices_map) >= 2:
                                    win_market[match, brisbane_date] = prices_map

            await browser.close()
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
                                
                                date = event.get("maxAdvertisedStartTimeUtc")
                                dt_utc = datetime.fromisoformat(date.replace("Z", "+00:00"))
                                brisbane_dt = dt_utc.astimezone(ZoneInfo("Australia/Brisbane"))
                                brisbane_date = brisbane_dt.date().strftime("%Y-%m-%d")
                                
                                prices = []
                                results = []
                                
                                for market in event.get("markets"):
                                    if market.get("eventName", "").lower() != "match result":
                                        continue
                                    
                                    result = market.get("outcomeName")
                                    price = market.get("price")
                                    results.append(result)
                                    prices.append(price)
                                
                                win_market[match, brisbane_date] = {
                                   result: price for result, price in zip(results, prices)
                                }                     

                
        return win_market
    
