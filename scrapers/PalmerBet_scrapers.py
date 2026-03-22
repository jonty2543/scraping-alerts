import time
import asyncio
import traceback
import re
import requests

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

    def _requests_json(self, url, retries=3, delay=1.0):
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json",
        }
        last_err = None
        for attempt in range(retries):
            try:
                resp = requests.get(url, headers=headers, timeout=20)
                if resp.status_code == 200:
                    return resp.json()
                last_err = f"HTTP {resp.status_code}"
            except Exception as e:
                last_err = e
            if attempt < retries - 1:
                time.sleep(delay * (attempt + 1))
        logger.warning(f"PalmerBet requests JSON failed for {url}: {last_err}")
        return None


    async def PalmerBet_scrape(self, comp=None, sport=None, market_type='h2h'):
        """
        NPC PalmerBet Scraper.
        """
        # Input checking
        
        win_market = {}

        all_markets = self._requests_json(self.url, retries=3, delay=1.2)
        if not all_markets:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36 Edg/116.0.1938.81"
                page = await browser.new_page(user_agent=ua)
                try:
                    await page.goto(self.url, wait_until="domcontentloaded")
                    all_markets = await page.evaluate(
                        f"""() => fetch('{self.url}')
                        .then(r => r.text())
                        .then(t => {{ try {{ return JSON.parse(t); }} catch (e) {{ return null; }} }})"""
                    )
                except Exception:
                    all_markets = None
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
                game_comp = str((game.get('paths') or [{}, {}, {}])[2].get('title', ''))
                comp_l = str(comp).lower()
                game_comp_l = game_comp.lower()
                is_nrl_alias = ('rugby league' in comp_l and game_comp_l == 'nrl') or (comp_l == 'nrl' and 'rugby league' in game_comp_l)
                if not (comp_l in game_comp_l or game_comp_l in comp_l or is_nrl_alias):
                    continue
                
            date = game.get("startTime")
            dt_utc = datetime.fromisoformat(date.replace("Z", "+00:00"))
            brisbane_dt = dt_utc.astimezone(ZoneInfo("Australia/Brisbane"))
            brisbane_date = brisbane_dt.date().strftime("%Y-%m-%d")
                    
            home_name = game.get("homeTeam", {}).get("title")
            away_name = game.get("awayTeam", {}).get("title")
            match = f"{home_name} vs {away_name}"

            prices = []
            results = []

            if market_type == 'h2h':
                for team_key in ["homeTeam", "awayTeam"]:
                    team = game.get(team_key, {})
                    name = team.get("title")
                    price = team.get("win", {}).get("price")
                    results.append(name)
                    prices.append(price)

                if sport == 'football':
                    draw_data = game.get("draw")
                    if draw_data:
                        results.append("draw")
                        prices.append(draw_data.get("price"))

            else:
                target_keywords = ["line", "handicap"] if market_type == "line" else ["total"]
                target_market = None
                for market in game.get("additionalMarkets", []):
                    market_type_text = str(market.get("type", "")).lower()
                    market_title_text = str(market.get("title", "")).lower()
                    if any(k in market_type_text or k in market_title_text for k in target_keywords):
                        target_market = market
                        break

                if not target_market:
                    continue

                for outcome in target_market.get("outcomes", []):
                    result = outcome.get("title")
                    if market_type == "line" and result:
                        mtch = re.search(r'(.+?)\s+([+-]?\d+(?:\.\d+)?)$', result)
                        if mtch:
                            result = f"{mtch.group(1).strip()} {float(mtch.group(2)):+.1f}"
                    results.append(result)
                    prices.append(outcome.get("price"))

            win_market[match, brisbane_date] = {res: price for res, price in zip(results, prices)}
                  
        return win_market
    
    
