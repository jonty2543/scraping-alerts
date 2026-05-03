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

    async def PalmerBet_scrape_nrl_tryscorers(self, comp='Australia National Rugby League'):
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

        def try_count(market_name):
            name = str(market_name or "").lower()
            blocked = [" or ", "combined", "combine", "half", "1st", "first", "last", "either", "&", "(", ")"]
            if any(token in name for token in blocked):
                return None
            if name in {"anytime tryscorer", "anytime try scorer", "1+ try", "player to score a try", "player to score 1 try"}:
                return 1
            if name in {"to score 2+ tries", "to score 2 or more tries", "player to score 2+ tries", "player to score 2 tries", "2+ tries"}:
                return 2
            if name in {"to score 3+ tries", "to score 3 or more tries", "player to score 3+ tries", "player to score 3 tries", "3+ tries"}:
                return 3
            return None

        def current_price(outcome):
            if outcome.get("price") is not None:
                return outcome.get("price")
            for price in outcome.get("prices", []) or []:
                snapshot = price.get("priceSnapshot") or {}
                current = snapshot.get("current")
                if current not in (None, 0, 0.0):
                    return current
            return None

        def iter_markets(node):
            if isinstance(node, dict):
                if isinstance(node.get("outcomes"), list):
                    yield node
                for val in node.values():
                    yield from iter_markets(val)
            elif isinstance(node, list):
                for item in node:
                    yield from iter_markets(item)

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36 Edg/116.0.1938.81"
            page = await browser.new_page(user_agent=ua)

            async def fetch_detail(game):
                for link in game.get("_links", []) or []:
                    href = link.get("href")
                    if not href or link.get("method", "GET") != "GET":
                        continue
                    detail_url = href if href.startswith("http") else f"https://fixture.palmerbet.online{href}"
                    if "channel=" not in detail_url:
                        sep = "&" if "?" in detail_url else "?"
                        detail_url = f"{detail_url}{sep}channel=website"
                    direct = self._requests_json(detail_url, retries=1, delay=0.0)
                    if direct is not None:
                        return direct
                    try:
                        return await page.evaluate(f"() => fetch('{detail_url}').then(response => response.json())")
                    except Exception as e:
                        logger.warning(f"PalmerBet detail fetch failed for {detail_url}: {e}")
                return game

            async def fetch_market_list(game):
                event_id = game.get("eventId")
                if not event_id:
                    return []
                market_url = (
                    "https://fixture.palmerbet.online/fixtures/sports/"
                    f"matches/{event_id}/markets?sportType=RugbyLeague&pageSize=1000&channel=website"
                )
                direct = self._requests_json(market_url, retries=1, delay=0.0)
                if direct is not None:
                    return direct.get("markets", []) if isinstance(direct, dict) else []
                try:
                    data = await page.evaluate(f"() => fetch('{market_url}').then(response => response.json())")
                    return data.get("markets", []) if isinstance(data, dict) else []
                except Exception as e:
                    logger.warning(f"PalmerBet market list fetch failed for {event_id}: {e}")
                    return []

            async def fetch_market_prices(market):
                for link in market.get("_links", []) or []:
                    href = link.get("href")
                    if not href or link.get("method", "GET") != "GET":
                        continue
                    market_url = href if href.startswith("http") else f"https://fixture.palmerbet.online{href}"
                    if "channel=" not in market_url:
                        sep = "&" if "?" in market_url else "?"
                        market_url = f"{market_url}{sep}channel=website"
                    direct = self._requests_json(market_url, retries=1, delay=0.0)
                    if direct is not None:
                        return direct.get("market", direct) if isinstance(direct, dict) else market
                    try:
                        data = await page.evaluate(f"() => fetch('{market_url}').then(response => response.json())")
                        return data.get("market", data) if isinstance(data, dict) else market
                    except Exception as e:
                        logger.warning(f"PalmerBet market price fetch failed for {market_url}: {e}")
                return market

            for game in all_markets.get("matches", []):
                if game.get("status") != 'NotStarted':
                    continue

                if comp:
                    game_comp = str((game.get('paths') or [{}, {}, {}])[2].get('title', ''))
                    comp_l = str(comp).lower()
                    game_comp_l = game_comp.lower()
                    is_nrl_alias = ('rugby league' in comp_l and game_comp_l == 'nrl') or (comp_l == 'nrl' and 'rugby league' in game_comp_l)
                    if not (comp_l in game_comp_l or game_comp_l in comp_l or is_nrl_alias):
                        continue

                detail = await fetch_detail(game)
                if isinstance(detail, dict) and "match" in detail:
                    detail = detail["match"]
                if not isinstance(detail, dict):
                    detail = game

                date = detail.get("startTime") or game.get("startTime")
                if not date:
                    continue
                dt_utc = datetime.fromisoformat(date.replace("Z", "+00:00"))
                brisbane_dt = dt_utc.astimezone(ZoneInfo("Australia/Brisbane"))
                brisbane_date = brisbane_dt.date().strftime("%Y-%m-%d")

                home_name = (detail.get("homeTeam") or game.get("homeTeam") or {}).get("title")
                away_name = (detail.get("awayTeam") or game.get("awayTeam") or {}).get("title")
                if not home_name or not away_name:
                    continue
                match = f"{home_name} vs {away_name}"

                prices = {}
                candidate_markets = list(iter_markets(detail))
                candidate_markets.extend(await fetch_market_list(game))

                for market_stub in candidate_markets:
                    title = market_stub.get("title") or market_stub.get("type")
                    tries = try_count(title)
                    if tries not in {1, 2, 3}:
                        continue

                    market = await fetch_market_prices(market_stub)
                    if not isinstance(market, dict):
                        market = market_stub
                    tries = try_count(market.get("title") or market.get("type"))
                    if tries not in {1, 2, 3}:
                        continue
                    for outcome in market.get("outcomes", []) or []:
                        player = outcome.get("title") or outcome.get("name")
                        price = current_price(outcome)
                        if not player or price is None:
                            continue
                        player = re.sub(r'\s+\d\+$', '', str(player)).strip()
                        if player.lower() in {"no try", "no tryscorer"}:
                            continue
                        prices[f"{player} {tries}+"] = price

                if prices:
                    win_market[(match, brisbane_date)] = prices

            await browser.close()
            return win_market
    
    
