import time
import asyncio, random
import traceback
import re
import requests

from loguru                       import logger
from random                       import randrange
from playwright_stealth           import stealth_async
from playwright.async_api         import async_playwright
from datetime import datetime
from zoneinfo import ZoneInfo

from scrapers.margin_utils import canonical_margin_selection, is_target_margin_market


def _pointsbet_nrl_tryscorer_try_count(market_name):
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


def _pointsbet_outcome_player_name(outcome):
    label_fields = (
        "name",
        "playerName",
        "participantName",
        "competitorName",
        "displayName",
        "label",
        "description",
    )
    for field in label_fields:
        player = outcome.get(field)
        if player is None:
            continue
        player = re.sub(r'\s+\d\+$', '', str(player)).strip()
        if not player or player.lower() in {"no try", "no tryscorer"}:
            continue
        if re.search(r'[A-Za-z]', player):
            return player
    return None


def parse_pointsbet_nrl_tryscorer_event(event):
    match_name = event.get("name")
    starts_at = event.get("startsAt")
    if not match_name or not starts_at:
        return None, {}

    dt_utc = datetime.fromisoformat(starts_at.replace("Z", "+00:00"))
    brisbane_date = dt_utc.astimezone(ZoneInfo("Australia/Brisbane")).date().strftime("%Y-%m-%d")

    prices = {}
    markets = (
        (event.get('fixedOddsMarkets') or []) +
        (event.get('specialFixedOddsMarkets') or []) +
        (event.get('insightMarkets') or [])
    )
    for market in markets:
        tries = _pointsbet_nrl_tryscorer_try_count(
            market.get("eventName") or market.get("eventClass") or market.get("name")
        )
        if tries not in {1, 2, 3}:
            continue
        for outcome in market.get("outcomes", []):
            player = _pointsbet_outcome_player_name(outcome)
            price = outcome.get("price")
            try:
                price = float(price)
            except (TypeError, ValueError):
                continue
            if not player or price <= 1:
                continue
            prices[f"{player} {tries}+"] = price

    return (match_name, brisbane_date), prices
 
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

    def _requests_json(self, url, retries=3, delay=1.0):
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json",
            "Cache-Control": "no-cache, no-store, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        }
        last_err = None
        for attempt in range(retries):
            try:
                sep = "&" if "?" in url else "?"
                request_url = f"{url}{sep}_={int(time.time() * 1000)}"
                resp = requests.get(request_url, headers=headers, timeout=20)
                if resp.status_code == 200:
                    return resp.json()
                last_err = f"HTTP {resp.status_code}: {resp.text[:200]}"
            except Exception as e:
                last_err = e
            if attempt < retries - 1:
                time.sleep(delay * (attempt + 1))
        logger.warning(f"PointsBet requests JSON failed for {url}: {last_err}")
        return None


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
                
                date = event.get("startsAt")
                dt_utc = datetime.fromisoformat(date.replace("Z", "+00:00"))
                brisbane_dt = dt_utc.astimezone(ZoneInfo("Australia/Brisbane"))
                brisbane_date = brisbane_dt.date().strftime("%Y-%m-%d")

                markets = event['specialFixedOddsMarkets']
                
                if event.get("isLive") is True:
                    continue
                
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
                    
                    win_market[market_name, brisbane_date] = {
                        result: price for result, price in zip(results, prices)
                    }
                
        return win_market
    
    async def POINTSBET_scrape_nrl(self, market_type):
        """
        Nrl Pointsbet Scraper.
        """
        all_markets = self._requests_json(self.url)
        if not all_markets:
            logger.error("Failed to fetch PointsBet NRL markets")
            return {}

        win_market = {}
        events = all_markets.get('events', [])

        for event in events:
            markets = event.get('specialFixedOddsMarkets', [])

            if event.get("isLive") is True:
                continue

            date = event.get("startsAt")
            if not date:
                continue
            dt_utc = datetime.fromisoformat(date.replace("Z", "+00:00"))
            brisbane_dt = dt_utc.astimezone(ZoneInfo("Australia/Brisbane"))
            brisbane_date = brisbane_dt.date().strftime("%Y-%m-%d")

            for market in markets:
                if market.get('eventClass') != market_type and market.get('eventName') != market_type:
                    continue
                outcomes = market.get('outcomes', [])

                if len(outcomes) < 2:
                    logger.warning(
                        f"Skipping {market.get('name')} — only {len(outcomes)} outcome(s)."
                    )
                    continue

                market_name = event['name']

                prices = []
                results = []

                for outcome in outcomes:
                    result = outcome['name']
                    price = outcome['price']
                    results.append(result)
                    prices.append(price)

                win_market[market_name, brisbane_date] = {
                    result: price for result, price in zip(results, prices)
                }

        return win_market

    async def POINTSBET_scrape_nrl_margin(self):
        """Scrape the NRL Margins 12.5 market as Team 1-12 / Team 13+."""
        payload = self._requests_json(self.url)
        if not payload:
            logger.error("Failed to fetch PointsBet NRL margin markets")
            return {}

        win_market = {}
        for event in payload.get("events", []):
            if event.get("isLive") is True:
                continue
            detail = event
            event_key = event.get("key")
            if event_key:
                detail_url = f"https://api.au.pointsbet.com/api/mes/v3/events/{event_key}"
                detail = self._requests_json(detail_url, retries=2, delay=0.5) or event

            starts_at = detail.get("startsAt") or event.get("startsAt")
            match_name = detail.get("name") or event.get("name")
            if not starts_at or not match_name:
                continue
            dt_utc = datetime.fromisoformat(starts_at.replace("Z", "+00:00"))
            brisbane_date = dt_utc.astimezone(ZoneInfo("Australia/Brisbane")).date().isoformat()

            markets = (detail.get("fixedOddsMarkets") or []) + (detail.get("specialFixedOddsMarkets") or [])
            for market in markets:
                parsed = {}
                for outcome in market.get("outcomes", []):
                    result = canonical_margin_selection(outcome.get("name"))
                    price = outcome.get("price")
                    if result and price is not None:
                        parsed[result] = price
                market_name = market.get("eventClass") or market.get("eventName") or market.get("name")
                if is_target_margin_market(market_name, list(parsed)):
                    win_market[match_name, brisbane_date] = parsed
                    break

        return win_market

    async def POINTSBET_scrape_nrl_tryscorers(self):
        """
        Scrape NRL player tryscorer markets.
        Returns {(match, date): {"Player 1+": price, "Player 2+": price, ...}}.
        """
        all_markets = self._requests_json(self.url)
        if not all_markets:
            logger.error("Failed to fetch PointsBet NRL tryscorer markets")
            return {}

        win_market = {}

        def fetch_event_detail(event):
            event_key = event.get("key")
            if not event_key:
                return event
            detail_url = f"https://api.au.pointsbet.com/api/mes/v3/events/{event_key}"
            detail = self._requests_json(detail_url, retries=2, delay=0.5)
            if detail is None:
                logger.warning(f"Pointsbet event detail fetch failed for {event_key}")
                return event
            return detail

        events = all_markets.get('events', [])
        for event in events:
            if event.get("isLive") is True:
                continue

            detail = fetch_event_detail(event)
            if detail is not event:
                detail.setdefault("name", event.get("name"))
                detail.setdefault("startsAt", event.get("startsAt"))
                detail.setdefault("insightMarkets", event.get("insightMarkets"))
            market_key, prices = parse_pointsbet_nrl_tryscorer_event(detail)
            if market_key and prices:
                win_market[market_key] = prices

        return {k: v for k, v in win_market.items() if v}
    
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
                
                if event.get("isLive") is True:
                    continue
                
                date = event.get("startsAt")
                dt_utc = datetime.fromisoformat(date.replace("Z", "+00:00"))
                brisbane_dt = dt_utc.astimezone(ZoneInfo("Australia/Brisbane"))
                brisbane_date = brisbane_dt.date().strftime("%Y-%m-%d")

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
                    
                    win_market[market_name, brisbane_date] = {
                        result: price for result, price in zip(results, prices)
                    }
                
        return win_market
    
    
class PBRacingScraper:
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
        
        
    async def POINTSBET_scrape_races(self, code):
        """
        Racing Pointsbet Scraper.
        """
        logger.info(f"Starting PointsBet scrape for racing code={code}, url={self.url}")
        
        code_map = {'4':'Greyhounds'}
    
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36 Edg/116.0.1938.81"
            page = await browser.new_page(user_agent=ua)
    
            logger.debug("Navigating to main URL")
            await page.goto(self.url)
    
            logger.debug("Fetching main JSON days data")
            days = await page.evaluate(f"() => fetch('{self.url}').then(response => response.json())")
    
            if not days:
                logger.error("Failed to fetch markets – no days returned")
                await browser.close()
                return {}
    
            logger.info(f"Fetched {len(days)} days from {self.url}")
    
            all_dict = {}
    
            for day in days:
                day_str = day.get('groupLabel')
                logger.info(f"Processing day: {day_str} with {len(day.get('meetings', []))} meetings")
    
                for meeting in day.get('meetings', []):
                    meeting_type = meeting.get('racingType')
                    
                    if code != meeting_type:
                        logger.debug(f"Skipping meeting {meeting.get('venue')} (racingType={meeting_type})")
                        continue
                    if meeting.get('countryCode') != 'AUS':
                        logger.debug(f"Skipping meeting {meeting.get('venue')} (racingType={meeting_type})")
                        continue
                        
                    meeting_id = meeting.get('meetingId')
                    meeting_name = meeting.get('venue', '').upper()
                    logger.info(f"Scraping meeting {meeting_name} ({meeting_id})")
    
                    for race in meeting.get('races', []):
                        race_id = race.get('raceId')
                        race_name = race.get('name')
                        
                        if race.get("resultStatus") != 0:
                            continue
    
                        race_url = f'https://api.au.pointsbet.com/api/racing/v3/races/{race_id}'
                        race_data = await page.evaluate(
                            f"() => fetch('{race_url}').then(response => response.json())"
                        )
    
                        if not race_data:
                            logger.warning(f"No race data found for {race_id}")
                            continue
    
                        race_no = race_data.get('number')
                        title = f'R{race_no} - {meeting_name} ({day_str})'
                        logger.info(f"Scraping {title} ({len(race_data.get('runners', []))} runners)")
                        
    
                        for runner in race_data.get('runners', []):
                            runner_name = runner.get('runnerName')
                            runner_price = runner.get('fluctuations', {}).get('current')
                            
                            if runner['isScratched'] == 'true':
                                continue
    
                            if runner_name not in all_dict:
                                all_dict[runner_name] = {}
    
                            all_dict[runner_name]['market'] = title
                            all_dict[runner_name]['name'] = runner_name
                            all_dict[runner_name]['price'] = runner_price
                        
                        await asyncio.sleep(random.uniform(0.5, 2.0))
    
    
            await browser.close()
            logger.info(f"Finished scrape for code={code}, collected {len(all_dict)} runners")
    
        return all_dict
                                
                        
                        
                        
        
                        
                        

                        
                        
                    

                    
                    
                
        
        
    
    
    
    
