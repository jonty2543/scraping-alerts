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
import pytz

class SBRacingScraper:
    def __init__(self, url, race_code, chosen_date):
        """
        Sportsbet Scraper initialisation function.

        :param: url str: Link to Sportsbet page that is being scraped.
        :param: race_code str: Race code that is being scraped. Options include "Racing", "Greyhound", "Harness", "International".
        :param: chosen_date str: The date of the race. Should be in this format: YYYY-mm-dd
        """
        self.url = url
        self.race_code = self.map_race_code(race_code)
        self.chosen_date = chosen_date
        # self.jurisdiction = self.map_jurisdiction(jurisdiction)

    # Change mappings
    def map_race_code(self, race_code):
        """
        Map race code input by the user with Sportsbet's format.

        :param: race_code str: Race code that will be scraped later. Options include "Racing", "Greyhound", "Harness", "International".
        :return: Tuple with race code(s) as the first element(s).
        """
        logger.info("Starting Sportsbet scraping...")

        if race_code == 'Racing':
            return ('horse',)
        elif race_code == 'Greyhound':
            return ('greyhound',)
        elif race_code == 'Harness':
            return ('harness',)
        elif race_code == 'International':
            return ('horse', 'greyhound', 'harness')
        else:
            return (race_code)

    async def SPORTSBET_scrape(self, mode: str = "win"):
        """
        Main Sportsbet Scraper.

        :param: mode str: Which prices to scrape from TAB? Options are "win" and "place". Default is win prices.
        :return: List (dict type) contains race events & entrants, also return empty dict if there is an error with fetching.
        """
        # Input checking
        jsonParam: str = ""
        if mode == "win" or mode == "Win":
            jsonParam = "winPrice"
        elif mode == "place" or mode == "Place":
            jsonParam = "placePrice"
        else:
            raise ValueError("Unknown mode option. Please choose between 'win'/'Win' or 'place'/'Place'")

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

            section_markets = {}
            # Find the matching section
            for section in all_markets["dates"][0]["sections"]:
                if (section["raceType"] in self.race_code) and (len(section["meetings"]) != 0):
                    section_markets = section["meetings"]
            if len(section_markets) == 0:
                logger.info(f"No {self.race_code} Markets Found for {self.chosen_date}")

            markets_present = False
            total_markets_count = 0
            all_dict = {}

            for market in section_markets:
                class_id = market['classId']
                meeting_name = market["name"]
                # Loop through the races for each meeting
                for event in market["events"]:
                    if (event['type'] in self.race_code) and (event['category'] == 'standard') and (event["regionGroup"] == "Aus/NZ") and (event["statusCode"] != 'R'):
                        markets_present = True
                        total_markets_count += 1

                        race_number = event["raceNumber"]

                        # Adjust race number and meeting name for extra races
                        race_number = event["raceNumber"]
                        meeting_label = meeting_name.upper()

                        if race_number > 12:
                            race_number -= 12
                            meeting_label += " Extra"



                        # Format the race names
                        race_name = "R{race_number} - {meeting_name} ({date})".format(
                            race_number = str(race_number),
                            meeting_name = "WAGGA" if (meeting_name == "Riverina Paceway") and ('harness' in self.race_code) else meeting_label,
                            date = self.chosen_date
                        )

                        # Generate the link
                        link_midpart = event['httpLink']
                        link = f"https://www.sportsbet.com.au/apigw/sportsbook-racing/{link_midpart}WithContext?classId={class_id}"

                        # Fetch race event details
                        runner_dict = {}
                        race_data = await page.evaluate(f"() => fetch('{link}').then(response => response.json())")
                        if not race_data:
                            continue
                        if ('racecardEvent' not in race_data):
                            logger.warning(f"No race data found for {race_name}")
                            continue

                        race_event = race_data['racecardEvent']
                        suspended = False
                        for market in race_event['markets']:
                            # Look for the Fixed Win market
                            if market["name"] == "Win or Place":
                                suspended = not market["livePriceAvailable"]
                                for selection in market['selections']:
                                    if selection['statusCode'] == 'A' and not selection['isOut']:
                                        runner_name = selection['name'].replace(' (Doubtful)', "").replace(' (S)', "")
                                        for price in selection["prices"]:
                                            # Look for the pricing with priceCode "L"
                                            if (price["priceCode"] == "L") and (jsonParam in price):
                                                runner_dict[runner_name] = price[jsonParam]

                        # Populate race dict
                        race_dict = {
                            'runners': runner_dict,
                            'suspended': suspended
                        }
                        all_dict[race_name] = race_dict

            if not markets_present:
                logger.info(f"No {', '.join(self.race_code)} Markets Found for {self.chosen_date}")

            await browser.close()

        logger.info(f"{total_markets_count} Total SportsBet {self.race_code} Futures Markets")
        return all_dict
    
    
class SBSportsScraper:
    def __init__(self, url, chosen_date):
        """
        Sportsbet Scraper initialisation function.

        :param: url str: Link to Sportsbet page that is being scraped.
        :param: race_code str: Race code that is being scraped. Options include "Racing", "Greyhound", "Harness", "International".
        :param: chosen_date str: The date of the race. Should be in this format: YYYY-mm-dd
        """
        self.url = url
        self.chosen_date = chosen_date
        # self.jurisdiction = self.map_jurisdiction(jurisdiction)
    
    async def SPORTSBET_scraper(self, market_type=None, competition_id='none', retries=3, delay=2):
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

            all_markets = await page.evaluate(f"() => fetch('{self.url}').then(response => response.json())")
            if not all_markets:
                logger.error("Failed to fetch markets")
                await browser.close()
                                        
            win_market = {}
                
            for market in all_markets:
                
                if market.get("hasBIRStarted") == 'true':
                    continue
                
                if competition_id != 'none':
                    if market.get("competitionId") != competition_id:
                        continue
                    
                if market_type != None:
                    if market['primaryMarket']['name'] != market_type:
                        continue
                    
                if market.get("eventSort") != 'MTCH':
                    continue
                
                utc_ts = market.get("startTime")  # e.g., 1761371400 (Unix timestamp)
                brisbane = pytz.timezone("Australia/Brisbane")
                
                # Convert Unix timestamp (UTC) → Brisbane datetime
                utc_dt = datetime.utcfromtimestamp(utc_ts).replace(tzinfo=pytz.utc)
                brisbane_dt = utc_dt.astimezone(brisbane)
                brisbane_date = brisbane_dt.date().strftime("%Y-%m-%d")

                                    
                primaryMarket = market['primaryMarket']
                
                selections = primaryMarket['selections']
                market_name = market['displayName']
                
                prices = []
                results = []
                
                for selection in selections:
                    result = selection['name']
                    price = selection['price']['winPrice']
                    results.append(result)
                    prices.append(price)
                
                win_market[market_name, brisbane_date] = {
                    result: price for result, price in zip(results, prices)
                }
                
        return win_market

    async def SPORTSBET_scraper_lines_totals(self, market_kind='line', competition_id='none'):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36 Edg/116.0.1938.81"
            page = await browser.new_page(user_agent=ua)

            async def goto_with_retry(url, retries=3, delay=1.5):
                last_err = None
                for attempt in range(retries):
                    try:
                        await page.goto(url, wait_until="domcontentloaded")
                        return True
                    except Exception as e:
                        last_err = e
                        if attempt < retries - 1:
                            await asyncio.sleep(delay * (attempt + 1))
                logger.error(f"Failed to navigate to Sportsbet URL after retries: {last_err}")
                return False

            async def fetch_json(url, default=None, retries=3, delay=1.0):
                last_err = None
                for attempt in range(retries):
                    try:
                        return await page.evaluate(f"() => fetch('{url}').then(r => r.json())")
                    except Exception as e:
                        last_err = e
                        if attempt < retries - 1:
                            await asyncio.sleep(delay * (attempt + 1))
                logger.warning(f"Sportsbet fetch failed for {url}: {last_err}")
                return default

            ok = await goto_with_retry(self.url)
            if not ok:
                await browser.close()
                return {}

            all_events = await fetch_json(self.url, default=[])
            if not all_events:
                logger.error("Failed to fetch markets")
                await browser.close()
                return {}

            # Build canonical event list from the H2H-style events feed so we get every match id.
            events_url = self.url.replace("primaryMarketOnly=false", "primaryMarketOnly=true")
            events_from_primary = await fetch_json(events_url, default=[])
            event_by_id = {}

            for src in [all_events, events_from_primary]:
                if not isinstance(src, list):
                    continue
                for ev in src:
                    ev_id = ev.get("id") or ev.get("eventId")
                    if ev_id is not None:
                        event_by_id[str(ev_id)] = ev

            events_to_process = list(event_by_id.values())
            if not events_to_process and isinstance(all_events, list):
                events_to_process = all_events

            win_market = {}

            def iter_markets(node):
                if isinstance(node, dict):
                    if isinstance(node.get("selections"), list):
                        yield node
                    for val in node.values():
                        yield from iter_markets(val)
                elif isinstance(node, list):
                    for item in node:
                        yield from iter_markets(item)

            for event in events_to_process:
                if event.get("hasBIRStarted") is True or str(event.get("hasBIRStarted")).lower() == 'true':
                    continue

                if competition_id != 'none' and str(event.get("competitionId")) != str(competition_id):
                    continue

                event_sort = str(event.get("eventSort", "")).upper()
                if event_sort and event_sort != 'MTCH':
                    continue

                event_id = event.get("id") or event.get("eventId")
                if not event_id:
                    continue

                utc_ts = event.get("startTime")
                brisbane = pytz.timezone("Australia/Brisbane")
                utc_dt = datetime.utcfromtimestamp(utc_ts).replace(tzinfo=pytz.utc)
                brisbane_dt = utc_dt.astimezone(brisbane)
                brisbane_date = brisbane_dt.date().strftime("%Y-%m-%d")

                event_prices = {}

                def _extract_hcap_num(selection):
                    for candidate in [selection.get("displayHandicap"), selection.get("unformattedHandicap")]:
                        try:
                            if candidate is not None and str(candidate).strip() != "":
                                return float(str(candidate).replace("+", ""))
                        except (TypeError, ValueError):
                            continue
                    return None

                def collect_market(market):
                    selections = market.get("selections", []) or []
                    if len(selections) != 2:
                        return

                    parsed = []
                    for selection in selections:
                        raw_name = str(selection.get("name") or "").strip()
                        if not raw_name:
                            continue

                        price = (selection.get("price") or {}).get("winPrice")
                        if price is None:
                            continue

                        hcap_num = _extract_hcap_num(selection)

                        if market_kind == 'total':
                            side = None
                            raw_lower = raw_name.lower()
                            if "over" in raw_lower:
                                side = "Over"
                            elif "under" in raw_lower:
                                side = "Under"
                            if side is None:
                                continue

                            mtch = re.search(r'[-+]?\d+(?:\.\d+)?', raw_name)
                            value = abs(float(mtch.group(0))) if mtch else (abs(hcap_num) if hcap_num is not None else None)
                            if value is None:
                                continue

                            parsed.append({
                                "side": side.lower(),
                                "value": round(float(value), 1),
                                "result": f"{side} {abs(float(value)):.1f}",
                                "price": price,
                            })
                            continue

                        # line
                        mtch = re.search(r'([-+]?\d+(?:\.\d+)?)', raw_name)
                        value = None
                        sign = None
                        if mtch:
                            num = float(mtch.group(1))
                            value = abs(num)
                            if num > 0:
                                sign = 1
                            elif num < 0:
                                sign = -1

                        if value is None and hcap_num is not None:
                            value = abs(float(hcap_num))
                        if value is None:
                            continue

                        if sign is None and hcap_num is not None:
                            if float(hcap_num) > 0:
                                sign = 1
                            elif float(hcap_num) < 0:
                                sign = -1

                        team_name = re.sub(r'\s*[-+]?\d+(?:\.\d+)?\s*$', '', raw_name).strip()
                        if not team_name:
                            team_name = raw_name

                        parsed.append({
                            "team": team_name,
                            "value": round(float(value), 1),
                            "sign": sign,
                            "price": price,
                        })

                    if len(parsed) != 2:
                        return

                    if market_kind == 'total':
                        sides = {item["side"] for item in parsed}
                        values = {item["value"] for item in parsed}
                        if sides != {"over", "under"} or len(values) != 1:
                            return
                        for item in parsed:
                            event_prices[item["result"]] = item["price"]
                        return

                    # line: require a true two-sided handicap pair with same abs value
                    if abs(parsed[0]["value"] - parsed[1]["value"]) > 0.11:
                        return

                    s0, s1 = parsed[0]["sign"], parsed[1]["sign"]
                    if s0 is None and s1 is None:
                        if parsed[0]["price"] == parsed[1]["price"]:
                            s0, s1 = -1, 1
                        else:
                            s0 = -1 if parsed[0]["price"] < parsed[1]["price"] else 1
                            s1 = -s0
                    elif s0 is None:
                        s0 = -s1
                    elif s1 is None:
                        s1 = -s0

                    if s0 == s1 or s0 == 0 or s1 == 0:
                        return

                    value = round(parsed[0]["value"], 1)
                    event_prices[f"{parsed[0]['team']} {s0 * value:+.1f}"] = parsed[0]["price"]
                    event_prices[f"{parsed[1]['team']} {s1 * value:+.1f}"] = parsed[1]["price"]

                def maybe_collect_market(market):
                    market_name = str(market.get("name", "")).lower()
                    selections = market.get("selections", []) or []
                    selection_names = [str(sel.get("name", "")).lower() for sel in selections]

                    if any(token in market_name for token in ["1st", "2nd", "half", "period", "quarter", "team", "margin"]):
                        return
                    if market_kind == 'line':
                        has_line_name = "line" in market_name or "handicap" in market_name
                        has_line_selections = (
                            len(selection_names) >= 2 and
                            sum(1 for n in selection_names if re.search(r'[+-]\d+(?:\.\d+)?', n)) >= 2
                        )
                        if not has_line_name and not has_line_selections:
                            return
                    else:
                        has_total_name = "total" in market_name or "points" in market_name or "score" in market_name
                        has_over_under = (
                            len(selection_names) >= 2 and
                            any("over" in n for n in selection_names) and
                            any("under" in n for n in selection_names)
                        )
                        if not has_total_name and not has_over_under:
                            return
                    collect_market(market)

                async def fetch_json_candidates(urls):
                    for url in urls:
                        if not url:
                            continue
                        data = await fetch_json(url, default=None, retries=2, delay=0.8)
                        if data:
                            return data
                    return None

                # First pass: try markets directly on the event payload
                maybe_collect_market(event.get("primaryMarket") or {})
                for market in event.get("marketList", []) or []:
                    maybe_collect_market(market)

                # Direct event markets endpoint (contains extra markets beyond base events list).
                direct_markets_url = f"https://www.sportsbet.com.au/apigw/sportsbook-sports/Sportsbook/Sports/Events/{event_id}/Markets"
                direct_markets = await fetch_json(direct_markets_url, default=[])
                if isinstance(direct_markets, dict):
                    direct_markets = direct_markets.get("markets") or direct_markets.get("items") or []
                if isinstance(direct_markets, list):
                    for market in direct_markets:
                        maybe_collect_market(market)

                # Fallback: query detailed market groupings per event
                groupings_url = f"https://www.sportsbet.com.au/apigw/sportsbook-sports/Sportsbook/Sports/Events/{event_id}/MarketGroupings"
                groupings = await fetch_json(groupings_url, default=[])
                if isinstance(groupings, dict):
                    groupings = groupings.get("marketGroupings") or groupings.get("items") or []
                if not isinstance(groupings, list):
                    groupings = []

                for grouping in groupings:
                    grouping_id = grouping.get("id")
                    if grouping_id is None:
                        continue

                    markets_url = f"https://www.sportsbet.com.au/apigw/sportsbook-sports/Sportsbook/Sports/Events/{event_id}/MarketGroupings/{grouping_id}/Markets"
                    markets = await fetch_json(markets_url, default=[])
                    if isinstance(markets, dict):
                        markets = markets.get("markets") or markets.get("items") or []
                    if not isinstance(markets, list):
                        markets = []

                    for market in markets:
                        maybe_collect_market(market)

                # Hard fallback for Sportsbet "Game Lines" grouping id used in this codebase.
                for fallback_gid in [229]:
                    markets_url = f"https://www.sportsbet.com.au/apigw/sportsbook-sports/Sportsbook/Sports/Events/{event_id}/MarketGroupings/{fallback_gid}/Markets"
                    markets = await fetch_json(markets_url, default=[])
                    if isinstance(markets, dict):
                        markets = markets.get("markets") or markets.get("items") or []
                    if not isinstance(markets, list):
                        continue
                    for market in markets:
                        maybe_collect_market(market)

                # Follow event links to fetch the sport card with deeper markets.
                raw_http_link = str(event.get("httpLink") or "").lstrip("/")
                raw_topic_link = str(event.get("topicLink") or "").lstrip("/")
                event_id_str = str(event_id)

                candidate_paths = []
                if raw_http_link:
                    candidate_paths.append(raw_http_link)
                    candidate_paths.append(raw_http_link.replace("Sportsbet/Sports/", "Sportsbet/Sportsbook/Sports/"))
                if raw_topic_link:
                    candidate_paths.append(raw_topic_link)
                candidate_paths.extend([
                    f"Sportsbook/Sports/Events/{event_id_str}/SportCard",
                    f"Sportsbet/Sports/Events/{event_id_str}/SportCard",
                    f"Sportsbet/Sportsbook/Sports/Events/{event_id_str}/SportCard",
                    f"Sportsbook/Sports/Events/{event_id_str}/WithContext",
                    f"Sportsbet/Sportsbook/Sports/Events/{event_id_str}/WithContext",
                ])

                candidate_urls = [
                    f"https://www.sportsbet.com.au/apigw/sportsbook-sports/{path}"
                    for path in candidate_paths if path
                ]

                sport_card = await fetch_json_candidates(candidate_urls)
                if sport_card:
                    for market in iter_markets(sport_card):
                        maybe_collect_market(market)

                if len(event_prices) >= 2:
                    event_name = event.get("displayName") or event.get("name")
                    win_market[event_name, brisbane_date] = event_prices

            await browser.close()
            return win_market
    

    async def SPORTSBET_scraper_football(self, competition_id='none', retries=3, delay=2):
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

            all_markets = await page.evaluate(f"() => fetch('{self.url}').then(response => response.json())")
            if not all_markets:
                logger.error("Failed to fetch markets")
                await browser.close()
                                        
            win_market = {}
                
            for market in all_markets:
                
                if market.get("hasBIRStarted") == 'true':
                    continue
                
                if competition_id != 'none':
                    if market.get("competitionId") != competition_id:
                        continue
                    
                if market.get("eventSort") != 'MTCH':
                    continue
                    
                primaryMarket = market['primaryMarket']
                
                if primaryMarket['name'] != "Win-Draw-Win":
                    continue
                                
                utc_ts = market.get("startTime")  # e.g., 1761371400 (Unix timestamp)
                brisbane = pytz.timezone("Australia/Brisbane")
                
                # Convert Unix timestamp (UTC) → Brisbane datetime
                utc_dt = datetime.utcfromtimestamp(utc_ts).replace(tzinfo=pytz.utc)
                brisbane_dt = utc_dt.astimezone(brisbane)
                brisbane_date = brisbane_dt.date().strftime("%Y-%m-%d")

                
                selections = primaryMarket['selections']
                market_name = market['displayName']
                
                prices = []
                results = []
                
                for selection in selections:
                    result = selection['name']
                    price = selection['price']['winPrice']
                    results.append(result)
                    prices.append(price)
                
                win_market[market_name, brisbane_date] = {
                    result: price for result, price in zip(results, prices)
                }
                
        return win_market
    
    async def SPORTSBET_scraper_union(self, competition_id='none'):
        """
        Union Sportsbet Scraper.
        """
        async with async_playwright() as p:
            # Stealth Browser Set Up to Access Sportsbet API (Not Needed but just copied over from TAB)
            browser = await p.chromium.launch(headless=True)
            ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36 Edg/116.0.1938.81"
            page = await browser.new_page(user_agent=ua)

            await page.goto(self.url)

            all_events = await page.evaluate(f"() => fetch('{self.url}').then(response => response.json())")
            if not all_events:
                logger.error("Failed to fetch markets")
                await browser.close()
    
            draw_no_bet_markets = {}
    
            # Step 2: Loop through each event
            for event in all_events:
                
                if event.get("hasBIRStarted") == 'true':
                    continue
    
                # Filter by competition if needed
                if competition_id != 'none':
                    if event.get("competitionId") != competition_id:
                        continue
    
                if event.get("eventSort") != "MTCH":
                    continue
    
                event_id = event.get("id")
                if not event_id:
                    continue
                                
                utc_ts = event.get("startTime")  # e.g., 1761371400 (Unix timestamp)
                brisbane = pytz.timezone("Australia/Brisbane")
                
                # Convert Unix timestamp (UTC) → Brisbane datetime
                utc_dt = datetime.utcfromtimestamp(utc_ts).replace(tzinfo=pytz.utc)
                brisbane_dt = utc_dt.astimezone(brisbane)
                brisbane_date = brisbane_dt.date().strftime("%Y-%m-%d")

    
                # Step 3: Fetch the market groupings for this event
                market_url = f"https://www.sportsbet.com.au/apigw/sportsbook-sports/Sportsbook/Sports/Events/{event_id}/MarketGroupings/229/Markets"
                markets = await page.evaluate(f"() => fetch('{market_url}').then(r => r.json())")
                if not markets:
                    logger.warning(f"No markets found for event {event_id}")
                    continue
    
                # Step 4: Look for 'Draw No Bet' market
                for market in markets:
                    if market.get("name") == "Draw No Bet":
                        selections = market.get("selections", [])
                        prices = {sel["name"]: sel["price"]["winPrice"] for sel in selections}
                        draw_no_bet_markets[event.get("displayName"), brisbane_date] = prices
                        break  # Only need the first Draw No Bet market
    
            await browser.close()
            return draw_no_bet_markets
        
        
    async def SPORTSBET_scrape_mma(self, competition_id='none', retries=3, delay=2):
        """
        MMA Sportsbet Scraper.
        """
        # Input checking

        async with async_playwright() as p:
            # Stealth Browser Set Up to Access Sportsbet API (Not Needed but just copied over from TAB)
            browser = await p.chromium.launch(headless=True)
            ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36 Edg/116.0.1938.81"
            page = await browser.new_page(user_agent=ua)

            await page.goto(self.url)

            all_comps = await page.evaluate(f"() => fetch('{self.url}').then(response => response.json())")
            if not all_comps:
                logger.error("Failed to fetch markets")
                await browser.close()
                                        
            win_market = {}
                
            for comp in all_comps:
                
                if competition_id != 'none':
                    if comp.get("id") != competition_id:
                        continue
                    
                for event in comp.get("events"):
                    
                    event_name = event['name']
                    
                    utc_ts = event.get("startTime")  # e.g., 1761371400 (Unix timestamp)
                    brisbane = pytz.timezone("Australia/Brisbane")
                    
                    # Convert Unix timestamp (UTC) → Brisbane datetime
                    utc_dt = datetime.utcfromtimestamp(utc_ts).replace(tzinfo=pytz.utc)
                    brisbane_dt = utc_dt.astimezone(brisbane)
                    brisbane_date = brisbane_dt.date().strftime("%Y-%m-%d")

                    
                    if event.get("hasBIRStarted") == 'true':
                        continue
                    
                    for market in event.get("marketList"):
                        if market.get("name") != 'Match Betting':
                            continue
                    
                        prices = []
                        results = []
                        
                        for selection in market.get("selections"):
                            result = selection['name']
                            price = selection['price']['winPrice']
                            results.append(result)
                            prices.append(price)
                        
                        win_market[event_name, brisbane_date] = {
                            result: price for result, price in zip(results, prices)
                        }
                    
        return win_market
