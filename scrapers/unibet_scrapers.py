import time
import asyncio
import traceback
import re

from loguru                       import logger
from random                       import randrange
from playwright_stealth           import stealth_async
from playwright.async_api         import async_playwright
from datetime import datetime
from zoneinfo import ZoneInfo
 
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
                    
                    if event.get("state") != 'NOT_STARTED':
                        continue
                        
                    match_name = event.get("englishName") or event.get("name")
                    
                    date = event.get("start")
                    dt_utc = datetime.fromisoformat(date.replace("Z", "+00:00"))
                    brisbane_dt = dt_utc.astimezone(ZoneInfo("Australia/Brisbane"))
                    brisbane_date = brisbane_dt.date().strftime("%Y-%m-%d")

                    if not match_name:
                        continue

                    win_market.setdefault((match_name, brisbane_date), {})

                    for bet_offer in comp.get("betOffers", []):
                        bet_type = bet_offer.get("betOfferType", {}).get("englishName", "")
                        if bet_type.lower() == "match":  # Match odds only
                            for outcome in bet_offer.get("outcomes", []):
                                player = outcome.get("englishLabel")
                                odds = outcome.get("oddsDecimal")
                                try:
                                    win_market[match_name, brisbane_date][player] = float(odds)
                                except (TypeError, ValueError):
                                    win_market[match_name, brisbane_date][player] = None

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
                    
                    if event.get("state") != 'NOT_STARTED':
                        continue
                    
                    match_name = event.get("englishName") or event.get("name")

                    if not match_name:
                        continue
                    
                    date = event.get("start")
                    dt_utc = datetime.fromisoformat(date.replace("Z", "+00:00"))
                    brisbane_dt = dt_utc.astimezone(ZoneInfo("Australia/Brisbane"))
                    brisbane_date = brisbane_dt.date().strftime("%Y-%m-%d")

                    win_market.setdefault((match_name, brisbane_date), {})

                    for bet_offer in comp.get("betOffers", []):
                        bet_type = bet_offer.get("betOfferType", {}).get("englishName", "")
                        if bet_type.lower() == "match":  # Match odds only
                            for outcome in bet_offer.get("outcomes", []):
                                player = outcome.get("participant")
                                odds = outcome.get("oddsDecimal")
                                try:
                                    win_market[match_name, brisbane_date][player] = float(odds)
                                except (TypeError, ValueError):
                                    win_market[match_name, brisbane_date][player] = None

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
                
                date = event.get("start")
                dt_utc = datetime.fromisoformat(date.replace("Z", "+00:00"))
                brisbane_dt = dt_utc.astimezone(ZoneInfo("Australia/Brisbane"))
                brisbane_date = brisbane_dt.date().strftime("%Y-%m-%d")
                
                if event.get("state") != 'NOT_STARTED':
                    continue
                    
                match_name = event.get("englishName") or event.get("name")

                if not match_name:
                    continue

                win_market.setdefault((match_name, brisbane_date), {})

                for bet_offer in game.get("betOffers", []):
                    bet_type = bet_offer.get("betOfferType", {}).get("englishName", "")
                    if bet_type.lower() == "match":  # Match odds only
                        for outcome in bet_offer.get("outcomes", []):
                            team = outcome.get("englishLabel")
                            odds = outcome.get("oddsDecimal")
                            try:
                                win_market[match_name, brisbane_date][team] = float(odds)
                            except (TypeError, ValueError):
                                win_market[match_name, brisbane_date][team] = None

        return win_market
    
    async def UNIBET_scrape_sport(self, comp=None, market_type="Match", include_line=False, market_match_mode="exact"):
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
                
                if event.get("state") != 'NOT_STARTED':
                    continue
                    
                match_name = event.get("englishName") or event.get("name")
                
                date = event.get("start")
                dt_utc = datetime.fromisoformat(date.replace("Z", "+00:00"))
                brisbane_dt = dt_utc.astimezone(ZoneInfo("Australia/Brisbane"))
                brisbane_date = brisbane_dt.date().strftime("%Y-%m-%d")
        
                if not match_name:
                    continue
        
                win_market.setdefault((match_name, brisbane_date), {})
        
                for bet_offer in comp.get("betOffers", []):
                    bet_type = str(bet_offer.get("betOfferType", {}).get("englishName", ""))
                    bet_type_l = bet_type.lower()

                    if isinstance(market_type, (list, tuple, set)):
                        targets = [str(t).lower() for t in market_type]
                    else:
                        targets = [str(market_type).lower()]

                    if market_match_mode == "contains":
                        if not any(t in bet_type_l for t in targets):
                            continue
                    else:
                        if bet_type_l not in targets:
                            continue

                    if not bet_type:
                        continue

                    for outcome in bet_offer.get("outcomes", []):
                        team = outcome.get("participant") or outcome.get("englishLabel") or ""
                        if include_line:
                            line = outcome.get("line")
                            try:
                                if line is not None:
                                    team = f"{team} {float(line) / 1000:+.1f}"
                            except (TypeError, ValueError):
                                pass

                        odds = outcome.get("oddsDecimal")
                        try:
                            win_market[match_name, brisbane_date][team] = float(odds)
                        except (TypeError, ValueError):
                            win_market[match_name, brisbane_date][team] = None
        
        
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

    async def UNIBET_scrape_nrl_tryscorers(self, comp="NRL"):
        win_market = {}

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36 Edg/116.0.1938.81"
            page = await browser.new_page(user_agent=ua)

            await page.goto(self.url)
            all_markets = await page.evaluate(f"() => fetch('{self.url}').then(response => response.json())")
            if not all_markets:
                logger.error("Failed to fetch markets")
                await browser.close()
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

            def process_event(comp_event):
                event = comp_event.get("event", {})
                if event.get("state") != 'NOT_STARTED':
                    return

                match_name = event.get("englishName") or event.get("name")
                date = event.get("start")
                if not match_name or not date:
                    return
                dt_utc = datetime.fromisoformat(date.replace("Z", "+00:00"))
                brisbane_dt = dt_utc.astimezone(ZoneInfo("Australia/Brisbane"))
                brisbane_date = brisbane_dt.date().strftime("%Y-%m-%d")

                prices = win_market.setdefault((match_name, brisbane_date), {})
                for bet_offer in comp_event.get("betOffers", []):
                    bet_type = (
                        bet_offer.get("betOfferType", {}).get("englishName")
                        or bet_offer.get("criterion", {}).get("englishLabel")
                        or bet_offer.get("criterion", {}).get("label")
                        or bet_offer.get("englishName")
                        or bet_offer.get("label")
                        or ""
                    )
                    tries = try_count(bet_type)
                    if tries not in {1, 2, 3}:
                        continue
                    for outcome in bet_offer.get("outcomes", []):
                        player = (
                            outcome.get("participant")
                            or outcome.get("englishLabel")
                            or outcome.get("label")
                            or outcome.get("englishName")
                        )
                        odds = outcome.get("oddsDecimal")
                        if odds is None and outcome.get("odds") is not None:
                            try:
                                odds = float(outcome.get("odds")) / 1000
                            except (TypeError, ValueError):
                                odds = None
                        if not player or odds is None:
                            continue
                        player = re.sub(r'\s+\d\+$', '', str(player)).strip()
                        if player.lower() in {"no try", "no tryscorer"}:
                            continue
                        try:
                            prices[f"{player} {tries}+"] = float(odds)
                        except (TypeError, ValueError):
                            pass

            def find_first_key(node, wanted):
                if isinstance(node, dict):
                    for key, val in node.items():
                        if key == wanted and val not in (None, ""):
                            return val
                        found = find_first_key(val, wanted)
                        if found not in (None, ""):
                            return found
                elif isinstance(node, list):
                    for item in node:
                        found = find_first_key(item, wanted)
                        if found not in (None, ""):
                            return found
                return None

            def find_first_any_key(node, wanted_keys):
                for key in wanted_keys:
                    found = find_first_key(node, key)
                    if found not in (None, ""):
                        return found
                return None

            async def fetch_kambi_settings():
                settings_url = "https://www.unibet.com.au/sportsbook-feeds/settings"
                try:
                    settings = await page.evaluate(f"() => fetch('{settings_url}').then(response => response.json())")
                except Exception as e:
                    logger.warning(f"Unibet settings fetch failed: {e}")
                    return None

                sports_client = ((settings or {}).get("cmsSettings") or {}).get("sportsClient") or {}
                gs_url = sports_client.get("apiUrl")
                if not gs_url:
                    return settings

                if str(gs_url).startswith("/"):
                    gs_url = f"https://www.unibet.com.au{gs_url}"
                try:
                    gs_settings = await page.evaluate(f"() => fetch('{gs_url}').then(response => response.json())")
                except Exception as e:
                    logger.warning(f"Unibet game launcher settings fetch failed: {e}")
                    return settings

                return {
                    "sportsbook_settings": settings,
                    "game_launcher_settings": gs_settings,
                }

            async def fetch_and_process_kambi_event(event_id, base_event):
                if not event_id:
                    return

                settings = await fetch_kambi_settings()
                if not settings:
                    return

                base_url = find_first_any_key(settings, [
                    "kambiOfferingApiBaseUrl",
                    "kambiApiBaseUrl",
                    "offeringApiBaseUrl",
                    "apiOfferingBaseUrl",
                ])
                offering = find_first_any_key(settings, [
                    "offering",
                    "kambiOffering",
                    "kambiOfferingPath",
                    "offeringPath",
                ])
                lang = find_first_any_key(settings, ["lang", "languageCode", "locale"]) or "en_AU"
                market = find_first_any_key(settings, ["market", "countryCode"]) or "AU"
                if not base_url or not offering:
                    logger.debug(
                        "Unibet Kambi settings missing offering API config. "
                        f"base_url_found={bool(base_url)} offering_found={bool(offering)}"
                    )
                    return

                base_url = str(base_url).rstrip("/")
                base_url = base_url.rstrip("/v2018")
                offering = str(offering).strip("/")
                event_url = (
                    f"{base_url}/v2018/{offering}/betoffer/event/{event_id}.json"
                    f"?lang={lang}&market={market}"
                )
                try:
                    detail = await page.evaluate(f"() => fetch('{event_url}').then(response => response.json())")
                except Exception as e:
                    logger.warning(f"Unibet Kambi event fetch failed for {event_id}: {e}")
                    return

                comp_event = {
                    "event": base_event,
                    "betOffers": detail.get("betOffers", []) if isinstance(detail, dict) else [],
                }
                process_event(comp_event)

            async def fetch_and_process_detail(comp_event):
                event_id = (comp_event.get("event") or {}).get("id")
                if not event_id:
                    return
                detail_url = (
                    "https://www.unibet.com.au/sportsbook-feeds/views/filter/"
                    f"rugby_league/nrl/matches/{event_id}?includeParticipants=true&useCombined=true"
                )
                try:
                    detail = await page.evaluate(f"() => fetch('{detail_url}').then(response => response.json())")
                except Exception as e:
                    logger.warning(f"Unibet event detail fetch failed for {event_id}: {e}")
                    return

                def walk(node):
                    if isinstance(node, dict):
                        if isinstance(node.get("betOffers"), list) and isinstance(node.get("event"), dict):
                            process_event(node)
                        for val in node.values():
                            walk(val)
                    elif isinstance(node, list):
                        for item in node:
                            walk(item)

                walk(detail)

            try:
                groups = all_markets["layout"]["sections"][1]["widgets"][0]["matches"]["groups"]
            except (KeyError, IndexError, TypeError) as e:
                logger.error(f"Unexpected JSON structure from Unibet: {e}")
                await browser.close()
                return win_market

            for group in groups:
                if comp and group.get("name") != comp:
                    continue
                if group.get("subGroups"):
                    for subgroup in group["subGroups"]:
                        for comp_event in subgroup.get("events", []):
                            process_event(comp_event)
                            await fetch_and_process_detail(comp_event)
                            await fetch_and_process_kambi_event(
                                (comp_event.get("event") or {}).get("id"),
                                comp_event.get("event") or {},
                            )
                else:
                    for comp_event in group.get("events", []):
                        process_event(comp_event)
                        await fetch_and_process_detail(comp_event)
                        await fetch_and_process_kambi_event(
                            (comp_event.get("event") or {}).get("id"),
                            comp_event.get("event") or {},
                        )

            await browser.close()
            return {k: v for k, v in win_market.items() if v}
    
    
    
    
