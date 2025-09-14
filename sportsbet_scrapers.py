import time
import asyncio
import traceback
import json

from loguru                       import logger
from random                       import randrange
from playwright_stealth           import stealth_async
from playwright.async_api         import async_playwright

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
                                        runner_name = selection['name'].replace(' \(Doubtful\)', "").replace(' \(S\)', "")
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
    
    async def SPORTSBET_scraper(self, competition_id='none', retries=3, delay=2):
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
                
                if competition_id != 'none':
                    if market.get("competitionId") != competition_id:
                        continue
                    
                if market.get("eventSort") != 'MTCH':
                    continue
                    
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
                
                win_market[market_name] = {
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
    
                # Filter by competition if needed
                if competition_id != 'none':
                    if event.get("competitionId") != competition_id:
                        continue
    
                if event.get("eventSort") != "MTCH":
                    continue
    
                event_id = event.get("id")
                if not event_id:
                    continue
    
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
                        draw_no_bet_markets[event.get("displayName")] = prices
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
                        
                        win_market[event_name] = {
                            result: price for result, price in zip(results, prices)
                        }
                    
        return win_market