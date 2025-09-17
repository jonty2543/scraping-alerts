import time
import asyncio, random
import traceback

from loguru                       import logger
from random                       import randrange
from playwright_stealth           import stealth_async
from playwright.async_api         import async_playwright
 
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
                markets = event['specialFixedOddsMarkets']
                
                if event.get("isLive") == 'true':
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
                    
                    win_market[market_name] = {
                        result: price for result, price in zip(results, prices)
                    }
                
        return win_market
    
    async def POINTSBET_scrape_nrl(self, market_type):
        """
        Nrl Pointsbet Scraper.
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
                markets = event['specialFixedOddsMarkets']
                
                if event.get("isLive") == 'true':
                    continue
                
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
                    
                    win_market[market_name] = {
                        results[0]: prices[0],
                        results[1]: prices[1]
                    }
                
        return win_market
    
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
                
                if event.get("isLive") == 'true':
                    continue

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
                    
                    win_market[market_name] = {
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
                                
                        
                        
                        
        
                        
                        

                        
                        
                    

                    
                    
                
        
        
    
    
    
    
