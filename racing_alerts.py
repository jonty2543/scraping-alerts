import pytz
from datetime import datetime
from loguru import logger
import sportsbet_scrapers as sb
import asyncio
import nest_asyncio
nest_asyncio.apply()

async def main():
    
    chosen_date = datetime.now(pytz.timezone("Australia/Brisbane")).date().strftime("%Y-%m-%d")
    print(chosen_date)

    sb_url = f"https://www.sportsbet.com.au/apigw/sportsbook-racing/Sportsbook/Racing/AllRacing/{chosen_date}"
    racing_codes = ['Greyhound']

    for racing_code in racing_codes:
        price_modes = ['win']
        for mode in price_modes:
            sb_markets_dict = {}
            runners = {}
            
            logger.info(f"Scraping Sportsbet {racing_code} Data")
            sb_scraper = sb.SBRacingScraper(sb_url, racing_code, chosen_date)
            sb_markets_dict[racing_code] = await sb_scraper.SPORTSBET_scrape(mode)
            
            if sb_markets_dict[racing_code] is None:
                logger.info(f"Could not retrieve Sportsbet {racing_code} Market Data")
                
            sb_market_names = list(sb_markets_dict[racing_code].keys())
            logger.info(f"All Sportsbet Market Names for {racing_code}: {sb_market_names}")
            
            for market in sb_market_names:
                for key, value in zip(sb_markets_dict[racing_code][market]['runners'].keys(),
                                    sb_markets_dict[racing_code][market]['runners'].values()):
                    runners[key] = {'market': market, 'name': key, 'price': value}

if __name__ == "__main__":
    asyncio.run(main())
        
        