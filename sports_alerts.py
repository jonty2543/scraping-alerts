import pytz
from datetime import datetime, timedelta
from loguru import logger

import sportsbet_scrapers as sb
import pointsbet_scrapers as pb
import unibet_scrapers as ub
import PalmerBet_scrapers as palm
import betr_scrapers as betr
import betright_scrapers as br
import betdeluxe_scrapers as bd
import surge_scrapers as ss

import functions as f

import asyncio
import nest_asyncio
import pandas as pd
import numpy as np
import requests
from rapidfuzz import process, fuzz
from thefuzz import fuzz, process
from functools import reduce
import re
import unidecode
from supabase import create_client, Client
import inspect
import time
import boto3
from io import StringIO

nest_asyncio.apply()

async def main():
    
    # %%
    chosen_date = datetime.now(pytz.timezone("Australia/Brisbane")).date().strftime("%Y-%m-%d")
    offset = (datetime.now(pytz.timezone("Australia/Brisbane")).date().weekday() - 0) % 7  
    monday = datetime.now(pytz.timezone("Australia/Brisbane")).date() - timedelta(days=offset)
    one_week = (datetime.now(pytz.timezone("Australia/Brisbane")) + timedelta(14)).date().strftime("%Y-%m-%d")
    two_week = (datetime.now(pytz.timezone("Australia/Brisbane")) + timedelta(14)).date().strftime("%Y-%m-%d")
    print(chosen_date)
    print(one_week)
    print(two_week)
    print(monday)
    
    pb_union_url = f'https://api.au.pointsbet.com/api/mes/v3/events/featured/competition/15797?page=1'
    pb_nrl_url = f'https://api.au.pointsbet.com/api/mes/v3/events/featured/competition/7593?page=1'

    palm_union_url = f'https://fixture.palmerbet.online/fixtures/sports/5bfcf787-edfc-48f4-b328-1d221aa07ae0/matches?sportType=rugbyunion&pageSize=1000&channel=website'
    palm_tennis_url = f'https://fixture.palmerbet.online/fixtures/sports/9d6bbedd-0b09-4031-9884-e264499a2aa5/matches?sportType=tennis&pageSize=1000&channel=website'
    palm_nrl_url = f'https://fixture.palmerbet.online/fixtures/sports/cf404de1-1953-4d55-b92e-4e022f186b22/matches?sportType=rugbyleague&pageSize=1000&channel=website'
    palm_football_url = f'https://fixture.palmerbet.online/fixtures/sports/b4073512-cdd5-4953-950f-3f7ad31fa955/matches?sportType=Soccer&pageSize=1000'
    palm_basketball_url = f'https://fixture.palmerbet.online/fixtures/sports/b26e5acc-02ff-4b22-ae69-0491fbd2500e/matches?sportType=basketball&pageSize=24&channel=website'

    betr_union_url = 'https://web20-api.bluebet.com.au/MasterCategory?EventTypeId=105&WithLevelledMarkets=true'
    betr_nrl_url = 'https://web20-api.bluebet.com.au/MasterCategory?EventTypeId=102&WithLevelledMarkets=true'
    betr_mma_url = 'https://web20-api.bluebet.com.au/MasterCategory?EventTypeId=128&WithLevelledMarkets=true'
    
    sb_ufc_url = 'https://www.sportsbet.com.au/apigw/sportsbook-sports/Sportsbook/Sports/Class/71/Events?displayType=coupon&detailsLevel=O'


    WEBHOOK_ARBS = "https://discord.com/api/webhooks/1407711750824132620/dBAZkjoBIHPV-vUNk7C0E4MJ7nUtF1BKd4O1lpHhq_4qbk-47kew9bFmRiSpELAqk6i4"  
    WEBHOOK_PROBS = "https://discord.com/api/webhooks/1408080883885539439/bAnj7a_NBVxFyXecCMTBw84obdX4OMuO1388GDNfo1ViW4Kmylb0Gc5bITjrfiEOwRNc"  
    WEBHOOK_TEST = "https://discord.com/api/webhooks/1408057314455588864/jAclediH3bdFu-0PXK4Xbd7wykeU0NgJueMEaEwP8x3vJAExfZ-RFAT0FAdwT-alP2D4"

    # ---- Supabase credentials ----
    SUPABASE_URL = "https://glrzwxpxkckxaogpkwmn.supabase.co"
    SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imdscnp3eHB4a2NreGFvZ3Brd21uIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1NjA3OTU3NiwiZXhwIjoyMDcxNjU1NTc2fQ.YOF9ryJbhBoKKHT0n4eZDMGrR9dczR8INHVs_By4vRU"
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    
    
    # %% S3 Model Data
    
    #NPC
    s3 = boto3.client('s3')
    
    bucket_name = "model-prices"
    key = f"npc/{monday}"
    

    try:
        response = s3.get_object(Bucket=bucket_name, Key=key)
        npc_csv = response['Body'].read().decode('utf-8')
        npc_model_data = pd.read_csv(StringIO(npc_csv))
    
        model_union_markets = {
            row["Match"]: {
                row["HomeTeam"]: round(row["HomePrice"], 2),
                row["AwayTeam"]: round(row["AwayPrice"], 2)
            }
            for _, row in npc_model_data.iterrows()
        }
    except :
        model_union_markets = {}
        logger.info(f'No model prices for union')

    
    # %% #---------Football--------#
    
    pb_football_compids = f.get_pb_comps('soccer')           
        
    logger.info(f"Scraping Pointsbet Football Data")
    pb_football_markets = {}
    for comp_id in pb_football_compids:
        pb_scraper = pb.PBSportsScraper(f.get_pb_url(comp_id),  chosen_date=chosen_date)
        comp_markets = await pb_scraper.POINTSBET_scrape_sport(market_type='Match Result')
        pb_football_markets.update(comp_markets)
        
    time.sleep(5)
    
    logger.info(f"Scraping Sportsbet Football Data")
    sb_scraper = sb.SBSportsScraper(f.get_sportsbet_url(29),  chosen_date=chosen_date)
    sb_football_markets = await sb_scraper.SPORTSBET_scraper_football()
    
    time.sleep(5)
    
    '''
    logger.info(f"Scraping Unibet football Data")
    ub_scraper = ub.UBSportsScraper(get_ub_url('football'),  chosen_date=chosen_date)
    ub_football_markets = await ub_scraper.UNIBET_scrape_football()'''
    
    logger.info(f"Scraping Palmerbet football Data")
    palm_scraper = palm.PalmerBetSportsScraper(palm_football_url,  chosen_date=chosen_date)
    palm_football_markets = await palm_scraper.PalmerBet_scrape(comp=None, sport='football')
    
    time.sleep(5)
    
    logger.info("Scraping betright football data")
    br_scraper = br.BRSportsScraper(f.get_betright_url(100),  chosen_date=chosen_date)
    br_football_markets = await br_scraper.BETRIGHT_scrape_football()
    
    time.sleep(5)
    
    logger.info("Scraping betdeluxe football data")
    bd_scraper = bd.BDSportsScraper(f.get_betdeluxe_url('soccer'), chosen_date=chosen_date)
    bd_football_markets = await bd_scraper.BETDELUXE_scraper(sport='soccer')
        
    # Football df
    bookmakers = {
        "Sportsbet": sb_football_markets,
        "Pointsbet": pb_football_markets,
        "Palmerbet": palm_football_markets,
        "Betright": br_football_markets,
        "Betdeluxe": bd_football_markets
    }
    
    price_cols = ['Sportsbet', 'Pointsbet', 'Palmerbet', 'Betright', 'Betdeluxe']  # , 'Unibet']
    
    f.process_odds(bookmakers, price_cols, table_name="Football Odds", outcomes=3)

    
    
    # %% #---------Tennis--------#
    pb_tennis_compids = f.get_pb_comps('tennis')

    logger.info(f"Scraping Pointsbet tennis Data")
    pb_tennis_markets = {}
    for comp_id in pb_tennis_compids:
        pb_scraper = pb.PBSportsScraper(f.get_pb_url(comp_id),  chosen_date=chosen_date)
        comp_markets = await pb_scraper.POINTSBET_scrape_sport(market_type='Match Result')
        pb_tennis_markets.update(comp_markets)
    
    logger.info(f"Scraping Sportsbet tennis Data")
    sb_scraper = sb.SBSportsScraper(f.get_sportsbet_url(13),  chosen_date=chosen_date)
    sb_tennis_markets = await sb_scraper.SPORTSBET_scraper()
    
    time.sleep(5)
    
    logger.info(f"Scraping Unibet tennis Data")
    ub_scraper = ub.UBSportsScraper(f.get_ub_url('tennis'),  chosen_date=chosen_date)
    ub_tennis_markets = await ub_scraper.UNIBET_scrape_tennis()
    
    time.sleep(5)
    
    logger.info("Scraping betdeluxe tennis data")
    bd_scraper = bd.BDSportsScraper(f.get_betdeluxe_url('tennis'), chosen_date=chosen_date)
    bd_tennis_markets = await bd_scraper.BETDELUXE_scraper(sport='tennis')
    
    time.sleep(5)
    
    surge_tennis_compids = f.get_surge_comps('tennis')           
        
    logger.info(f"Scraping Surgebet Tennis Data")
    surge_tennis_markets = {}
    for comp_id in surge_tennis_compids:
        surge_scraper = ss.SurgeSportsScraper(f.get_surge_url('tennis', comp_id),  chosen_date=chosen_date)
        comp_markets = await surge_scraper.Surge_scrape(market ='Match Result')
        surge_tennis_markets.update(comp_markets)
        
    time.sleep(5)
    
    #tennis df
    bookmakers = {
        "Sportsbet": sb_tennis_markets,
        "Pointsbet": pb_tennis_markets,
        "Unibet": ub_tennis_markets,
        "Betdeluxe": bd_tennis_markets,
        "SurgeBet": surge_tennis_markets
    }
    
    price_cols = ['Sportsbet', 'Pointsbet', 'Unibet', 'Betdeluxe', 'SurgeBet']  # , 'Unibet']'''
    
    f.process_odds(
        bookmakers,
        price_cols,
        "Tennis Odds",      # table_name
        match_threshold=70, # optional
        names=True          # optional
    )
        
    
    # %% #---------union--------#
    logger.info(f"Scraping Sportsbet Union Data")
    sb_scraper = sb.SBSportsScraper(f.get_sportsbet_url(sportId=12),  chosen_date=chosen_date)
    sb_union_markets = await sb_scraper.SPORTSBET_scraper_union()
    
    time.sleep(5)
    
    logger.info(f"Scraping Unibet union Data")
    ub_scraper = ub.UBSportsScraper(f.get_ub_url('rugby_union'),  chosen_date=chosen_date)
    ub_union_markets = await ub_scraper.UNIBET_scrape_union()
    
    time.sleep(5)
    
    pb_union_compids = f.get_pb_comps('rugby-union')  
    pb_union_markets = {}   
    logger.info(f"Scraping Pointsbet union Data")
    for comp_id in pb_union_compids:      
        pb_scraper = pb.PBSportsScraper(f.get_pb_url(comp_id),  chosen_date=chosen_date)
        comp_markets = await pb_scraper.POINTSBET_scrape_union(market_type='Head to Head')
        pb_union_markets.update(comp_markets)
        
    time.sleep(5)
    
    logger.info(f"Scraping Palmersbet union Data")
    palm_scraper = palm.PalmerBetSportsScraper(palm_union_url,  chosen_date=chosen_date)
    palm_union_markets = await palm_scraper.PalmerBet_scrape()
    
    time.sleep(5)
    
    logger.info(f"Scraping Betr union Data")
    betr_scraper = betr.BetrSportsScraper(betr_union_url,  chosen_date=chosen_date)
    betr_union_markets = await betr_scraper.Betr_scrape_union()
    
    time.sleep(5)
    
    logger.info("Scraping betright union data")
    br_scraper = br.BRSportsScraper(f.get_betright_url(105),  chosen_date=chosen_date)
    br_union_markets = await br_scraper.BETRIGHT_scraper()
    
    time.sleep(5)
        
    logger.info("Scraping betdeluxe union data")
    bd_scraper = bd.BDSportsScraper(f.get_betdeluxe_url('rugby-union'), chosen_date=chosen_date)
    bd_union_markets = await bd_scraper.BETDELUXE_scraper(sport='rugby-union')
    
    
    logger.info("Fetching union model odds")    

    
    bookmakers = {
        "Sportsbet": sb_union_markets,
        "Pointsbet": pb_union_markets,
        "Unibet": ub_union_markets,
        "Palmerbet": palm_union_markets,
        #"Betr": betr_union_markets,  # Added Betr
        "Betright": br_union_markets,
        "Betdeluxe": bd_union_markets
    }
    
    # Updated list of price columns for fuzzy_merge_prices
    price_cols = ['Sportsbet', 'Pointsbet', 'Unibet', 'Palmerbet', 'Betright', 'Betdeluxe']
    
    if model_union_markets:
        bookmakers['Model'] = model_union_markets
        price_cols.append('Model')
    
    f.process_odds(bookmakers, price_cols, table_name="Rugby Union Odds")
    
    
    # %% #---------NRL--------#
    #get_sportsbet_compids(23)
    logger.info(f"Scraping Sportsbet NRL Data")
    sb_scraper = sb.SBSportsScraper(f.get_sportsbet_url(sportId=23),  chosen_date=chosen_date)
    sb_nrl_markets = await sb_scraper.SPORTSBET_scraper(competition_id=3436)
    
    time.sleep(5)
    
    logger.info(f"Scraping Pointsbet NRL Data")
    pb_scraper = pb.PBSportsScraper(pb_nrl_url,  chosen_date=chosen_date)
    pb_nrl_markets = await pb_scraper.POINTSBET_scrape_nrl(market_type='Match Result')
    
    time.sleep(5)
    
    logger.info(f"Scraping Unibet NRL Data")
    ub_scraper = ub.UBSportsScraper(f.get_ub_url('rugby_league'),  chosen_date=chosen_date)
    ub_nrl_markets = await ub_scraper.UNIBET_scrape_sport(comp='NRL')
    
    time.sleep(5)
    
    logger.info(f"Scraping Palmerbet NRL Data")
    palm_scraper = palm.PalmerBetSportsScraper(palm_nrl_url,  chosen_date=chosen_date)
    palm_nrl_markets = await palm_scraper.PalmerBet_scrape(comp='Australia National Rugby League')
    
    time.sleep(5)
    
    logger.info(f"Scraping Betr NRL Data")
    betr_scraper = betr.BetrSportsScraper(betr_nrl_url,  chosen_date=chosen_date)
    betr_nrl_markets = await betr_scraper.Betr_scrape_union(comp='NRL Telstra Premiership')
    
    time.sleep(5)
    
    logger.info("Scraping betright NRL data")
    br_scraper = br.BRSportsScraper(f.get_betright_url(102),  chosen_date=chosen_date)
    br_nrl_markets = await br_scraper.BETRIGHT_scraper()
    
    time.sleep(5)
    
    # --- Combine bookmaker markets ---
    bookmakers = {
        "Sportsbet": sb_nrl_markets,
        "Pointsbet": pb_nrl_markets,
        "Unibet": ub_nrl_markets,
        "Palmerbet": palm_nrl_markets,
        #"Betr": betr_nrl_markets,  # Added Betr
        "Betright": br_nrl_markets
    }
    
    price_cols = ['Sportsbet', 'Pointsbet', 'Unibet', 'Palmerbet', 'Betright']

    f.process_odds(bookmakers, price_cols, table_name="NRL Odds")
    
    
    #%% E-sports
    
    logger.info(f"Scraping Sportsbet E Sports Data")
    sb_scraper = sb.SBSportsScraper(f.get_sportsbet_url(sportId=206),  chosen_date=chosen_date)
    sb_esports_markets = await sb_scraper.SPORTSBET_scraper()
    
    time.sleep(5)
    
    pb_esports_compids = f.get_pb_comps('e-sports')  
    pb_esports_markets = {}   
    logger.info(f"Scraping Pointsbet E sports Data")
    for comp_id in pb_esports_compids:      
        pb_scraper = pb.PBSportsScraper(f.get_pb_url(comp_id),  chosen_date=chosen_date)
        comp_markets = await pb_scraper.POINTSBET_scrape_sport(market_type='Match Result')
        pb_esports_markets.update(comp_markets)
        
    time.sleep(5)
        
    logger.info(f"Scraping Unibet E sports Data")
    ub_scraper = ub.UBSportsScraper(f.get_ub_url('esports'),  chosen_date=chosen_date)
    ub_esports_markets = await ub_scraper.UNIBET_scrape_sport()
    
    time.sleep(5)
    
    logger.info("Scraping betright E Sports data")
    br_scraper = br.BRSportsScraper(f.get_betright_url(124),  chosen_date=chosen_date)
    br_esports_markets = await br_scraper.BETRIGHT_scraper()
    
    
    # --- Combine bookmaker markets ---
    bookmakers = {
        "Sportsbet": sb_esports_markets,
        "Pointsbet": pb_esports_markets,
        "Unibet": ub_esports_markets,
        "Betright": br_esports_markets
    }
    
    price_cols = ["Sportsbet", "Pointsbet", "Unibet", "Betright"]
    
    f.process_odds(bookmakers, price_cols, table_name="E-Sports Odds")
    
    
    #%% mma
    
    logger.info(f"Scraping Sportsbet UFC Data")
    sb_scraper = sb.SBSportsScraper(sb_ufc_url,  chosen_date=chosen_date)
    sb_mma_markets = await sb_scraper.SPORTSBET_scrape_mma()
    
    time.sleep(5)
    
    pb_mma_compids = f.get_pb_comps('mma')  
    pb_mma_markets = {}   
    logger.info(f"Scraping Pointsbet MMA Data")
    for comp_id in pb_mma_compids:      
        pb_scraper = pb.PBSportsScraper(f.get_pb_url(comp_id),  chosen_date=chosen_date)
        comp_markets = await pb_scraper.POINTSBET_scrape_sport(market_type='Fight Result')
        pb_mma_markets.update(comp_markets)
        
    time.sleep(5)
        
    logger.info(f"Scraping Unibet mma Data")
    ub_scraper = ub.UBSportsScraper(f.get_ub_url('ufc_mma'),  chosen_date=chosen_date)
    ub_mma_markets = await ub_scraper.UNIBET_scrape_sport()
    
    time.sleep(5)
                
    logger.info("Scraping betdeluxe mma data")
    bd_scraper = bd.BDSportsScraper(f.get_betdeluxe_url('martial-arts'), chosen_date=chosen_date)
    bd_mma_markets = await bd_scraper.BETDELUXE_scraper(sport='martial-arts', market_name='Fight Result')
    
    time.sleep(5)
    
    surge_mma_compids = f.get_surge_comps('martial-arts')           
        
    logger.info(f"Scraping Surgebet MMA Data")
    surge_mma_markets = {}
    for comp_id in surge_mma_compids:
        surge_scraper = ss.SurgeSportsScraper(f.get_surge_url('martial-arts', comp_id),  chosen_date=chosen_date)
        comp_markets = await surge_scraper.Surge_scrape(market ='Fight Result')
        surge_mma_markets.update(comp_markets)
        
    time.sleep(5)
    
    # --- Combine bookmaker markets ---
    bookmakers = {
        "Sportsbet": sb_mma_markets,
        "Pointsbet": pb_mma_markets,
        "Unibet": ub_mma_markets,
        "Betdeluxe": bd_mma_markets,
        "SurgeBet": surge_mma_markets
    }
    
    price_cols = ["Sportsbet", "Pointsbet", "Unibet", "Betdeluxe", "SurgeBet"]
    
    f.process_odds(bookmakers, price_cols, table_name="MMA Odds")
    
    
    
    #%% Basketball
    
    logger.info(f"Scraping Sportsbet Basketball Data")
    sb_scraper = sb.SBSportsScraper(f.get_sportsbet_url(sportId=16),  chosen_date=chosen_date)
    sb_basketball_us_markets = await sb_scraper.SPORTSBET_scraper()
    sb_scraper = sb.SBSportsScraper(f.get_sportsbet_url(sportId=63),  chosen_date=chosen_date)
    sb_basketball_other_markets = await sb_scraper.SPORTSBET_scraper()
    
    sb_basketball_markets = {**sb_basketball_us_markets, **sb_basketball_other_markets}
    
    time.sleep(5)

    pb_basketball_compids = f.get_pb_comps('basketball')  
    pb_basketball_markets = {}   
    logger.info(f"Scraping Pointsbet basketball Data")
    for comp_id in pb_basketball_compids:      
        pb_scraper = pb.PBSportsScraper(f.get_pb_url(comp_id),  chosen_date=chosen_date)
        comp_markets = await pb_scraper.POINTSBET_scrape_nrl(market_type='Head to Head')
        pb_basketball_markets.update(comp_markets)
        
    time.sleep(5)
    
    logger.info(f"Scraping Unibet Basketball Data")
    ub_scraper = ub.UBSportsScraper(f.get_ub_url('basketball'), chosen_date=chosen_date)
    ub_basketball_markets = await ub_scraper.UNIBET_scrape_sport()
    
    time.sleep(5)
    
    logger.info(f"Scraping Palmerbet Basketball Data")
    palm_scraper = palm.PalmerBetSportsScraper(palm_basketball_url, chosen_date=chosen_date)
    palm_basketball_markets = await palm_scraper.PalmerBet_scrape()   
    
    time.sleep(5)
    
    logger.info("Scraping betright Basketball data")
    br_scraper = br.BRSportsScraper(f.get_betright_url(107),  chosen_date=chosen_date)
    br_basketball_markets = await br_scraper.BETRIGHT_scraper()
    
    time.sleep(5)
    
    surge_basketball_compids = f.get_surge_comps('basketball')           
        
    logger.info(f"Scraping Surgebet basketball Data")
    surge_basketball_markets = {}
    for comp_id in surge_basketball_compids:
        surge_scraper = ss.SurgeSportsScraper(f.get_surge_url('basketball', comp_id),  chosen_date=chosen_date)
        comp_markets = await surge_scraper.Surge_scrape(market ='Money Line')
        surge_basketball_markets.update(comp_markets)
        
    time.sleep(5)

    # --- Combine bookmaker markets ---
    bookmakers = {
        "Sportsbet": sb_basketball_markets,
        "Pointsbet": pb_basketball_markets,
        "Unibet": ub_basketball_markets,
        "Palmerbet": palm_basketball_markets,
        "Betright": br_basketball_markets,
        "SurgeBet": surge_basketball_markets
    }
    
    price_cols = ["Sportsbet", "Pointsbet", "Unibet", "Palmerbet", "Betright", "SurgeBet"]
    
    f.process_odds(bookmakers, price_cols, table_name="Basketball Odds")
    
if __name__ == "__main__":
    asyncio.run(main())

