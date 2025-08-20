import pytz
from datetime import datetime, timedelta
from loguru import logger
import sportsbet_scrapers as sb
import pointsbet_scrapers as pb
import unibet_scrapers as ub
import PalmerBet_scrapers as palm
import betr_scrapers as betr
import asyncio
import nest_asyncio
import pandas as pd
import numpy as np
import requests
from rapidfuzz import process, fuzz
from thefuzz import fuzz, process
from fuzzywuzzy import fuzz, process
from functools import reduce
import re
import unidecode



nest_asyncio.apply()

async def main():
    
    # %%
    chosen_date = datetime.now(pytz.timezone("Australia/Brisbane")).date().strftime("%Y-%m-%d")
    one_week = (datetime.now(pytz.timezone("Australia/Brisbane")) + timedelta(7)).date().strftime("%Y-%m-%d")
    print(chosen_date)
    print(one_week)
    
    
    def normalize_match(s):
        if pd.isna(s):
            return ""
        s = unidecode.unidecode(str(s))  # ensure string, remove accents
        s = re.sub(r'[^a-zA-Z0-9 ]', ' ', s)  # remove punctuation
        s = re.sub(r'\s+', ' ', s).strip()    # collapse spaces
        return s.lower()
    
    def normalize_result(s):
        if pd.isna(s):
            return ""
        s = unidecode.unidecode(str(s))
        s = re.sub(r'[^a-zA-Z0-9 ]', ' ', s)
        s = re.sub(r'\s+', ' ', s).strip()
        return s.lower()
    
    def fuzzy_merge_prices(dfs, bookie_names, outcomes=3, match_threshold=70, result_threshold=70):
        dfs = [df.copy() for df in dfs]
        
        # Normalize matches/results before fuzzy logic
        for df in dfs:
            df['match_norm'] = df['match'].apply(normalize_match)
            df['result_norm'] = df['result'].apply(normalize_result)
        
        base_df = dfs[0].copy()
        base_df['match_fuzzy'] = base_df['match_norm']
        base_df['result_fuzzy'] = base_df['result_norm']
        
        for i, other_df in enumerate(dfs[1:], 1):
            other_df = other_df.copy()
            other_df['match_fuzzy'] = other_df['match_norm']
            other_df['result_fuzzy'] = other_df['result_norm']
            
            # Fuzzy match matches
            match_map = {}
            for val in base_df['match_fuzzy'].dropna().unique():
                result = process.extractOne(
                    val, other_df['match_norm'].dropna().unique(), scorer=fuzz.token_sort_ratio
                )
                if result:
                    match_name, score = result[0], result[1]
                    if score >= match_threshold:
                        match_map[val] = match_name
            other_df['match_fuzzy'] = other_df['match_norm'].map({v: k for k, v in match_map.items()})
            
            # Fuzzy match results
            result_map = {}
            for val in base_df['result_fuzzy'].dropna().unique():
                result = process.extractOne(
                    val, other_df['result_norm'].dropna().unique(), scorer=fuzz.token_sort_ratio
                )
                if result:
                    result_name, score = result[0], result[1]
                    if score >= result_threshold:
                        result_map[val] = result_name
            other_df['result_fuzzy'] = other_df['result_norm'].map({v: k for k, v in result_map.items()})
            
            # Force keys to string type to avoid dtype mismatch
            for col in ['match_fuzzy', 'result_fuzzy']:
                base_df[col] = base_df[col].astype(str)
                other_df[col] = other_df[col].astype(str)
            
            # Merge
            base_df = base_df.merge(
                other_df[['match_fuzzy', 'result_fuzzy', bookie_names[i]]],
                on=['match_fuzzy', 'result_fuzzy'],
                how='outer'
            )
        
        # Calc best prices + best bookies
        price_cols = bookie_names
        base_df['best_price'] = base_df[price_cols].max(axis=1)
        base_df['best_bookie'] = base_df.apply(lambda row: [col for col in price_cols if row[col] == row['best_price']], axis=1)
        base_df['best_prob'] = 1 / base_df['best_price']
        
        # --- compute match-level market percent ---
        match_mkt = base_df.groupby('match_fuzzy', as_index=False)['best_prob'].sum().rename(columns={'best_prob': 'mkt_percent'})
        
        # Merge mkt_percent back to row-level data
        npc_mkt_percents = base_df.merge(match_mkt, on='match_fuzzy', how='left')
        
        
        # Keep only relevant columns
        npc_mkt_percents = npc_mkt_percents[['match', 'result', 'mkt_percent', 'best_price', 'best_bookie']]
        
        return base_df, npc_mkt_percents
    
    def get_pb_comps(sport):
        pb_comps_url = f'https://api.au.pointsbet.com/api/v2/sports/{sport}/competitions'
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/115.0.0.0 Safari/537.36",
            "Accept": "application/json",
        }
        
        response = requests.get(pb_comps_url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            pb_compids = [
                comp["key"]
                for locale in data.get("locales", [])
                for comp in locale.get("competitions", [])
            ]
        else:
            print(f"Failed to retrieve data: {response.status_code} {response.text}")
            
        return pb_compids

    
    def get_sportsbet_url(sportId: int):
        return f'https://www.sportsbet.com.au/apigw/sportsbook-sports/Sportsbook/Sports/Events?primaryMarketOnly=true&fromDate={chosen_date}T00:00:00&toDate={one_week}T23:59:59&sportsId={sportId}&numEventsPerClass=2000&detailsLevel=O'
    
    def get_sportsbet_compids(sportId: int):
        url = get_sportsbet_url(sportId)
        
        comp_dict = {}
        
        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()

            for event in data:
                comp_id = event.get('competitionId')
                comp_name = event.get('competitionName')
    
                if comp_id and comp_name:
                    # Only add if not already in dict or matches existing name
                    if comp_id not in comp_dict:
                        comp_dict[comp_id] = comp_name
                    elif comp_dict[comp_id] != comp_name:
                        print(f"⚠ Duplicate comp_id {comp_id} with different name: "
                              f"{comp_dict[comp_id]} vs {comp_name}")
        
        return comp_dict
    
    def get_pb_url(competition_id: int):
        return f'https://api.au.pointsbet.com/api/mes/v3/events/featured/competition/{competition_id}?page=1'
    
    def get_ub_url(sport):
        return f'https://www.unibet.com.au/sportsbook-feeds/views/filter/{sport}/all/matches?includeParticipants=true&useCombined=true&ncid=1755083716'
    
    pb_union_url = f'https://api.au.pointsbet.com/api/mes/v3/events/featured/competition/15797?page=1'
    pb_nrl_url = f'https://api.au.pointsbet.com/api/mes/v3/events/featured/competition/7593?page=1'
    
    palm_union_url = f'https://fixture.palmerbet.online/fixtures/sports/5bfcf787-edfc-48f4-b328-1d221aa07ae0/matches?sportType=rugbyunion&pageSize=24&channel=website'
    palm_tennis_url = f'https://fixture.palmerbet.online/fixtures/sports/9d6bbedd-0b09-4031-9884-e264499a2aa5/matches?sportType=tennis&pageSize=24&channel=website'
    palm_nrl_url = f'https://fixture.palmerbet.online/fixtures/sports/cf404de1-1953-4d55-b92e-4e022f186b22/matches?sportType=rugbyleague&pageSize=24&channel=website'
    
    betr_union_url = 'https://web20-api.bluebet.com.au/MasterCategory?EventTypeId=105&WithLevelledMarkets=true'
   
    
    
    # %% #---------Football--------#
    pb_football_compids = get_pb_comps('soccer')

    logger.info(f"Scraping Pointsbet EPL Data")
    pb_scraper = pb.PBSportsScraper(get_pb_url(136535),  chosen_date=chosen_date)
    pb_epl_markets = await pb_scraper.POINTSBET_scrape_sport(market_type='Match Result')
    
    logger.info(f"Scraping Sportsbet EPL Data")
    sb_scraper = sb.SBSportsScraper(get_sportsbet_url(29),  chosen_date=chosen_date)
    sb_epl_markets = await sb_scraper.SPORTSBET_scraper(competition_id=718)
    
    #EPL df
    
    bookmakers = {
        "sb": sb_epl_markets,
        "pb": pb_epl_markets,
    }
    
    dfs = {}
    
    for name, markets in bookmakers.items():
        rows = []
        for match, odds in markets.items():
            for result, price in odds.items():
                rows.append({"match": match, "result": result, f"{name}_price": price})
        dfs[name] = pd.DataFrame(rows)
    
    # Access them like:
    sb_epl_df = dfs["sb"]
    pb_epl_df = dfs["pb"]
        
    dfs = [sb_epl_df, pb_epl_df]
    price_cols = ['sb_price', 'pb_price']
    
    epl_df, epl_mkt_percents = fuzzy_merge_prices(dfs, price_cols, outcomes=3)

    print(epl_mkt_percents.head(60))
    
    
    logger.info(f"Scraping Pointsbet Football Data")
    pb_football_markets = {}
    for comp_id in pb_football_compids:
        pb_scraper = pb.PBSportsScraper(get_pb_url(comp_id),  chosen_date=chosen_date)
        comp_markets = await pb_scraper.POINTSBET_scrape_sport(market_type='Match Result')
        pb_football_markets.update(comp_markets)
    
    logger.info(f"Scraping Sportsbet Football Data")
    sb_scraper = sb.SBSportsScraper(get_sportsbet_url(29),  chosen_date=chosen_date)
    sb_football_markets = await sb_scraper.SPORTSBET_scraper()
    
    logger.info(f"Scraping Unibet football Data")
    ub_scraper = ub.UBSportsScraper(get_ub_url('football'),  chosen_date=chosen_date)
    ub_football_markets = await ub_scraper.UNIBET_scrape_tennis()
    
    
    #Football df
    bookmakers = {
        "sb": sb_football_markets,
        "pb": pb_football_markets,
        "ub": ub_football_markets
    }
    
    dfs = {}
    
    for name, markets in bookmakers.items():
        rows = []
        for match, odds in markets.items():
            for result, price in odds.items():
                rows.append({"match": match, "result": result, f"{name}_price": price})
        dfs[name] = pd.DataFrame(rows)
    
    # Access them like:
    sb_football_df = dfs["sb"]
    pb_football_df = dfs["pb"]
    ub_football_df = dfs["ub"]
    
    dfs = [sb_football_df, pb_football_df, ub_football_df]
    price_cols = ['sb_price', 'pb_price', 'ub_price']
                
    football_df, football_mkt_percents = fuzzy_merge_prices(dfs, price_cols, outcomes=3)
    
    print(football_df.head(10)) 
    print(football_mkt_percents.sort_values(by='mkt_percent', ascending=True).head(60))
            
    
    
    
    # %% #---------Tennis--------#
    pb_tennis_compids = get_pb_comps('tennis')

    logger.info(f"Scraping Pointsbet tennis Data")
    pb_tennis_markets = {}
    for comp_id in pb_tennis_compids:
        pb_scraper = pb.PBSportsScraper(get_pb_url(comp_id),  chosen_date=chosen_date)
        comp_markets = await pb_scraper.POINTSBET_scrape_sport(market_type='Match Result')
        pb_tennis_markets.update(comp_markets)
    
    logger.info(f"Scraping Sportsbet tennis Data")
    sb_scraper = sb.SBSportsScraper(get_sportsbet_url(13),  chosen_date=chosen_date)
    sb_tennis_markets = await sb_scraper.SPORTSBET_scraper()
    
    logger.info(f"Scraping Unibet tennis Data")
    ub_scraper = ub.UBSportsScraper(get_ub_url('tennis'),  chosen_date=chosen_date)
    ub_tennis_markets = await ub_scraper.UNIBET_scrape_tennis()
    
    #tennis df
    bookmakers = {
        "sb": sb_tennis_markets,
        "pb": pb_tennis_markets,
        "ub": ub_tennis_markets
    }
    
    dfs = {}
    
    for name, markets in bookmakers.items():
        rows = []
        for match, odds in markets.items():
            for result, price in odds.items():
                rows.append({"match": match, "result": result, f"{name}_price": price})
        dfs[name] = pd.DataFrame(rows)
    
    # Access them like:
    sb_tennis_df = dfs["sb"]
    pb_tennis_df = dfs["pb"]
    ub_tennis_df = dfs["ub"]
    
    dfs = [sb_tennis_df, pb_tennis_df, ub_tennis_df]
    price_cols = ['sb', 'pb', 'ub']
    
    print(sb_tennis_df.head(5))
            
    tennis_df, tennis_mkt_percents = fuzzy_merge_prices(dfs, price_cols, outcomes=2)
    
    print(tennis_df.head(60)) 
    print(tennis_mkt_percents.sort_values(by='market_percent', ascending=True).head(60))
    
    
    
    
    
   
    
    # %% #---------NPC--------#
    logger.info(f"Scraping Sportsbet Union Data")
    sb_scraper = sb.SBSportsScraper(get_sportsbet_url(sportId=12),  chosen_date=chosen_date)
    sb_npc_markets = await sb_scraper.SPORTSBET_scraper(competition_id=27313)
    
    logger.info(f"Scraping Unibet NPC Data")
    ub_scraper = ub.UBSportsScraper(get_ub_url('rugby_union'),  chosen_date=chosen_date)
    ub_npc_markets = await ub_scraper.UNIBET_scrape_union(comp='NZ NPC')
    
    logger.info(f"Scraping Pointsbet NPC Data")
    pb_scraper = pb.PBSportsScraper(pb_union_url,  chosen_date=chosen_date)
    pb_npc_markets = await pb_scraper.POINTSBET_scrape_union(market_type='Head to Head')
    
    logger.info(f"Scraping Palmersbet NPC Data")
    palm_scraper = palm.PalmerBetSportsScraper(palm_union_url,  chosen_date=chosen_date)
    palm_npc_markets = await palm_scraper.PalmerBet_scrape_npc(comp='New Zealand Mitre 10 Cup')
    
    logger.info(f"Scraping Betr NPC Data")
    betr_scraper = betr.BetrSportsScraper(betr_union_url,  chosen_date=chosen_date)
    betr_npc_markets = await betr_scraper.Betr_scrape_union(comp='Bunnings NPC')
    
        
    bookmakers = {
        "sb": sb_npc_markets,
        "pb": pb_npc_markets,
        "ub": ub_npc_markets,
        "palm": palm_npc_markets,
        "betr": betr_npc_markets  # Added Betr
    }
    
    dfs = {}
    
    # Convert each bookmaker's markets into a DataFrame
    for name, markets in bookmakers.items():
        rows = []
        for match, odds in markets.items():
            for result, price in odds.items():
                rows.append({"match": match, "result": result, f"{name}_price": price})
        dfs[name] = pd.DataFrame(rows)
    
    # Access individual DataFrames
    sb_npc_df = dfs["sb"]
    pb_npc_df = dfs["pb"]
    ub_npc_df = dfs["ub"]
    palm_npc_df = dfs["palm"]
    betr_npc_df = dfs["betr"]  # Added Betr
    
    # List of DataFrames for merging
    dfs_list = [sb_npc_df, pb_npc_df, ub_npc_df, palm_npc_df, betr_npc_df]
    
    # Updated list of price columns for fuzzy_merge_prices
    price_cols = ['sb_price', 'pb_price', 'ub_price', 'palm_price', 'betr_price']
    
    # Run fuzzy merge
    npc_df, npc_mkt_percents = fuzzy_merge_prices(dfs_list, price_cols, outcomes=3)
    
    #print(npc_df)
    print(npc_mkt_percents)
        
    
    # %% #---------NRL--------#
    get_sportsbet_compids(23)
    logger.info(f"Scraping Sportsbet NRL Data")
    sb_scraper = sb.SBSportsScraper(get_sportsbet_url(sportId=23),  chosen_date=chosen_date)
    sb_nrl_markets = await sb_scraper.SPORTSBET_scraper(competition_id=3436)
    
    logger.info(f"Scraping Pointsbet NRL Data")
    pb_scraper = pb.PBSportsScraper(pb_nrl_url,  chosen_date=chosen_date)
    pb_nrl_markets = await pb_scraper.POINTSBET_scrape_nrl(market_type='Match Result')
    
    logger.info(f"Scraping Unibet NRL Data")
    ub_scraper = ub.UBSportsScraper(get_ub_url('rugby_league'),  chosen_date=chosen_date)
    ub_nrl_markets = await ub_scraper.UNIBET_scrape_union(comp='NRL')
    
    logger.info(f"Scraping Palmersbet NRL Data")
    palm_scraper = palm.PalmerBetSportsScraper(palm_nrl_url,  chosen_date=chosen_date)
    palm_nrl_markets = await palm_scraper.PalmerBet_scrape_npc(comp='Australia National Rugby League')
    
    logger.info(f"Scraping Betr NPC Data")
    betr_scraper = betr.BetrSportsScraper(betr_union_url,  chosen_date=chosen_date)
    betr_npc_markets = await betr_scraper.Betr_scrape_union(comp='Bunnings NPC')
    
    bookmakers = {
        "sb": sb_nrl_markets,
        "pb": pb_nrl_markets,
    }
    
    dfs = {}
    
    for name, markets in bookmakers.items():
        rows = []
        for match, odds in markets.items():
            for result, price in odds.items():
                rows.append({"match": match, "result": result, f"{name}_price": price})
        dfs[name] = pd.DataFrame(rows)
    
    # Access them like:
    sb_nrl_df = dfs["sb"]
    pb_nrl_df = dfs["pb"]
    
    print(sb_nrl_df)
    print(pb_nrl_df)

    
    nrl_df, nrl_mkt_percents = fuzzy_merge_prices(sb_nrl_df, pb_nrl_df, outcomes=2)
    
    print(nrl_df)
    print(nrl_mkt_percents)

if __name__ == "__main__":
    asyncio.run(main())

