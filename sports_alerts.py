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
from functools import reduce
import re
import unidecode



nest_asyncio.apply()

async def main():
    
    # %%
    chosen_date = datetime.now(pytz.timezone("Australia/Brisbane")).date().strftime("%Y-%m-%d")
    one_week = (datetime.now(pytz.timezone("Australia/Brisbane")) + timedelta(14)).date().strftime("%Y-%m-%d")
    two_week = (datetime.now(pytz.timezone("Australia/Brisbane")) + timedelta(14)).date().strftime("%Y-%m-%d")
    print(chosen_date)
    print(one_week)
    print(two_week)
    
    
    def normalize_match(match):
        if pd.isna(match):
            return ""
        match = match.lower()
        match = re.sub(r'[-]', 'v', match)  # replace - or / with v
        match = re.sub(r' vs | v ', ' v ', match)  # unify separators
        match = re.sub(r'\s+', ' ', match).strip()
        return match
    
    def normalize_result(s):
        if pd.isna(s):
            return ""
        s = unidecode.unidecode(str(s))
        s = re.sub(r'[^a-zA-Z0-9 ]', ' ', s)
        s = re.sub(r'\s+', ' ', s).strip()
        return s.lower()
    
    def normalize_name(name: str) -> str:
        """
        Normalize a player's name:
        - Remove accents
        - Handle "Last, First" -> "First Last"
        - Remove extra spaces and punctuation
        - Lowercase everything
        """
        if not name:
            return ""
        
        name = unidecode.unidecode(name)  # remove accents
        name = name.strip()
        
        # Handle "Last, First" -> "First Last"
        if "," in name:
            parts = [p.strip() for p in name.split(",")]
            if len(parts) == 2:
                name = f"{parts[1]} {parts[0]}"
        
        # Remove any remaining non-alphanumeric characters except spaces
        name = re.sub(r'[^a-zA-Z0-9 ]', ' ', name)
        name = re.sub(r'\s+', ' ', name).strip()
        return name.lower()
    
        
    def normalize_players_result(result: str) -> str:
        if pd.isna(result):
            return ""
        
        # Remove accents
        result = unidecode.unidecode(result)
        
        # If there's a comma, switch Last, First → First Last
        if ',' in result:
            parts = [p.strip() for p in result.split(',')]
            result = ' '.join(parts[::-1])
        
        # Remove extra characters and lowercase
        result = re.sub(r'[^a-zA-Z0-9 ]', ' ', result)
        result = re.sub(r'\s+', ' ', result).strip().lower()
        
        return result
    
        
    def normalize_players_match(match: str) -> str:
        if pd.isna(match):
            return ""
        
        match = unidecode.unidecode(match)
        
        # Split into sides
        sides = re.split(r'\s+v\s+', match, flags=re.IGNORECASE)
        normalized_sides = []
        
        for side in sides:
            side = side.strip()
            # If comma, flip
            if ',' in side:
                parts = [p.strip() for p in side.split(',')]
                side = ' '.join(parts[::-1])
            # Remove extra chars and lowercase
            side = re.sub(r'[^a-zA-Z0-9 ]', ' ', side)
            side = re.sub(r'\s+', ' ', side).strip().lower()
            normalized_sides.append(side)
        
        # Always sort alphabetically
        normalized_sides.sort()
        
        return ' v '.join(normalized_sides)
            
    def fuzzy_merge_prices(dfs, bookie_names, outcomes=3, match_threshold=80, result_threshold=50, names=False):
    
        # Copy dfs to avoid modifying originals
        dfs = [df.copy() for df in dfs]
        
        if names:
            for df in dfs:
                df['match_norm'] = df['match'].apply(normalize_players_match)
                df['result_norm'] = df['result'].apply(normalize_players_result)
        
        else:
            # Normalize match and result names
            for df in dfs:
                df['match_norm'] = df['match'].apply(normalize_match)
                df['result_norm'] = df['result'].apply(normalize_result)
    
        # Use first dataframe as base
        base_df = dfs[0].copy()
        base_df['match_fuzzy'] = base_df['match_norm']
        base_df['result_fuzzy'] = base_df['result_norm']
    
        # Merge each other dataframe
        for i, other_df in enumerate(dfs[1:], 1):
            other_df = other_df.copy()
    
            # Only consider matches that actually exist in this bookie
            other_matches = other_df['match_norm'].dropna().unique()
            match_map = {}

            for base_mtch in base_df['match_fuzzy'].dropna().unique():
                best_mtch = process.extractOne(
                    base_mtch,
                    other_matches,
                    scorer=fuzz.token_sort_ratio
                )
                if best_mtch and best_mtch[1] >= match_threshold:
                    match_map[base_mtch] = best_mtch[0]
    
            # Map matches
            other_df['match_fuzzy'] = other_df['match_norm'].map({v: k for k, v in match_map.items()})
    
            # Fuzzy match results only for matched matches
            for base_mtch, other_mtch in match_map.items():
                base_rows = base_df[base_df['match_fuzzy'] == base_mtch]
                other_rows = other_df[other_df['match_norm'] == other_mtch]
    
                result_map = {}
                for res in base_rows['result_fuzzy'].dropna().unique():
                    best_res = process.extractOne(
                        res,
                        other_rows['result_norm'].dropna().unique(),
                        scorer=fuzz.token_sort_ratio
                    )
                    if best_res and best_res[1] >= result_threshold:
                        result_map[res] = best_res[0]
    
                other_df.loc[other_rows.index, 'result_fuzzy'] = other_rows['result_norm'].map(
                    {v: k for k, v in result_map.items()}
                )
                
            if 'result_fuzzy' not in other_df.columns:
                other_df['result_fuzzy'] = np.nan
                
            for col in ['match_fuzzy', 'result_fuzzy']:
                if col not in other_df.columns:
                    other_df[col] = None
                other_df[col] = other_df[col].astype(str).replace('nan', '')
            
            base_df['match_fuzzy'] = base_df['match_fuzzy'].astype(str).replace('nan', '')
            base_df['result_fuzzy'] = base_df['result_fuzzy'].astype(str).replace('nan', '')
    
            # Merge bookie column into base_df
            merge_cols = ['match_fuzzy', 'result_fuzzy', bookie_names[i]]
            base_df = base_df.merge(
                other_df[merge_cols],
                on=['match_fuzzy', 'result_fuzzy'],
                how='left'
            )
    
        # Calculate best prices and bookies
        base_df['best_price'] = base_df[bookie_names].max(axis=1, skipna=True)
        base_df['best_bookie'] = base_df.apply(
            lambda row: [col for col in bookie_names if row[col] == row['best_price']], axis=1
        )
        base_df['best_prob'] = 1 / base_df['best_price']
    
        # Compute match-level market percent
        match_mkt = base_df.groupby('match_fuzzy', as_index=False)['best_prob'].sum().rename(
            columns={'best_prob': 'mkt_percent'}
        )
    
        # Merge back
        mkt_percents = base_df.merge(match_mkt, on='match_fuzzy', how='left')
    
        # Keep only relevant columns for alerting
        mkt_percents = mkt_percents[['match', 'result', 'mkt_percent', 'best_price', 'best_bookie']]
    
        return base_df, mkt_percents
    
    def arb_alert(arbs, test=False):
        if not test:
            try:
                with open("alerts_sent.txt", "r") as f:
                    sent_alerts = set(f.read().splitlines())
            except FileNotFoundError:
                sent_alerts = set()
                
            webhook = WEBHOOK_ARBS
        
        else:
            webhook = WEBHOOK_TEST
        
        for match_name, group in arbs.groupby('match'):
            
            if not test:
                if f'{match_name} {chosen_date}' in sent_alerts:
                    continue  # Skip if alert already sent
                
            mkt_percent = group['mkt_percent'].iloc[0] * 100  
            
            outcomes = []
            for _, row in group.iterrows():
                bookies = ", ".join(row['best_bookie'])
                outcomes.append(f"{row['result']} {row['best_price']}$ on {bookies}")
            
            # Join outcomes with commas and 'and' for the last item
            if len(outcomes) > 1:
                outcomes_text = "; ".join(outcomes[:-1]) + "; " + outcomes[-1]
            else:
                outcomes_text = outcomes[0]
            
            message = f"{match_name}: {mkt_percent:.2f}% market; {outcomes_text}"
        
            try:
                response = requests.post(webhook, json={"content": message})
                response.raise_for_status()
                
                if not test:
                    # Append the match to the file after successful send
                    with open("alerts_sent.txt", "a") as f:
                        f.write(f"{match_name} {chosen_date}\n")
        
            except Exception as e:
                logger.error(f"Webhook failed: {e}")
                
    def prob_alert(df, diff_lim, test=False):
        if not test:
            try:
                with open("prob_alerts_sent.txt", "r") as f:
                    sent_alerts = set(f.read().splitlines())
            except FileNotFoundError:
                sent_alerts = set()
                
            webhook = WEBHOOK_PROBS
        
        else:
            webhook = WEBHOOK_TEST
    
        for _, row in df.iterrows():
            result_name = row["result"]
            match = row['match']
    
            # Convert odds to implied probabilities (ignoring NaNs)
            probs = {}
            prices = {}
            for bookie in df.columns[2:]:  # skip "result" column
                if pd.notna(row[bookie]):
                    probs[bookie] = 1 / row[bookie]
    
            if not probs:
                continue
            
            if pd.isna(result_name):
                continue
    
            # Find max diff in probabilities
            max_prob = max(probs.values())
            min_prob = min(probs.values())
            diff = max_prob - min_prob
    
            if diff >= diff_lim:
                alert_key = f"{result_name} {chosen_date}"
                
                if not test:
                    if alert_key in sent_alerts:
                        continue  # already sent
    
                # Format details
                details = ", ".join([f"{b}: {(1/p):.2f}" for b, p in probs.items()])
                message = (f"{match}: {result_name} has a probability difference of {diff:.3f}\n"
                           f"{details}")
    
                try:
                    response = requests.post(webhook, json={"content": message})
                    response.raise_for_status()    
                    
                    if not test:
                        # Save to sent alerts file
                        with open("prob_alerts_sent.txt", "a") as f:
                            f.write(f"{alert_key}\n")
    
                except Exception as e:
                    logger.error(f"Webhook failed: {e}")
                
    
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
    
    def result_searcher(df, result):
        print(df[df['result'] == result])
        
    def match_searcher(df, match):
        print(df[df['match'] == match])
    
    pb_union_url = f'https://api.au.pointsbet.com/api/mes/v3/events/featured/competition/15797?page=1'
    pb_nrl_url = f'https://api.au.pointsbet.com/api/mes/v3/events/featured/competition/7593?page=1'
    
    palm_union_url = f'https://fixture.palmerbet.online/fixtures/sports/5bfcf787-edfc-48f4-b328-1d221aa07ae0/matches?sportType=rugbyunion&pageSize=24&channel=website'
    palm_tennis_url = f'https://fixture.palmerbet.online/fixtures/sports/9d6bbedd-0b09-4031-9884-e264499a2aa5/matches?sportType=tennis&pageSize=24&channel=website'
    palm_nrl_url = f'https://fixture.palmerbet.online/fixtures/sports/cf404de1-1953-4d55-b92e-4e022f186b22/matches?sportType=rugbyleague&pageSize=24&channel=website'
    
    betr_union_url = 'https://web20-api.bluebet.com.au/MasterCategory?EventTypeId=105&WithLevelledMarkets=true'
    betr_nrl_url = 'https://web20-api.bluebet.com.au/MasterCategory?EventTypeId=102&WithLevelledMarkets=true'
    
    WEBHOOK_ARBS = "https://discord.com/api/webhooks/1407711750824132620/dBAZkjoBIHPV-vUNk7C0E4MJ7nUtF1BKd4O1lpHhq_4qbk-47kew9bFmRiSpELAqk6i4"  
    WEBHOOK_PROBS = "https://discord.com/api/webhooks/1408080883885539439/bAnj7a_NBVxFyXecCMTBw84obdX4OMuO1388GDNfo1ViW4Kmylb0Gc5bITjrfiEOwRNc"  
    WEBHOOK_TEST = "https://discord.com/api/webhooks/1408057314455588864/jAclediH3bdFu-0PXK4Xbd7wykeU0NgJueMEaEwP8x3vJAExfZ-RFAT0FAdwT-alP2D4"
    
    # %% #---------Football--------#
    pb_football_compids = get_pb_comps('soccer')
    '''
    logger.info(f"Scraping Pointsbet EPL Data")
    pb_scraper = pb.PBSportsScraper(get_pb_url(136535),  chosen_date=chosen_date)
    pb_epl_markets = await pb_scraper.POINTSBET_scrape_sport(market_type='Match Result')
    
    logger.info(f"Scraping Sportsbet EPL Data")
    sb_scraper = sb.SBSportsScraper(get_sportsbet_url(29),  chosen_date=chosen_date)
    sb_epl_markets = await sb_scraper.SPORTSBET_scraper(competition_id=718)
    
    #EPL df
    
    bookmakers = {
        "Sportsbet": sb_epl_markets,
        "Pointsbet": pb_epl_markets,
    }
    
    dfs = {}
    
    for name, markets in bookmakers.items():
        rows = []
        for match, odds in markets.items():
            for result, price in odds.items():
                rows.append({"match": match, "result": result, f"{name}": price})
        dfs[name] = pd.DataFrame(rows)
    
    # Access them like:
    sb_epl_df = dfs["Sportsbet"]
    pb_epl_df = dfs["Pointsbet"]
        
    dfs = [sb_epl_df, pb_epl_df]
    price_cols = ['Sportsbet', 'Pointsbet']
    
    epl_df, epl_mkt_percents = fuzzy_merge_prices(dfs, price_cols, outcomes=3)

    print(epl_mkt_percents.head(60))
    print(epl_df)
    
    epl_arbs = epl_mkt_percents[epl_mkt_percents['mkt_percent'] < 1]
    arb_alert(epl_arbs)
    
    epl_price_diffs = epl_df[['result', 'match'] + price_cols]
    prob_alert(epl_price_diffs, diff_lim=0.01, test=True)'''
    
                
        
    logger.info(f"Scraping Pointsbet Football Data")
    pb_football_markets = {}
    for comp_id in pb_football_compids:
        pb_scraper = pb.PBSportsScraper(get_pb_url(comp_id),  chosen_date=chosen_date)
        comp_markets = await pb_scraper.POINTSBET_scrape_sport(market_type='Match Result')
        pb_football_markets.update(comp_markets)
    
    logger.info(f"Scraping Sportsbet Football Data")
    sb_scraper = sb.SBSportsScraper(get_sportsbet_url(29),  chosen_date=chosen_date)
    sb_football_markets = await sb_scraper.SPORTSBET_scraper()
    '''
    logger.info(f"Scraping Unibet football Data")
    ub_scraper = ub.UBSportsScraper(get_ub_url('football'),  chosen_date=chosen_date)
    ub_football_markets = await ub_scraper.UNIBET_scrape_football()'''
    
    
    #Football df
    bookmakers = {
        "Sportsbet": sb_football_markets,
        "Pointsbet": pb_football_markets
        #"Unibet": ub_football_markets
    }
    
    dfs = {}
    
    for name, markets in bookmakers.items():
        rows = []
        for match, odds in markets.items():
            for result, price in odds.items():
                rows.append({"match": match, "result": result, f"{name}": price})
        dfs[name] = pd.DataFrame(rows)
    
    # Access them like:
    sb_football_df = dfs["Sportsbet"]
    pb_football_df = dfs["Pointsbet"]
    #ub_football_df = dfs["Unibet"]
    
    dfs = [sb_football_df, pb_football_df]#, ub_football_df]
    
    for df in dfs:
        # Normalize results
        df['result_norm'] = df['result'].apply(normalize_result)
        # Normalize matches
        df['match_norm'] = df['match'].apply(normalize_match)
    
    #print(f"sb_football_df:{sb_football_df[['match_norm', 'result_norm']].head(50)}")
    #print(f"pb_football_df:{pb_football_df[['match_norm', 'result_norm']].head(50)}")
    #print(f"ub_football_df:{ub_football_df[['match_norm', 'result_norm']].head(50)}")
    
    price_cols = ['Sportsbet', 'Pointsbet']#, 'Unibet']
                
    football_df, football_mkt_percents = fuzzy_merge_prices(dfs, price_cols, match_threshold=70, result_threshold=50, outcomes=3)
    
    #print(football_df.head(60))
    print(football_mkt_percents.sort_values(by='mkt_percent', ascending=True).head(10))
            
    football_arbs = football_mkt_percents[football_mkt_percents['mkt_percent'] < 1]
    arb_alert(football_arbs)
    
    football_price_diffs = football_df[['result', 'match'] + price_cols]
    prob_alert(football_price_diffs, diff_lim=0.08)
    
    #result_searcher(pb_football_df, 'Al Ahli Jeddah')
    #match_searcher(pb_football_df, 'Al-Nassr Riyadh v Al Ahli Jeddah')
    
    
    # %% #---------Tennis--------#
    '''pb_tennis_compids = get_pb_comps('tennis')

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
        "Sportsbet": sb_tennis_markets,
        "Pointsbet": pb_tennis_markets,
        "Unibet": ub_tennis_markets
    }
    
    dfs = {}
    
    for name, markets in bookmakers.items():
        rows = []
        for match, odds in markets.items():
            for result, price in odds.items():
                rows.append({"match": match, "result": result, f"{name}": price})
        dfs[name] = pd.DataFrame(rows)
    
    # Access them like:
    sb_tennis_df = dfs["Sportsbet"]
    pb_tennis_df = dfs["Pointsbet"]
    ub_tennis_df = dfs["Unibet"]
    
    dfs = [sb_tennis_df, pb_tennis_df, ub_tennis_df]

    for df in dfs:
        # Normalize results
        df['result_norm'] = df['result'].apply(normalize_players_result)
        # Normalize matches
        df['match_norm'] = df['match'].apply(normalize_players_match)
    
    print(f"sb_tennis_df:{sb_tennis_df[['match_norm', 'result_norm']].head(50)}")
    print(f"pb_tennis_df:{pb_tennis_df[['match_norm', 'result_norm']].head(50)}")
    print(f"ub_tennis_df:{ub_tennis_df[['match_norm', 'result_norm']].head(50)}")
    
    
    price_cols = ['Sportsbet', 'Pointsbet', 'Unibet']
                
    tennis_df, tennis_mkt_percents = fuzzy_merge_prices(dfs, price_cols, outcomes=2, match_threshold=80, result_threshold=50, names=True)
    
    #print(tennis_df.head(60))
    #print(tennis_mkt_percents.sort_values(by='mkt_percent', ascending=True).head(60))
    
    #tennis_arbs = tennis_mkt_percents[tennis_mkt_percents['mkt_percent'] < 1]
    #arb_alert(tennis_arbs)
    
    #tennis_price_diffs = tennis_df[['result', 'match'] + price_cols]
    #prob_alert(tennis_price_diffs, diff_lim=0.07, test=True)'''
    
    
    
   
    
    # %% #---------NPC--------#
    logger.info(f"Scraping Sportsbet Union Data")
    sb_scraper = sb.SBSportsScraper(get_sportsbet_url(sportId=12),  chosen_date=chosen_date)
    sb_npc_markets = await sb_scraper.SPORTSBET_scraper_union(competition_id=27313)
    
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
        "Sportsbet": sb_npc_markets,
        "Pointsbet": pb_npc_markets,
        "Unibet": ub_npc_markets,
        "Palmerbet": palm_npc_markets,
        "Betr": betr_npc_markets  # Added Betr
    }
    
    dfs = {}
    
    # Convert each bookmaker's markets into a DataFrame
    for name, markets in bookmakers.items():
        rows = []
        for match, odds in markets.items():
            for result, price in odds.items():
                rows.append({"match": match, "result": result, f"{name}": price})
        dfs[name] = pd.DataFrame(rows)
    
    # Access individual DataFrames
    sb_npc_df = dfs["Sportsbet"]
    pb_npc_df = dfs["Pointsbet"]
    ub_npc_df = dfs["Unibet"]
    palm_npc_df = dfs["Palmerbet"]
    betr_npc_df = dfs["Betr"]  # Added Betr
    
    # List of DataFrames for merging
    dfs_list = [sb_npc_df, pb_npc_df, ub_npc_df, palm_npc_df, betr_npc_df]
    
    # Updated list of price columns for fuzzy_merge_prices
    price_cols = ['Sportsbet', 'Pointsbet', 'Unibet', 'Palmerbet', 'Betr']
    
    # Run fuzzy merge
    npc_df, npc_mkt_percents = fuzzy_merge_prices(dfs_list, price_cols, outcomes=2)
    
    #print(npc_df)
    #print(npc_df)
    print(npc_mkt_percents.head(10))
    
    npc_arbs = npc_mkt_percents[npc_mkt_percents['mkt_percent'] < 1]
    arb_alert(npc_arbs)
    
    npc_price_diffs = npc_df[['result', 'match'] + price_cols]
    prob_alert(npc_price_diffs, diff_lim=0.06)
        
    
    # %% #---------NRL--------#
    #get_sportsbet_compids(23)
    logger.info(f"Scraping Sportsbet NRL Data")
    sb_scraper = sb.SBSportsScraper(get_sportsbet_url(sportId=23),  chosen_date=chosen_date)
    sb_nrl_markets = await sb_scraper.SPORTSBET_scraper(competition_id=3436)
    
    logger.info(f"Scraping Pointsbet NRL Data")
    pb_scraper = pb.PBSportsScraper(pb_nrl_url,  chosen_date=chosen_date)
    pb_nrl_markets = await pb_scraper.POINTSBET_scrape_nrl(market_type='Match Result')
    
    logger.info(f"Scraping Unibet NRL Data")
    ub_scraper = ub.UBSportsScraper(get_ub_url('rugby_league'),  chosen_date=chosen_date)
    ub_nrl_markets = await ub_scraper.UNIBET_scrape_nrl(comp='NRL')
    
    logger.info(f"Scraping Palmersbet NRL Data")
    palm_scraper = palm.PalmerBetSportsScraper(palm_nrl_url,  chosen_date=chosen_date)
    palm_nrl_markets = await palm_scraper.PalmerBet_scrape_npc(comp='Australia National Rugby League')
    
    logger.info(f"Scraping Betr NRL Data")
    betr_scraper = betr.BetrSportsScraper(betr_nrl_url,  chosen_date=chosen_date)
    betr_nrl_markets = await betr_scraper.Betr_scrape_union(comp='NRL Telstra Premiership')
    
    # --- Combine bookmaker markets ---
    bookmakers = {
        "Sportsbet": sb_nrl_markets,
        "Pointsbet": pb_nrl_markets,
        "Unibet": ub_nrl_markets,
        "Palmerbet": palm_nrl_markets,
        "Betr": betr_nrl_markets  # Added Betr
    }
    
    dfs = {}
    
    # Convert each bookmaker's markets into a DataFrame
    for name, markets in bookmakers.items():
        rows = []
        for match, odds in markets.items():
            for result, price in odds.items():
                rows.append({"match": match, "result": result, f"{name}": price})
        dfs[name] = pd.DataFrame(rows)
    
    # Access individual DataFrames
    sb_nrl_df = dfs["Sportsbet"]
    pb_nrl_df = dfs["Pointsbet"]
    ub_nrl_df = dfs["Unibet"]
    palm_nrl_df = dfs["Palmerbet"]
    betr_nrl_df = dfs["Betr"]  # Added Betr
    
    #print(f'sb_nrl_df:{sb_nrl_df}')
    #print(f'pb_nrl_df:{pb_nrl_df}')
    #print(f'ub_nrl_df:{ub_nrl_df}')
    #print(f'palm_nrl_df:{palm_nrl_df}')
    #print(f'betr_nrl_df:{betr_nrl_df}')
    
    # List of DataFrames for merging
    dfs_list = [sb_nrl_df, pb_nrl_df, ub_nrl_df, palm_nrl_df, betr_nrl_df]
    
    # Updated list of price columns for fuzzy_merge_prices
    price_cols = ['Sportsbet', 'Pointsbet', 'Unibet', 'Palmerbet', 'Betr']
    
    # Run fuzzy merge
    nrl_df, nrl_mkt_percents = fuzzy_merge_prices(dfs_list, price_cols, outcomes=3)
    
    #print(nrl_df)
    #print(nrl_df.head(60))
    print(nrl_mkt_percents.head(10))
    
    nrl_arbs = nrl_mkt_percents[nrl_mkt_percents['mkt_percent'] < 1]
    arb_alert(nrl_arbs)
    
    nrl_price_diffs = nrl_df[['result', 'match'] + price_cols]
    prob_alert(nrl_price_diffs, diff_lim=0.06)
    
if __name__ == "__main__":
    asyncio.run(main())

