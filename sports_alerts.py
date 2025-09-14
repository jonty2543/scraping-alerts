import pytz
from datetime import datetime, timedelta
from loguru import logger
import sportsbet_scrapers as sb
import pointsbet_scrapers as pb
import unibet_scrapers as ub
import PalmerBet_scrapers as palm
import betr_scrapers as betr
import betright_scrapers as br
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
    
    def fuzzy_merge_prices(dfs, bookie_names, outcomes=3, match_threshold=80, result_threshold=70, names=False):
    
        # Return empty if all dfs are empty
        if all(df.empty for df in dfs):
            return pd.DataFrame(columns=['match', 'result', 'mkt_percent', 'best_price', 'best_bookie']), pd.DataFrame()
    
        # Pick the base_df as the longest DataFrame (most rows)
        non_empty_dfs = [(df, name) for df, name in zip(dfs, bookie_names) if not df.empty]
        if not non_empty_dfs:
            return pd.DataFrame(columns=['match', 'result', 'mkt_percent', 'best_price', 'best_bookie']), pd.DataFrame()
    
        base_df, base_bookie = max(non_empty_dfs, key=lambda x: len(x[0]))
        base_df = base_df.copy()
    
        # Normalize base_df
        if names:
            base_df['match_norm'] = base_df['match'].apply(normalize_players_match)
            base_df['result_norm'] = base_df['result'].apply(normalize_players_result)
        else:
            base_df['match_norm'] = base_df['match'].apply(normalize_match)
            base_df['result_norm'] = base_df['result'].apply(normalize_result)
    
        base_df['match_fuzzy'] = base_df['match_norm']
        base_df['result_fuzzy'] = base_df['result_norm']
    
        # Merge other DataFrames
        for other_df, bookie in zip(dfs, bookie_names):
            if bookie == base_bookie:  # skip base
                continue
    
            if other_df.empty:
                base_df[bookie] = 0.0
                continue
    
            other_df = other_df.copy()
    
            # Normalize
            if names:
                other_df['match_norm'] = other_df['match'].apply(normalize_players_match)
                other_df['result_norm'] = other_df['result'].apply(normalize_players_result)
            else:
                other_df['match_norm'] = other_df['match'].apply(normalize_match)
                other_df['result_norm'] = other_df['result'].apply(normalize_result)
    
            # Fuzzy match matches
            other_matches = other_df['match_norm'].dropna().unique()
            match_map = {}
            for base_mtch in base_df['match_fuzzy'].dropna().unique():
                best_mtch = process.extractOne(base_mtch, other_matches, scorer=fuzz.token_sort_ratio)
                if best_mtch and best_mtch[1] >= match_threshold:
                    match_map[base_mtch] = best_mtch[0]
    
            other_df['match_fuzzy'] = other_df['match_norm'].map({v: k for k, v in match_map.items()})
    
            # Fuzzy match results
            for base_mtch, other_mtch in match_map.items():
                base_rows = base_df[base_df['match_fuzzy'] == base_mtch]
                other_rows = other_df[other_df['match_norm'] == other_mtch]
                result_map = {}
                for res in base_rows['result_fuzzy'].dropna().unique():
                    best_res = process.extractOne(res, other_rows['result_norm'].dropna().unique(),
                                                 scorer=fuzz.token_sort_ratio)
                    if best_res and best_res[1] >= result_threshold:
                        result_map[res] = best_res[0]
                other_df.loc[other_rows.index, 'result_fuzzy'] = other_rows['result_norm'].map(
                    {v: k for k, v in result_map.items()}
                )
    
            # Ensure columns exist
            for col in ['match_fuzzy', 'result_fuzzy']:
                if col not in other_df.columns:
                    other_df[col] = None
                other_df[col] = other_df[col].astype(str).replace('nan', '')
    
            base_df['match_fuzzy'] = base_df['match_fuzzy'].astype(str).replace('nan', '')
            base_df['result_fuzzy'] = base_df['result_fuzzy'].astype(str).replace('nan', '')
    
            # Merge
            if bookie in other_df.columns:
                merge_cols = ['match_fuzzy', 'result_fuzzy', bookie]
                base_df = base_df.merge(other_df[merge_cols], on=['match_fuzzy', 'result_fuzzy'], how='left')
            else:
                base_df[bookie] = 0.0
    
        # Ensure all bookie columns exist (for missing ones)
        for bookie in bookie_names:
            if bookie not in base_df.columns:
                base_df[bookie] = 0.0
    
        # Best price / bookie
        bookie_cols = [b for b in bookie_names if b in base_df.columns]
        base_df['best_price'] = base_df[bookie_cols].max(axis=1, skipna=True)
        base_df['best_bookie'] = base_df.apply(
            lambda row: ', '.join([col for col in bookie_cols if row[col] == row['best_price']]), axis=1
        )
        base_df['best_prob'] = base_df['best_price'].apply(lambda x: 1 / x if pd.notnull(x) and x > 0 else 0.0)
    
        # Market %
        if not base_df.empty:
            match_mkt = base_df.groupby('match_fuzzy', as_index=False)['best_prob'].sum().rename(
                columns={'best_prob': 'mkt_percent'}
            )
            mkt_percents = base_df.merge(match_mkt, on='match_fuzzy', how='left')
            mkt_percents['mkt_percent'] = (mkt_percents['mkt_percent'] * 100).round(4)
            mkt_percents = mkt_percents[['match', 'result', 'mkt_percent', 'best_price', 'best_bookie']]
        else:
            mkt_percents = pd.DataFrame(columns=['match', 'result', 'mkt_percent', 'best_price', 'best_bookie'])
    
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
                
            mkt_percent = group['mkt_percent'].iloc[0]
            
            outcomes = []
            for _, row in group.iterrows():
                bookies = row['best_bookie'] if isinstance(row['best_bookie'], str) else ", ".join(row['best_bookie'])
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
                if pd.notna(row[bookie]) and row[bookie] > 0:
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
    
    sb_ufc_url = 'https://www.sportsbet.com.au/apigw/sportsbook-sports/Sportsbook/Sports/Class/71/Events?displayType=coupon&detailsLevel=O'
    
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
    
    
    def process_odds(
        bookmakers: dict,
        price_cols: list,
        table_name: str,
        match_threshold: int = 80,
        outcomes: int = 2,
    ):
        """
        Process odds from multiple bookmakers, merge, calculate market %, 
        insert into Supabase, and send arb alerts.
        
        Args:
            bookmakers: dict of {bookmaker_name: markets_dict}
            price_cols: list of bookmaker names (must match keys in bookmakers)
            table_name: Supabase table to insert into
            match_threshold: fuzzy match threshold for merging
            outcomes: number of outcomes per market (e.g. 2 for MMA, 3 for soccer)
        """
        dfs = {}
    
        # Convert each bookmaker's markets into a DataFrame
        for name, markets in bookmakers.items():
            rows = []
            for match, odds in markets.items():
                for result, price in odds.items():
                    rows.append({"match": match, "result": result, f"{name}": price})
            dfs[name] = pd.DataFrame(rows)
    
        dfs_list = [dfs[name] for name in price_cols]
    
        logger.info(f"Merging {table_name} dfs")
        merged_df, mkt_percents = fuzzy_merge_prices(
            dfs_list, price_cols, match_threshold=match_threshold, outcomes=outcomes
        )
    
        # Attach market %
        merged_df = pd.merge(
            merged_df,
            mkt_percents[["match", "result", "mkt_percent"]],
            on=["match", "result"],
        )
        merged_df = merged_df[merged_df["mkt_percent"] > 80]

        
        # Log coverage per bookmaker
        for name in price_cols:
            if len(dfs[name]) > 0:
                logger.info(
                    f"{name} matched: {merged_df[name].count() / len(dfs[name]):.2%}"
                )
    
        # Cleanup + rename
        col_map = {
            "match": "Match",
            "result": "Result",
            "best_bookie": "Best Bookie",
            "best_price": "Best Price",
            "mkt_percent": "Market %",
        }
        col_map.update({name: name for name in price_cols})
    
        cleaned_df = merged_df.fillna(0.0)
        df_mapped = cleaned_df.rename(columns=col_map)
        df_mapped = df_mapped[[col for col in df_mapped.columns if col in col_map.values()]]
        df_mapped["Best Bookie"] = df_mapped["Best Bookie"].apply(
            lambda x: ", ".join(x) if isinstance(x, list) else str(x)
        )
        df_mapped["Market %"] = df_mapped["Market %"].round(4)
    
        # Convert to dict records
        records = df_mapped.to_dict(orient="records")
    
        # Supabase insert
        logger.info(f"Clearing {table_name} table before insert...")
        supabase.table(table_name).delete().neq("Match", "").execute()
    
        logger.info(f"Inserting fresh {table_name} records...")
        supabase.table(table_name).insert(records).execute()
    
        # Print some sample market %
        print(mkt_percents[["match", "mkt_percent"]].head(10))
    
        # Arbitrage alerts
        logger.info(f"Sending {table_name} arb alerts")
        arbs = mkt_percents[(mkt_percents["mkt_percent"] > 90) & (mkt_percents["mkt_percent"] < 100)]
        arb_alert(arbs)
    
        return df_mapped, mkt_percents
    
    def get_pb_url(competition_id: int):
        return f'https://api.au.pointsbet.com/api/mes/v3/events/featured/competition/{competition_id}?page=1'
    
    def get_ub_url(sport):
        return f'https://www.unibet.com.au/sportsbook-feeds/views/filter/{sport}/all/matches?includeParticipants=true&useCombined=true'
    
    def get_betright_url(sportId: int):
        return f'https://next-api.betright.com.au/Sports/MasterCategory?eventTypeId={sportId}'
    
    def result_searcher(df, result):
        print(df[df['result'] == result])
        
    def match_searcher(df, match):
        print(df[df['match'] == match])

    pb_union_url = f'https://api.au.pointsbet.com/api/mes/v3/events/featured/competition/15797?page=1'
    pb_nrl_url = f'https://api.au.pointsbet.com/api/mes/v3/events/featured/competition/7593?page=1'
    
    palm_union_url = f'https://fixture.palmerbet.online/fixtures/sports/5bfcf787-edfc-48f4-b328-1d221aa07ae0/matches?sportType=rugbyunion&pageSize=1000&channel=website'
    palm_tennis_url = f'https://fixture.palmerbet.online/fixtures/sports/9d6bbedd-0b09-4031-9884-e264499a2aa5/matches?sportType=tennis&pageSize=1000&channel=website'
    palm_nrl_url = f'https://fixture.palmerbet.online/fixtures/sports/cf404de1-1953-4d55-b92e-4e022f186b22/matches?sportType=rugbyleague&pageSize=1000&channel=website'
    palm_football_url = f'https://fixture.palmerbet.online/fixtures/sports/b4073512-cdd5-4953-950f-3f7ad31fa955/matches?sportType=Soccer&pageSize=1000'
    
    betr_union_url = 'https://web20-api.bluebet.com.au/MasterCategory?EventTypeId=105&WithLevelledMarkets=true'
    betr_nrl_url = 'https://web20-api.bluebet.com.au/MasterCategory?EventTypeId=102&WithLevelledMarkets=true'
    betr_mma_url = 'https://web20-api.bluebet.com.au/MasterCategory?EventTypeId=128&WithLevelledMarkets=true'
    
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
    key = "npc/2025-09-12"
    #key = f"npc/{monday}"
    
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
        

    
    # %% #---------Football--------#
    
    pb_football_compids = get_pb_comps('soccer')           
        
    logger.info(f"Scraping Pointsbet Football Data")
    pb_football_markets = {}
    for comp_id in pb_football_compids:
        pb_scraper = pb.PBSportsScraper(get_pb_url(comp_id),  chosen_date=chosen_date)
        comp_markets = await pb_scraper.POINTSBET_scrape_sport(market_type='Match Result')
        pb_football_markets.update(comp_markets)
        
    time.sleep(5)
    
    logger.info(f"Scraping Sportsbet Football Data")
    sb_scraper = sb.SBSportsScraper(get_sportsbet_url(29),  chosen_date=chosen_date)
    sb_football_markets = await sb_scraper.SPORTSBET_scraper()
    
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
    br_scraper = br.BRSportsScraper(get_betright_url(100),  chosen_date=chosen_date)
    br_football_markets = await br_scraper.BETRIGHT_scrape_football()
    
    time.sleep(5)
        
    # Football df
    bookmakers = {
        "Sportsbet": sb_football_markets,
        "Pointsbet": pb_football_markets,
        "Palmerbet": palm_football_markets,
        "Betright": br_football_markets
    }
    
    price_cols = ['Sportsbet', 'Pointsbet', 'Palmerbet', 'Betright']  # , 'Unibet']
    
    process_odds(bookmakers, price_cols, table_name="Football Odds", outcomes=3)

    
    
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
    '''
    
    
    (1/2.31)*2.3 + ((1-1/2.31)*-1)
   
    
    # %% #---------union--------#
    logger.info(f"Scraping Sportsbet Union Data")
    sb_scraper = sb.SBSportsScraper(get_sportsbet_url(sportId=12),  chosen_date=chosen_date)
    sb_union_markets = await sb_scraper.SPORTSBET_scraper_union()
    
    time.sleep(5)
    
    logger.info(f"Scraping Unibet union Data")
    ub_scraper = ub.UBSportsScraper(get_ub_url('rugby_union'),  chosen_date=chosen_date)
    ub_union_markets = await ub_scraper.UNIBET_scrape_union()
    
    time.sleep(5)
    
    pb_union_compids = get_pb_comps('rugby-union')  
    pb_union_markets = {}   
    logger.info(f"Scraping Pointsbet union Data")
    for comp_id in pb_union_compids:      
        pb_scraper = pb.PBSportsScraper(get_pb_url(comp_id),  chosen_date=chosen_date)
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
    br_scraper = br.BRSportsScraper(get_betright_url(105),  chosen_date=chosen_date)
    br_union_markets = await br_scraper.BETRIGHT_scraper()
    
    time.sleep(5)
    
    logger.info("Fetching union model odds")    

    
    bookmakers = {
        "Sportsbet": sb_union_markets,
        "Pointsbet": pb_union_markets,
        "Unibet": ub_union_markets,
        "Palmerbet": palm_union_markets,
        "Betr": betr_union_markets,  # Added Betr
        "Betright": br_union_markets,
        "Model": model_union_markets
    }
    
    
    # Updated list of price columns for fuzzy_merge_prices
    price_cols = ['Sportsbet', 'Pointsbet', 'Unibet', 'Palmerbet', 'Betr', 'Betright', 'Model']
    
    process_odds(bookmakers, price_cols, table_name="Rugby Union Odds")
    
    
    # %% #---------NRL--------#
    #get_sportsbet_compids(23)
    logger.info(f"Scraping Sportsbet NRL Data")
    sb_scraper = sb.SBSportsScraper(get_sportsbet_url(sportId=23),  chosen_date=chosen_date)
    sb_nrl_markets = await sb_scraper.SPORTSBET_scraper(competition_id=3436)
    
    time.sleep(5)
    
    logger.info(f"Scraping Pointsbet NRL Data")
    pb_scraper = pb.PBSportsScraper(pb_nrl_url,  chosen_date=chosen_date)
    pb_nrl_markets = await pb_scraper.POINTSBET_scrape_nrl(market_type='Match Result')
    
    time.sleep(5)
    
    logger.info(f"Scraping Unibet NRL Data")
    ub_scraper = ub.UBSportsScraper(get_ub_url('rugby_league'),  chosen_date=chosen_date)
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
    br_scraper = br.BRSportsScraper(get_betright_url(102),  chosen_date=chosen_date)
    br_nrl_markets = await br_scraper.BETRIGHT_scraper()
    
    time.sleep(5)
    
    # --- Combine bookmaker markets ---
    bookmakers = {
        "Sportsbet": sb_nrl_markets,
        "Pointsbet": pb_nrl_markets,
        "Unibet": ub_nrl_markets,
        "Palmerbet": palm_nrl_markets,
        "Betr": betr_nrl_markets,  # Added Betr
        "Betright": br_nrl_markets
    }
    
    price_cols = ['Sportsbet', 'Pointsbet', 'Unibet', 'Palmerbet', 'Betr', 'Betright']

    process_odds(bookmakers, price_cols, table_name="NRL Odds")
    
    
    #%% E-sports
    
    logger.info(f"Scraping Sportsbet E Sports Data")
    sb_scraper = sb.SBSportsScraper(get_sportsbet_url(sportId=206),  chosen_date=chosen_date)
    sb_esports_markets = await sb_scraper.SPORTSBET_scraper()
    
    time.sleep(5)
    
    pb_esports_compids = get_pb_comps('e-sports')  
    pb_esports_markets = {}   
    logger.info(f"Scraping Pointsbet E sports Data")
    for comp_id in pb_esports_compids:      
        pb_scraper = pb.PBSportsScraper(get_pb_url(comp_id),  chosen_date=chosen_date)
        comp_markets = await pb_scraper.POINTSBET_scrape_sport(market_type='Match Result')
        pb_esports_markets.update(comp_markets)
        
    time.sleep(5)
        
    logger.info(f"Scraping Unibet E sports Data")
    ub_scraper = ub.UBSportsScraper(get_ub_url('esports'),  chosen_date=chosen_date)
    ub_esports_markets = await ub_scraper.UNIBET_scrape_sport()
    
    time.sleep(5)
    
    logger.info("Scraping betright E Sports data")
    br_scraper = br.BRSportsScraper(get_betright_url(124),  chosen_date=chosen_date)
    br_esports_markets = await br_scraper.BETRIGHT_scraper()
    
    time.sleep(5)
    
    # --- Combine bookmaker markets ---
    bookmakers = {
        "Sportsbet": sb_esports_markets,
        "Pointsbet": pb_esports_markets,
        "Unibet": ub_esports_markets,
        "Betright": br_esports_markets
    }
    
    price_cols = ["Sportsbet", "Pointsbet", "Unibet", "Betright"]
    
    process_odds(bookmakers, price_cols, table_name="E-Sports Odds")
    
    
    #%% mma
    
    logger.info(f"Scraping Sportsbet UFC Data")
    sb_scraper = sb.SBSportsScraper(sb_ufc_url,  chosen_date=chosen_date)
    sb_mma_markets = await sb_scraper.SPORTSBET_scrape_mma()
    
    time.sleep(5)
    
    pb_mma_compids = get_pb_comps('mma')  
    pb_mma_markets = {}   
    logger.info(f"Scraping Pointsbet MMA Data")
    for comp_id in pb_mma_compids:      
        pb_scraper = pb.PBSportsScraper(get_pb_url(comp_id),  chosen_date=chosen_date)
        comp_markets = await pb_scraper.POINTSBET_scrape_sport(market_type='Fight Result')
        pb_mma_markets.update(comp_markets)
        
    time.sleep(5)
        
    logger.info(f"Scraping Unibet mma Data")
    ub_scraper = ub.UBSportsScraper(get_ub_url('ufc_mma'),  chosen_date=chosen_date)
    ub_mma_markets = await ub_scraper.UNIBET_scrape_sport()
    
    time.sleep(5)
    
    # --- Combine bookmaker markets ---
    bookmakers = {
        "Sportsbet": sb_mma_markets,
        "Pointsbet": pb_mma_markets,
        "Unibet": ub_mma_markets
    }
    
    price_cols = ["Sportsbet", "Pointsbet", "Unibet"]
    
    process_odds(bookmakers, price_cols, table_name="MMA Odds")
    
    
    
    #%% Basketball
    '''
    logger.info(f"Scraping Sportsbet Basketball Data")
    sb_scraper = sb.SBSportsScraper(get_sportsbet_url(sportId=16),  chosen_date=chosen_date)
    sb_basketball_markets = await sb_scraper.SPORTSBET_scraper()
    
    pb_basketball_compids = get_pb_comps('basketball')  
    pb_basketball_markets = {}   
    logger.info(f"Scraping Pointsbet basketball Data")
    for comp_id in pb_basketball_compids:      
        pb_scraper = pb.PBSportsScraper(get_pb_url(comp_id),  chosen_date=chosen_date)
        comp_markets = await pb_scraper.POINTSBET_scrape_nrl(market_type='Head to Head')
        pb_basketball_markets.update(comp_markets)
    '''
    
if __name__ == "__main__":
    asyncio.run(main())

