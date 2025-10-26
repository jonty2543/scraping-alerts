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

chosen_date = datetime.now(pytz.timezone("Australia/Brisbane")).date().strftime("%Y-%m-%d")
offset = (datetime.now(pytz.timezone("Australia/Brisbane")).date().weekday() - 0) % 7  
monday = datetime.now(pytz.timezone("Australia/Brisbane")).date() - timedelta(days=offset)
one_week = (datetime.now(pytz.timezone("Australia/Brisbane")) + timedelta(14)).date().strftime("%Y-%m-%d")
two_week = (datetime.now(pytz.timezone("Australia/Brisbane")) + timedelta(14)).date().strftime("%Y-%m-%d")


pb_union_url = f'https://api.au.pointsbet.com/api/mes/v3/events/featured/competition/15797?page=1'
pb_nrl_url = f'https://api.au.pointsbet.com/api/mes/v3/events/featured/competition/7593?page=1'

palm_union_url = f'https://fixture.palmerbet.online/fixtures/sports/5bfcf787-edfc-48f4-b328-1d221aa07ae0/matches?sportType=rugbyunion&pageSize=1000&channel=website'
palm_tennis_url = f'https://fixture.palmerbet.online/fixtures/sports/9d6bbedd-0b09-4031-9884-e264499a2aa5/matches?sportType=tennis&pageSize=1000&channel=website'
palm_nrl_url = f'https://fixture.palmerbet.online/fixtures/sports/cf404de1-1953-4d55-b92e-4e022f186b22/matches?sportType=rugbyleague&pageSize=1000&channel=website'
palm_football_url = f'https://fixture.palmerbet.online/fixtures/sports/b4073512-cdd5-4953-950f-3f7ad31fa955/matches?sportType=Soccer&pageSize=1000'
palm_basketball_url = f'https://fixture.palmerbet.online/fixtures/sports/b26e5acc-02ff-4b22-ae69-0491fbd2500e/matches?sportType=basketball&pageSize=1000&channel=website'

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
    """
    Fuzzy merge bookmaker DataFrames by (match, date, result).
    - Fuzzy matches 'match' strings within the same date
    - Ensures matches on the same date are treated separately
    - Returns merged base_df and market % DataFrame
    """

    import pandas as pd
    from rapidfuzz import fuzz, process

    # Return empty if all dfs are empty
    if all(df.empty for df in dfs):
        cols = ['match', 'date', 'result', 'mkt_percent', 'best_price', 'best_bookie']
        return pd.DataFrame(columns=cols), pd.DataFrame(columns=cols)

    # Pick the base_df as the longest non-empty DataFrame
    non_empty_dfs = [(df, name) for df, name in zip(dfs, bookie_names) if not df.empty]
    if not non_empty_dfs:
        cols = ['match', 'date', 'result', 'mkt_percent', 'best_price', 'best_bookie']
        return pd.DataFrame(columns=cols), pd.DataFrame(columns=cols)

    base_df, base_bookie = max(non_empty_dfs, key=lambda x: len(x[0]))
    base_df = base_df.copy()

    # --- Normalize base_df ---
    if names:
        base_df['match_norm'] = base_df['match'].apply(normalize_players_match)
        base_df['result_norm'] = base_df['result'].apply(normalize_players_result)
    else:
        base_df['match_norm'] = base_df['match'].apply(normalize_match)
        base_df['result_norm'] = base_df['result'].apply(normalize_result)

    if 'date' in base_df.columns:
        base_df['date'] = pd.to_datetime(base_df['date'])

    # Initialize fuzzy columns
    base_df['match_fuzzy'] = base_df['match_norm']
    base_df['result_fuzzy'] = base_df['result_norm']

    # --- Merge other DataFrames ---
    for other_df, bookie in zip(dfs, bookie_names):
        if bookie == base_bookie:
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

        if 'date' in other_df.columns:
            other_df['date'] = pd.to_datetime(other_df['date'])

        # Initialize empty fuzzy columns upfront to avoid KeyErrors later
        for col in ['match_fuzzy', 'result_fuzzy']:
            if col not in other_df.columns:
                other_df[col] = None

        # --- Fuzzy match by (match, date) ---
        other_matches = other_df[['match_norm', 'date']].dropna().drop_duplicates()
        match_map = {}

        for _, base_row in base_df[['match_fuzzy', 'date']].dropna().drop_duplicates().iterrows():
            base_match, base_date = base_row['match_fuzzy'], base_row['date']

            same_date_matches = other_matches[other_matches['date'] == base_date]
            if same_date_matches.empty:
                continue

            best_mtch = process.extractOne(base_match, same_date_matches['match_norm'], scorer=fuzz.token_sort_ratio)
            if best_mtch and best_mtch[1] >= match_threshold:
                match_map[(base_match, base_date)] = best_mtch[0]

        # Map fuzzy matches back by (match, date)
        for (base_match, base_date), other_match in match_map.items():
            mask = (other_df['match_norm'] == other_match) & (other_df['date'] == base_date)
            other_df.loc[mask, 'match_fuzzy'] = base_match

        # --- Fuzzy match results within same match + date ---
        for (base_match, base_date), other_match in match_map.items():
            base_rows = base_df[(base_df['match_fuzzy'] == base_match) & (base_df['date'] == base_date)]
            other_rows = other_df[(other_df['match_norm'] == other_match) & (other_df['date'] == base_date)]
            result_map = {}

            for res in base_rows['result_fuzzy'].dropna().unique():
                best_res = process.extractOne(
                    res, other_rows['result_norm'].dropna().unique(), scorer=fuzz.token_sort_ratio
                )
                if best_res and best_res[1] >= result_threshold:
                    result_map[res] = best_res[0]

            # Assign mapped results safely
            if result_map:
                inv_map = {v: k for k, v in result_map.items()}
                other_df.loc[other_rows.index, 'result_fuzzy'] = other_rows['result_norm'].map(inv_map)

        # --- Clean up NaNs and prepare for merge ---
        for col in ['match_fuzzy', 'result_fuzzy']:
            if col not in other_df.columns:
                other_df[col] = ''
            else:
                other_df[col] = other_df[col].astype(str).replace('nan', '')
            base_df[col] = base_df[col].astype(str).replace('nan', '')

        merge_keys = ['match_fuzzy', 'result_fuzzy']
        if 'date' in base_df.columns and 'date' in other_df.columns:
            merge_keys.append('date')

        if bookie in other_df.columns:
            merge_cols = merge_keys + [bookie]
            base_df = base_df.merge(other_df[merge_cols], on=merge_keys, how='left')
        else:
            base_df[bookie] = 0.0

    # --- Ensure all bookie columns exist ---
    for bookie in bookie_names:
        if bookie not in base_df.columns:
            base_df[bookie] = 0.0

    # --- Compute best prices & market % ---
    bookie_cols = [b for b in bookie_names if b in base_df.columns and b != "Model"]

    base_df['best_price'] = base_df[bookie_cols].max(axis=1, skipna=True)
    base_df['best_bookie'] = base_df.apply(
        lambda row: ', '.join([col for col in bookie_cols if row[col] == row['best_price']]), axis=1
    )
    base_df['best_prob'] = base_df['best_price'].apply(lambda x: 1 / x if pd.notnull(x) and x > 0 else 0.0)

    # --- Market % per (match, date) ---
    group_cols = ['match_fuzzy']
    if 'date' in base_df.columns:
        group_cols.append('date')

    match_mkt = (
        base_df.groupby(group_cols, as_index=False)['best_prob']
        .sum()
        .rename(columns={'best_prob': 'mkt_percent'})
    )

    mkt_percents = base_df.merge(match_mkt, on=group_cols, how='left')
    mkt_percents['mkt_percent'] = (mkt_percents['mkt_percent'] * 100).round(4)

    mkt_percents = mkt_percents[['match', 'date', 'result', 'mkt_percent', 'best_price', 'best_bookie']]

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
        date = row.get('date', '')

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


def get_surge_comps(sport):
    surge_comps_url = f'https://api.blackstream.com.au/api/sports/v1/sports/{sport}/competitions'
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/115.0.0.0 Safari/537.36",
        "Accept": "application/json",
    }

    response = requests.get(surge_comps_url, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        comps = []
        for region in data.get("data", {}).get("regions", []):
            for comp in region.get("competitions", []):
                comps.append(
                    comp.get("id")
                )
            
    else:
        print(f"Failed to retrieve data: {response.status_code} {response.text}")
        
    return comps


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

def make_json_safe(df: pd.DataFrame) -> pd.DataFrame:
    """Convert all datetime/Timestamp columns to ISO string for JSON serialization."""
    df = df.copy()
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime("%Y-%m-%d")
    return df

def process_odds(
    bookmakers: dict,
    price_cols: list,
    table_name: str,
    match_threshold: int = 90,
    outcomes: int = 2,
    names=False
):
    """
    Process odds from multiple bookmakers, merge, calculate market %, 
    insert into Supabase, and send arb alerts.

    Args:
        bookmakers: dict of {bookmaker_name: {(match, date): {result: odds}}}
        price_cols: list of bookmaker names (must match keys in bookmakers)
        table_name: Supabase table to insert into
        match_threshold: fuzzy match threshold for merging
        outcomes: number of outcomes per market (e.g. 2 for MMA, 3 for soccer)
    """
    dfs = {}

    # Convert each bookmaker's markets into a DataFrame
    for name, markets in bookmakers.items():
        rows = []
        for (match, date), odds in markets.items():
            if len(odds) >= outcomes:
                for result, price in odds.items():
                    rows.append({
                        "match": match,
                        "date": date,
                        "result": result,
                        f"{name}": price
                    })
        df = pd.DataFrame(rows)

        if df.empty:
            logger.warning(f"Skipping {name} entirely — no valid markets found.")
            continue

        dfs[name] = df

    valid_price_cols = [name for name in price_cols if name in dfs and not dfs[name].empty]
    if not valid_price_cols:
        logger.error("❌ No valid bookmakers available after filtering. Exiting early.")
        return None, None

    dfs_list = [dfs[name] for name in valid_price_cols]
    logger.info(f"Including {len(valid_price_cols)} valid bookmakers: {valid_price_cols}")

    logger.info(f"Merging {table_name} dfs")
    merged_df, mkt_percents = fuzzy_merge_prices(
        dfs_list, price_cols, match_threshold=match_threshold, outcomes=outcomes
    )

    # Attach market %
    merged_df = pd.merge(
        merged_df,
        mkt_percents[["match", "result", "mkt_percent"]],
        on=["match", "result"]
    )
    merged_df = merged_df[merged_df["mkt_percent"] > 80]

    # Log coverage per bookmaker
    for name in valid_price_cols:
        total_rows = len(dfs.get(name, []))
        matched_rows = merged_df[name].count() if name in merged_df.columns else 0
        if total_rows > 0:
            logger.info(f"{name} matched: {matched_rows / total_rows:.2%}")
        else:
            logger.warning(f"{name} had no valid rows to match.")

    # Cleanup + rename
    col_map = {
        "match": "Match",
        "date": "Date",
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

    # Fetch current table for fluc comparison
    current_table = supabase.table(table_name).select("*").execute()
    current_df = pd.DataFrame(current_table.data)

    for col in ['Match', 'Date', 'Result'] + price_cols:
        if col not in current_df.columns:
            current_df[col] = None
            
    dupes = df_mapped[df_mapped.duplicated(subset=["Match", "Date", "Result"], keep=False)]
    if not dupes.empty:
        logger.warning(f"⚠️ Found {len(dupes)} duplicate (Match, Date, Result) rows before insert.")
        print(dupes[["Match", "Date", "Result"]])
        
    df_mapped["Date"] = pd.to_datetime(df_mapped["Date"]).dt.strftime("%Y-%m-%d")

    df_mapped = df_mapped.drop_duplicates(
        subset=["Match", "Date", "Result"],
        keep="last"   # keep last so newer odds override older ones
    ).reset_index(drop=True)

    # Convert to dict records for table insert
    df_mapped = make_json_safe(df_mapped)
    records = df_mapped.to_dict(orient="records")

    # --- Price Fluctuation Analysis ---
    flucs = pd.merge(
        df_mapped[['Match', 'Date', 'Result'] + price_cols],
        current_df[['Match', 'Date', 'Result'] + price_cols],
        on=['Match', 'Date', 'Result'],
        suffixes=('_new', '_old')
    )

    new_prices = flucs.melt(
        id_vars=['Match', 'Date', 'Result'],
        value_vars=[f"{c}_new" for c in price_cols],
        var_name='Bookie',
        value_name='New Price'
    )
    new_prices['Bookie'] = new_prices['Bookie'].str.replace('_new', '')

    old_prices = flucs.melt(
        id_vars=['Match', 'Date', 'Result'],
        value_vars=[f"{c}_old" for c in price_cols],
        var_name='Bookie',
        value_name='Old Price'
    )
    old_prices['Bookie'] = old_prices['Bookie'].str.replace('_old', '')

    # Combine back into long tidy DataFrame
    flucs_long = pd.merge(
        new_prices,
        old_prices,
        on=['Match', 'Date', 'Result', 'Bookie']
    )

    flucs_long['Time'] = datetime.now(pytz.timezone("Australia/Brisbane")).strftime('%Y-%m-%d %H:%M:%S')
    flucs_long['Prob Change'] = None

    mask = (flucs_long['New Price'] > 0) & (flucs_long['Old Price'] > 0)
    flucs_long.loc[mask, 'Prob Change'] = (
        1 / flucs_long.loc[mask, 'New Price'] -
        1 / flucs_long.loc[mask, 'Old Price']
    )

    flucs_long = flucs_long.replace([np.nan, np.inf, -np.inf], None)
    flucs_long['Sport'] = table_name.replace(" Odds", "")

    # Insert recent flucs
    flucs_long = make_json_safe(flucs_long)

    records_flucs = flucs_long.to_dict(orient="records")
    if records_flucs:
        try:
            supabase.table("Recent Flucs").insert(records_flucs).execute()
            print(f"✅ Inserted {len(records_flucs)} records into Recent Flucs.")
        except Exception as e:
            print("❌ Failed to insert records into Supabase:", e)
    else:
        print("⚠️ No flucs records to insert. Skipping.")

    # --- Aggregate historical prices ---
    flucs_long['Price'] = flucs_long['New Price']
    prices = flucs_long[['Match', 'Date', 'Result', 'Bookie', 'Price', 'Time', 'Sport']]

    agg_prices = (
        prices.loc[prices['Price'].gt(0) & prices['Price'].notna()]
        .sort_values(['Result', 'Match', 'Date', 'Price'], ascending=[True, True, True, False])
        .groupby(['Result', 'Match', 'Date'], as_index=False)
        .agg({
            'Price': ['mean', 'max'],
            'Bookie': 'first',
            'Time': 'max',
            'Sport': 'max'
        })
    )

    agg_prices.columns = ['Result', 'Match', 'Date', 'Average Price', 'Best Price', 'Best Bookie', 'Time', 'Sport']
    agg_prices = make_json_safe(agg_prices)
    records_prices = agg_prices.to_dict(orient="records")

    if records_prices:
        # Delete old matching rows first
        for rec in records_prices:
            supabase.table("Historical Odds").delete()\
                .eq("Match", rec.get("Match"))\
                .eq("Result", rec.get("Result"))\
                .eq("Date", rec.get("Date"))\
                .execute()

        try:
            supabase.table("Historical Odds").insert(records_prices).execute()
            print(f"✅ Inserted {len(records_prices)} records into Historical Odds.")
        except Exception as e:
            print("❌ Failed to insert records into Supabase:", e)

    # Clean up old flucs
    time_threshold = (datetime.now(pytz.timezone("Australia/Brisbane")) - timedelta(hours=3)).isoformat()
    supabase.table("Recent Flucs").delete().lt("Time", time_threshold).execute()

    # Refresh main odds table
    logger.info(f"Clearing {table_name} table before insert...")
    supabase.table(table_name).delete().neq("Match", "").execute()

    logger.info(f"Inserting fresh {table_name} records...")
    supabase.table(table_name).insert(records).execute()

    # Print some sample market %
    print(mkt_percents[["match", "mkt_percent"]].head(10))

    # Arbitrage alerts
    logger.info(f"Sending {table_name} arb alerts")
    arbs = mkt_percents[
        (mkt_percents["mkt_percent"] > 90) & 
        (mkt_percents["mkt_percent"] < 100)
    ]
    arb_alert(arbs)

    return df_mapped, mkt_percents

def get_pb_url(competition_id: int):
    return f'https://api.au.pointsbet.com/api/mes/v3/events/featured/competition/{competition_id}?page=1'

def get_ub_url(sport):
    return f'https://www.unibet.com.au/sportsbook-feeds/views/filter/{sport}/all/matches?includeParticipants=true&useCombined=true'

def get_betright_url(sportId: int):
    return f'https://next-api.betright.com.au/Sports/MasterCategory?eventTypeId={sportId}'

def get_betdeluxe_url(sport):
    return f'https://api.blackstream.com.au/api/sports/v1/sports/{sport}/competitions'

def get_surge_url(sport, comp_id):
    return f'https://api.blackstream.com.au/api/sports/v1/sports/{sport}/competitions/{comp_id}/events'


def result_searcher(df, result):
    print(df[df['result'] == result])
    
def match_searcher(df, match):
    print(df[df['match'] == match])

