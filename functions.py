import pytz
from datetime import datetime, timedelta
from loguru import logger
import os
from pathlib import Path

import scrapers.sportsbet_scrapers as sb
import scrapers.pointsbet_scrapers as pb
import scrapers.unibet_scrapers as ub
import scrapers.PalmerBet_scrapers as palm
import scrapers.betr_scrapers as betr
import scrapers.betright_scrapers as br
import scrapers.betdeluxe_scrapers as bd
import scrapers.surge_scrapers as ss

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

from collections import defaultdict
import math
import json
from typing import Dict, Iterable, List, Optional
from zoneinfo import ZoneInfo


def _load_local_env(path=".env"):
    env_path = Path(path)
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, val = line.split("=", 1)
        key = key.strip()
        val = val.strip().strip("'").strip('"')
        os.environ.setdefault(key, val)


_load_local_env()

chosen_date = datetime.now(pytz.timezone("Australia/Brisbane")).date().strftime("%Y-%m-%d")
offset = (datetime.now(pytz.timezone("Australia/Brisbane")).date().weekday() - 0) % 7  
monday = datetime.now(pytz.timezone("Australia/Brisbane")).date() - timedelta(days=offset)
one_week = (datetime.now(pytz.timezone("Australia/Brisbane")) + timedelta(7)).date().strftime("%Y-%m-%d")
two_week = (datetime.now(pytz.timezone("Australia/Brisbane")) + timedelta(14)).date().strftime("%Y-%m-%d")
one_month = (datetime.now(pytz.timezone("Australia/Brisbane")) + timedelta(60)).date().strftime("%Y-%m-%d")

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


WEBHOOK_ARBS = os.getenv("WEBHOOK_ARBS", "")
WEBHOOK_PROBS = os.getenv("WEBHOOK_PROBS", "")
WEBHOOK_TEST = os.getenv("WEBHOOK_TEST", "")

# ---- Supabase credentials ----
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY. Add them to .env for local runs.")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

BASE_URL = "https://gamma-api.polymarket.com"
CLOB_URL = "https://clob.polymarket.com"
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "pm-sports-orderbook/0.1",
    "Accept": "application/json"
})

def get_no_vig_odds_multiway(odds: list):
    """
    :param odds: List of original odds for a multi-way market.
    :return: Tuple of no-vig (fair) odds calculated using the iterative method.
    """
    c, target_overround, accuracy, current_error = 1, 0, 3, 1000
    max_error = (10 ** (-accuracy)) / 2

    fair_odds = list()
    while current_error > max_error:

        f = - 1 - target_overround
        for o in odds:
            f += (1 / o) ** c

        f_dash = 0
        for o in odds:
            f_dash += ((1 / o) ** c) * (-math.log(o))

        h = -f / f_dash
        c = c + h

        t = 0
        for o in odds:
            t += (1 / o) ** c
        current_error = abs(t - 1 - target_overround)

        fair_odds = list()
        for o in odds:
            fair_odds.append(round(o ** c, 3))

    return tuple(fair_odds)

print(get_no_vig_odds_multiway([2.56, 1.51]))


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
    s = re.sub(r'[^a-zA-Z0-9+\-\. ]', ' ', s)
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

    def extract_result_side(result_text):
        text = str(result_text).lower()
        if "over" in text:
            return "over"
        if "under" in text:
            return "under"
        mtch = re.search(r'([+-])\d+(?:\.\d+)?', text)
        if mtch:
            return "plus" if mtch.group(1) == "+" else "minus"
        return None

    # Return empty if all dfs are empty
    if all(df.empty for df in dfs):
        cols = ['match', 'date', 'result', 'mkt_percent', 'best_price', 'best_bookie']
        return pd.DataFrame(columns=cols), pd.DataFrame(columns=cols)

    # Pick base_df:
    # - value-aware markets (line/total): keep stable order so first active bookmaker is base
    # - other markets: longest non-empty DataFrame
    non_empty_dfs = [(df, name) for df, name in zip(dfs, bookie_names) if not df.empty]
    if not non_empty_dfs:
        cols = ['match', 'date', 'result', 'mkt_percent', 'best_price', 'best_bookie']
        return pd.DataFrame(columns=cols), pd.DataFrame(columns=cols)

    value_aware_any = any(('value' in df.columns) and df['value'].notna().any() for df, _ in non_empty_dfs)

    def coverage_score(df):
        keys = ['match']
        if 'date' in df.columns:
            keys.append('date')
        if ('value' in df.columns) and df['value'].notna().any():
            keys.append('value')
        try:
            return df[keys].drop_duplicates().shape[0]
        except Exception:
            return len(df)

    if value_aware_any:
        value_dfs = [(df, name) for df, name in non_empty_dfs if ('value' in df.columns) and df['value'].notna().any()]
        base_df, base_bookie = max(value_dfs, key=lambda x: (coverage_score(x[0]), len(x[0])))
    else:
        base_df, base_bookie = max(non_empty_dfs, key=lambda x: (coverage_score(x[0]), len(x[0])))
    base_df = base_df.copy()
    value_aware = ('value' in base_df.columns) and base_df['value'].notna().any()

    # --- Normalize base_df ---
    if names:
        base_df['match_norm'] = base_df['match'].apply(normalize_players_match)
        base_df['result_norm'] = base_df['result'].apply(normalize_players_result)
    else:
        base_df['match_norm'] = base_df['match'].apply(normalize_match)
        base_df['result_norm'] = base_df['result'].apply(normalize_result)

    if 'date' in base_df.columns:
        base_df['date'] = pd.to_datetime(base_df['date'])
    if value_aware:
        base_df['value'] = pd.to_numeric(base_df['value'], errors='coerce')
        base_df['result_side'] = base_df['result_norm'].apply(extract_result_side)

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
        other_has_value = ('value' in other_df.columns) and other_df['value'].notna().any()
        if value_aware and 'value' in other_df.columns:
            other_df['value'] = pd.to_numeric(other_df['value'], errors='coerce')
            other_df['result_side'] = other_df['result_norm'].apply(extract_result_side)

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
            if base_rows.empty or other_rows.empty:
                continue

            dedupe_cols = ['result_norm', 'result_fuzzy']
            if value_aware and 'value' in base_rows.columns and 'value' in other_rows.columns and other_has_value:
                dedupe_cols.append('value')

            for _, base_row in base_rows[dedupe_cols].dropna(subset=['result_norm', 'result_fuzzy']).drop_duplicates().iterrows():
                base_res_norm = base_row['result_norm']
                base_res_fuzzy = base_row['result_fuzzy']

                candidate_rows = other_rows.copy()
                if value_aware and other_has_value and 'value' in base_row.index and pd.notna(base_row.get('value')):
                    candidate_rows = candidate_rows[
                        candidate_rows['value'].notna() &
                        (candidate_rows['value'] - float(base_row['value'])).abs().le(0.11)
                    ]
                    if candidate_rows.empty:
                        continue

                    if 'result_side' in candidate_rows.columns and 'result_side' in base_row.index:
                        base_side = base_row.get('result_side')
                        if base_side and candidate_rows['result_side'].notna().any():
                            candidate_rows = candidate_rows[candidate_rows['result_side'] == base_side]
                            if candidate_rows.empty:
                                continue

                choices = candidate_rows['result_norm'].dropna().unique()
                if len(choices) == 0:
                    continue

                best_res = process.extractOne(base_res_norm, choices, scorer=fuzz.token_sort_ratio)
                if not best_res or best_res[1] < result_threshold:
                    continue

                mask = (
                    (other_df['match_norm'] == other_match) &
                    (other_df['date'] == base_date) &
                    (other_df['result_norm'] == best_res[0])
                )
                if value_aware and other_has_value and 'value' in base_row.index and pd.notna(base_row.get('value')):
                    mask = mask & (
                        other_df['value'].notna() &
                        (other_df['value'] - float(base_row['value'])).abs().le(0.11)
                    )
                    if 'result_side' in other_df.columns and 'result_side' in base_row.index:
                        base_side = base_row.get('result_side')
                        if base_side:
                            mask = mask & (other_df['result_side'] == base_side)
                other_df.loc[mask, 'result_fuzzy'] = base_res_fuzzy

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
        if value_aware and ('value' in base_df.columns) and ('value' in other_df.columns) and base_df['value'].notna().any() and other_df['value'].notna().any():
            merge_keys.append('value')

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
    if value_aware and 'value' in base_df.columns and base_df['value'].notna().any():
        group_cols.append('value')

    match_mkt = (
        base_df.groupby(group_cols, as_index=False)['best_prob']
        .sum()
        .rename(columns={'best_prob': 'mkt_percent'})
    )

    mkt_percents = base_df.merge(match_mkt, on=group_cols, how='left')
    mkt_percents['mkt_percent'] = (mkt_percents['mkt_percent'] * 100).round(4)
    out_cols = ['match', 'date', 'result', 'mkt_percent', 'best_price', 'best_bookie']
    if value_aware and 'value' in mkt_percents.columns:
        out_cols.insert(3, 'value')
    mkt_percents = mkt_percents[out_cols]

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

    if not webhook:
        logger.warning("ARB webhook is not set. Skipping arb alerts.")
        return
    
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

    if not webhook:
        logger.warning("Probability webhook is not set. Skipping prob alerts.")
        return

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
    return f'https://www.sportsbet.com.au/apigw/sportsbook-sports/Sportsbook/Sports/Events?primaryMarketOnly=true&fromDate={chosen_date}T00:00:00&toDate={one_month}T23:59:59&sportsId={sportId}&numEventsPerClass=2000&detailsLevel=O'

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

def _cleanup_recent_flucs(supabase, time_threshold, batch_size=1000):
    # Delete old flucs in batches. Prefer id-based deletes when id exists.
    try:
        id_probe = supabase.table("Recent Flucs").select("id").limit(1).execute()
        has_id = True
        if id_probe.data and "id" not in id_probe.data[0]:
            has_id = False

        if has_id:
            while True:
                res = (
                    supabase.table("Recent Flucs")
                    .select("id")
                    .lt("Time", time_threshold)
                    .limit(batch_size)
                    .execute()
                )
                ids = [row.get("id") for row in (res.data or []) if row.get("id") is not None]
                if not ids:
                    break
                supabase.table("Recent Flucs").delete().in_("id", ids).execute()
            return
    except Exception as e:
        msg = str(e)
        if "column Recent Flucs.id does not exist" not in msg:
            print(f"⚠️ Failed id-based cleanup of Recent Flucs: {e}")

    try:
        while True:
            res = (
                supabase.table("Recent Flucs")
                .select("Match,Date,Result,Bookie,Time")
                .lt("Time", time_threshold)
                .limit(batch_size)
                .execute()
            )
            rows = res.data or []
            if not rows:
                break

            for row in rows:
                (
                    supabase.table("Recent Flucs")
                    .delete()
                    .eq("Match", row.get("Match"))
                    .eq("Date", row.get("Date"))
                    .eq("Result", row.get("Result"))
                    .eq("Bookie", row.get("Bookie"))
                    .eq("Time", row.get("Time"))
                    .execute()
                )

            if len(rows) < batch_size:
                break
    except Exception as e2:
        try:
            supabase.table("Recent Flucs").delete().lt("Time", time_threshold).execute()
        except Exception as e3:
            print(f"⚠️ Failed fallback cleanup of Recent Flucs: {e2}")
            print(f"⚠️ Full-table cleanup also failed: {e3}")


def _derive_closing_table_name(table_name: str) -> str:
    if table_name.endswith(" Odds"):
        return f"{table_name[:-5]} Closing Odds"
    return f"{table_name} Closing"


def _store_closing_odds(
    supabase,
    current_df: pd.DataFrame,
    latest_df: pd.DataFrame,
    key_cols: list,
    price_cols: list,
    source_table_name: str,
    closing_table_name: Optional[str] = None,
):
    if current_df.empty:
        return

    if latest_df.empty:
        logger.warning(
            f"Skipping closing capture for {source_table_name}: latest snapshot is empty, "
            "which is too ambiguous to treat as a true market close."
        )
        return

    if not all(col in current_df.columns for col in key_cols):
        logger.warning(f"Skipping closing capture for {source_table_name}: current snapshot missing key columns.")
        return

    if not all(col in latest_df.columns for col in key_cols):
        logger.warning(f"Skipping closing capture for {source_table_name}: latest snapshot missing key columns.")
        return

    current_keys = current_df[key_cols].drop_duplicates()
    latest_keys = latest_df[key_cols].drop_duplicates()
    missing_keys = (
        current_keys
        .merge(latest_keys, on=key_cols, how="left", indicator=True)
        .loc[lambda df: df["_merge"] == "left_only", key_cols]
    )
    if missing_keys.empty:
        return

    base_cols = [col for col in latest_df.columns if col in current_df.columns]
    if not base_cols:
        logger.warning(f"Skipping closing capture for {source_table_name}: no shared columns between snapshots.")
        return

    closing_rows = current_df[base_cols].merge(missing_keys, on=key_cols, how="inner")
    valid_price_cols = [col for col in price_cols if col in closing_rows.columns]
    if valid_price_cols:
        closing_rows = closing_rows.loc[
            closing_rows[valid_price_cols].apply(
                lambda row: any(pd.notna(v) and float(v) > 0 for v in row),
                axis=1
            )
        ]
    if closing_rows.empty:
        return

    now_str = datetime.now(pytz.timezone("Australia/Brisbane")).strftime("%Y-%m-%d %H:%M:%S")
    closing_rows = closing_rows.copy()
    closing_rows["Closed Time"] = now_str
    closing_rows["Source Table"] = source_table_name
    closing_rows = make_json_safe(closing_rows)
    records = closing_rows.to_dict(orient="records")
    closing_table = closing_table_name or _derive_closing_table_name(source_table_name)

    try:
        for rec in records:
            delete_query = supabase.table(closing_table).delete()
            for key in key_cols:
                delete_query = delete_query.eq(key, rec.get(key))
            delete_query.execute()
    except Exception as e:
        logger.warning(f"Skipping closing capture for {source_table_name}: failed to prepare {closing_table}: {e}")
        return

    try:
        supabase.table(closing_table).insert(records).execute()
        logger.info(f"Inserted {len(records)} closing rows into {closing_table}.")
    except Exception as e:
        msg = str(e)
        optional_cols = {"Closed Time", "Source Table"}
        if any(f"Could not find the '{col}' column" in msg for col in optional_cols):
            fallback_records = [{k: v for k, v in rec.items() if k not in optional_cols} for rec in records]
            try:
                supabase.table(closing_table).insert(fallback_records).execute()
                logger.info(
                    f"Inserted {len(fallback_records)} closing rows into {closing_table} "
                    "without optional metadata columns."
                )
            except Exception as retry_e:
                logger.warning(f"Failed to insert closing rows into {closing_table}: {retry_e}")
        else:
            logger.warning(f"Failed to insert closing rows into {closing_table}: {e}")


def process_odds(
    bookmakers: dict,
    price_cols: list,
    table_name: str,
    match_threshold: int = 90,
    outcomes: int = 2,
    names=False,
    market=None,
    include_value=False,
    min_mkt_percent=80,
    upsert=False,
    upsert_keys=None,
    store_closing_odds=False,
    closing_table_name=None,
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
    
    def extract_value_from_result(result):
        mtch = re.search(r'[-+]?\d+(?:\.\d+)?', str(result))
        if not mtch:
            return None
        value = float(mtch.group(0))
        if market and str(market).lower() == "line":
            return abs(value)
        return value

    def extract_signed_value_from_result(result):
        mtch = re.search(r'[-+]?\d+(?:\.\d+)?', str(result))
        if not mtch:
            return None
        value = float(mtch.group(0))
        return value

    def strip_value_from_result(result):
        txt = str(result)
        return re.sub(r'\s*[-+]?\d+(?:\.\d+)?\+?\s*$', '', txt).strip()

    # Convert each bookmaker's markets into a DataFrame
    for name, markets in bookmakers.items():
        rows = []
        for (match, date), odds in markets.items():
            if len(odds) >= outcomes:
                for result, price in odds.items():
                    row = {
                        "match": match,
                        "date": date,
                        "result": result,
                        f"{name}": price
                    }
                    if include_value:
                        row["value"] = extract_value_from_result(result)
                        if market and str(market).lower() == "line":
                            row["value_signed"] = extract_signed_value_from_result(result)
                    rows.append(row)
        df = pd.DataFrame(rows)

        if df.empty:
            logger.warning(f"Skipping {name} entirely — no valid markets found.")
            continue

        dfs[name] = df

    valid_price_cols = [name for name in price_cols if name in dfs and not dfs[name].empty]
    if not valid_price_cols:
        logger.error("❌ No valid bookmakers available after filtering. Exiting early.")
        return None, None

    active_price_cols = valid_price_cols
    dfs_list = [dfs[name] for name in active_price_cols]
    logger.info(f"Including {len(active_price_cols)} valid bookmakers: {active_price_cols}")

    logger.info(f"Merging {table_name} dfs")
    merged_df, mkt_percents = fuzzy_merge_prices(
        dfs_list,
        active_price_cols,
        match_threshold=match_threshold,
        outcomes=outcomes,
        names=names,
    )

    # Attach market %
    odds_keys = ["match", "date", "result"]
    if include_value and "value" in merged_df.columns and "value" in mkt_percents.columns and merged_df["value"].notna().any() and mkt_percents["value"].notna().any():
        odds_keys.append("value")
    merged_df = pd.merge(
        merged_df,
        mkt_percents[odds_keys + ["mkt_percent"]],
        on=odds_keys
    )
    merged_df = merged_df[merged_df["mkt_percent"] > min_mkt_percent]

    # Log coverage per bookmaker
    for name in active_price_cols:
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
    col_map.update({name: name for name in active_price_cols})
    if market is not None:
        col_map["market"] = "Market"
    if include_value:
        if market and str(market).lower() == "line":
            col_map["value_signed"] = "Value"
        else:
            col_map["value"] = "Value"

    cleaned_df = merged_df.fillna(0.0)
    df_mapped = cleaned_df.rename(columns=col_map)
    df_mapped = df_mapped[[col for col in df_mapped.columns if col in col_map.values()]]
    if market is not None:
        df_mapped["Market"] = market
    if include_value:
        if "Value" not in df_mapped.columns:
            if market and str(market).lower() == "line":
                df_mapped["Value"] = df_mapped["Result"].apply(extract_signed_value_from_result)
            else:
                df_mapped["Value"] = df_mapped["Result"].apply(extract_value_from_result)
        if market and str(market).lower() in {"line", "total", "tryscorer"}:
            df_mapped["Result"] = df_mapped["Result"].apply(strip_value_from_result)
    df_mapped["Best Bookie"] = df_mapped["Best Bookie"].apply(
        lambda x: ", ".join(x) if isinstance(x, list) else str(x)
    )
    df_mapped["Market %"] = df_mapped["Market %"].round(4)

    # Fetch current table for fluc comparison
    current_table = supabase.table(table_name).select("*").execute()
    current_df = pd.DataFrame(current_table.data)

    required_current_cols = ['Match', 'Date', 'Result'] + active_price_cols
    if include_value:
        required_current_cols.append("Value")
    for col in required_current_cols:
        if col not in current_df.columns:
            current_df[col] = None
            
    dedupe_keys = ["Match", "Date", "Result"]
    if upsert_keys:
        dedupe_keys = [k for k in upsert_keys if k in df_mapped.columns]
    elif include_value and "Value" in df_mapped.columns:
        dedupe_keys.append("Value")

    dupes = df_mapped[df_mapped.duplicated(subset=dedupe_keys, keep=False)]
    if not dupes.empty:
        logger.warning(f"⚠️ Found {len(dupes)} duplicate rows before insert.")
        print(dupes[dedupe_keys])
        
    df_mapped["Date"] = pd.to_datetime(df_mapped["Date"]).dt.strftime("%Y-%m-%d")

    if dedupe_keys:
        # Prefer rows with broader bookmaker coverage, then stronger market quality.
        df_mapped["_coverage"] = df_mapped[active_price_cols].apply(
            lambda row: sum((pd.notna(v) and float(v) > 0) for v in row),
            axis=1
        )
        sort_cols = dedupe_keys + ["_coverage", "Market %"]
        df_mapped = (
            df_mapped.sort_values(sort_cols, ascending=[True] * len(dedupe_keys) + [False, False])
            .drop_duplicates(subset=dedupe_keys, keep="first")
            .drop(columns=["_coverage"])
            .reset_index(drop=True)
        )

    # Convert to dict records for table insert
    df_mapped = make_json_safe(df_mapped)
    records = df_mapped.to_dict(orient="records")

    if store_closing_odds:
        _store_closing_odds(
            supabase=supabase,
            current_df=current_df,
            latest_df=df_mapped,
            key_cols=dedupe_keys,
            price_cols=active_price_cols,
            source_table_name=table_name,
            closing_table_name=closing_table_name,
        )

    # --- Price Fluctuation Analysis ---
    fluc_keys = ['Match', 'Date', 'Result']
    if upsert_keys:
        fluc_keys = [k for k in upsert_keys if k in df_mapped.columns and k in current_df.columns]
    elif include_value and "Value" in df_mapped.columns:
        fluc_keys.append("Value")

    flucs = pd.merge(
        df_mapped[fluc_keys + active_price_cols],
        current_df[fluc_keys + active_price_cols],
        on=fluc_keys,
        suffixes=('_new', '_old'),
        how='outer'
    )

    new_prices = flucs.melt(
        id_vars=fluc_keys,
        value_vars=[f"{c}_new" for c in active_price_cols],
        var_name='Bookie',
        value_name='New Price'
    )
    new_prices['Bookie'] = new_prices['Bookie'].str.replace('_new', '')

    old_prices = flucs.melt(
        id_vars=fluc_keys,
        value_vars=[f"{c}_old" for c in active_price_cols],
        var_name='Bookie',
        value_name='Old Price'
    )
    old_prices['Bookie'] = old_prices['Bookie'].str.replace('_old', '')

    # Combine back into long tidy DataFrame
    flucs_long = pd.merge(
        new_prices,
        old_prices,
        on=fluc_keys + ['Bookie']
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
            msg = str(e)
            if "Could not find the 'Value' column of 'Recent Flucs'" in msg:
                try:
                    records_flucs_no_value = [{k: v for k, v in rec.items() if k != "Value"} for rec in records_flucs]
                    supabase.table("Recent Flucs").insert(records_flucs_no_value).execute()
                    print(f"✅ Inserted {len(records_flucs_no_value)} records into Recent Flucs (without Value column).")
                except Exception as retry_e:
                    print("❌ Failed to insert records into Supabase:", retry_e)
            else:
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
    _cleanup_recent_flucs(supabase, time_threshold)

    # Refresh or upsert main odds table
    if upsert:
        conflict_cols = upsert_keys or ["Match", "Date", "Result"]
        conflict_cols = [c for c in conflict_cols if c in df_mapped.columns]
        if not conflict_cols:
            logger.error(f"❌ No valid upsert keys found for {table_name}.")
        else:
            logger.info(f"Upserting {table_name} records on {conflict_cols}...")
            if records:
                try:
                    supabase.table(table_name).upsert(
                        records,
                        on_conflict=",".join(conflict_cols)
                    ).execute()
                except Exception as e:
                    print(f"❌ Failed upsert into {table_name}: {e}")
            else:
                print("⚠️ No records to upsert. Skipping Supabase upsert.")
    else:
        logger.info(f"Clearing {table_name} table before insert...")
        supabase.table(table_name).delete().neq("Match", "").execute()

        logger.info(f"Inserting fresh {table_name} records...")
        if records:
            try:
                supabase.table(table_name).insert(records).execute()
            except Exception as e:
                msg = str(e)
                if include_value and "duplicate key value violates unique constraint" in msg:
                    logger.warning(
                        f"{table_name} unique key does not currently allow multiple values per team. "
                        "Falling back to one row per Match/Date/Result."
                    )
                    fallback_df = df_mapped.copy()
                    if "Market %" in fallback_df.columns:
                        fallback_df = fallback_df.sort_values(
                            ["Match", "Date", "Result", "Market %"],
                            ascending=[True, True, True, False]
                        )
                    fallback_df = fallback_df.drop_duplicates(
                        subset=["Match", "Date", "Result"],
                        keep="first"
                    ).reset_index(drop=True)
                    fallback_records = fallback_df.to_dict(orient="records")
                    if fallback_records:
                        supabase.table(table_name).insert(fallback_records).execute()
                    else:
                        print("⚠️ No fallback records to insert.")
                else:
                    raise
        else:
            print("⚠️ No records to insert. Skipping Supabase insert.")
    
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

def process_line_total_wide(
    bookmakers: dict,
    price_cols: list,
    table_name: str,
    market_kind: str,
    match_threshold: int = 80,
    upsert: bool = False,
    upsert_keys=None,
    store_closing_odds: bool = False,
    closing_table_name: Optional[str] = None,
):
    """
    Process line/total odds into wide format keyed by (Match, Date, Result).
    Each bookie gets {bookie}_odds and {bookie}_line columns.
    Result is the team name (line) or Over/Under (total), not including the value.
    """
    from rapidfuzz import fuzz, process as fuzz_process

    is_line = market_kind.lower() == "line"
    required_sides = {"plus", "minus"} if is_line else {"over", "under"}

    def _strip_value(text):
        return re.sub(r'\s*[-+]?\d+(?:\.\d+)?\s*$', '', str(text).strip()).strip() or str(text).strip()

    def _get_side(text):
        s = str(text).lower()
        if is_line:
            m = re.search(r'[-+]?\d+(?:\.\d+)?', s)
            if m:
                v = float(m.group(0))
                if v > 0:
                    return "plus"
                if v < 0:
                    return "minus"
            return None
        if "over" in s:
            return "over"
        if "under" in s:
            return "under"
        return None

    def _get_value(text):
        m = re.search(r'([-+]?\d+(?:\.\d+)?)', str(text))
        return float(m.group(1)) if m else None

    def _pair_balance_score(side_prices):
        prices = []
        for side in required_sides:
            price = side_prices.get(side)
            if price is None or price <= 0:
                return (float("inf"), float("inf"))
            prices.append(float(price))
        return (abs(math.log(max(prices) / min(prices))), abs(sum(1 / p for p in prices) - 1.05))

    # Build per-bookie DataFrames
    bookie_dfs = {}
    for name, markets in bookmakers.items():
        rows = []
        skipped = 0
        for (match, date), odds in markets.items():
            by_value = defaultdict(list)
            for result, price in odds.items():
                side = _get_side(result)
                value = _get_value(result)
                stripped = _strip_value(result)
                if side is None or value is None:
                    logger.debug(f"{name} | {match} | skipping result={result!r} side={side} value={value}")
                    continue
                try:
                    price_val = float(price)
                except (TypeError, ValueError):
                    logger.debug(f"{name} | {match} | skipping result={result!r} invalid price={price!r}")
                    continue
                by_value[round(abs(float(value)), 1)].append({
                    "stripped": stripped,
                    "side": side,
                    "price": price_val,
                    "value": value,
                })

            complete_values = []
            for value_key, items in by_value.items():
                side_prices = {}
                for item in items:
                    if item["side"] not in side_prices or item["price"] > side_prices[item["side"]]:
                        side_prices[item["side"]] = item["price"]
                if required_sides.issubset(side_prices):
                    complete_values.append((value_key, side_prices))

            if not complete_values:
                skipped += 1
                sides_seen = {item["side"] for items in by_value.values() for item in items}
                logger.debug(f"{name} | {match} | missing sides: have={sides_seen} need={required_sides}")
                continue

            selected_value, _ = min(
                complete_values,
                key=lambda pair: (
                    _pair_balance_score(pair[1]),
                    pair[0],
                ),
            )

            best = {}  # (result_stripped, side) -> (odds_val, line_val)
            for item in by_value[selected_value]:
                key = (item["stripped"], item["side"])
                if key not in best or item["price"] > best[key][0]:
                    best[key] = (item["price"], item["value"])

            sides_seen = {k[1] for k in best}
            if not required_sides.issubset(sides_seen):
                skipped += 1
                logger.debug(f"{name} | {match} | missing sides: have={sides_seen} need={required_sides} keys={list(best.keys())}")
                continue

            for (stripped, side), (price, value) in best.items():
                rows.append({
                    "match": match,
                    "date": date,
                    "result": stripped,
                    "side": side,
                    f"{name}_odds": price,
                    f"{name}_line": value,
                })

        if rows:
            df = pd.DataFrame(rows)
            df["date"] = pd.to_datetime(df["date"])
            df["match_norm"] = df["match"].apply(normalize_match)
            df["result_norm"] = df["result"].apply(normalize_result)
            bookie_dfs[name] = df
            logger.info(f"{name}: {len(rows)} rows parsed, {skipped} games skipped (missing sides)")
        else:
            logger.warning(f"Skipping {name} — no valid {market_kind} markets. {skipped} games had missing sides.")

    valid_bookies = [n for n in price_cols if n in bookie_dfs]
    if not valid_bookies:
        logger.error(f"No valid bookmakers for {table_name}. Exiting.")
        return

    base_name = max(valid_bookies, key=lambda n: bookie_dfs[n]["match"].nunique())
    base_df = bookie_dfs[base_name].copy()
    base_df["match_key"] = base_df["match_norm"]
    base_df["result_key"] = base_df["result_norm"]

    odds_cols = [f"{base_name}_odds", f"{base_name}_line"]

    for name in valid_bookies:
        if name == base_name:
            continue
        other = bookie_dfs[name].copy()
        other["match_key"] = None
        other["result_key"] = None

        other_matches = other[["match_norm", "date"]].drop_duplicates()
        match_map = {}
        for _, br in base_df[["match_key", "date"]].drop_duplicates().iterrows():
            same_date = other_matches[other_matches["date"] == br["date"]]
            if same_date.empty:
                continue
            best = fuzz_process.extractOne(
                br["match_key"], same_date["match_norm"].tolist(), scorer=fuzz.token_sort_ratio
            )
            if best and best[1] >= match_threshold:
                match_map[(br["match_key"], br["date"])] = best[0]

        for (base_match, base_date), other_match in match_map.items():
            mask = (other["match_norm"] == other_match) & (other["date"] == base_date)
            other.loc[mask, "match_key"] = base_match

            if is_line:
                base_results = base_df[
                    (base_df["match_key"] == base_match) & (base_df["date"] == base_date)
                ][["result_key", "side"]].drop_duplicates()
                other_in_game = other[mask]

                for _, brow in base_results.iterrows():
                    same_side = other_in_game[other_in_game["side"] == brow["side"]]["result_norm"].tolist()
                    if not same_side:
                        continue
                    best_r = fuzz_process.extractOne(brow["result_key"], same_side, scorer=fuzz.token_sort_ratio)
                    if best_r and best_r[1] >= 70:
                        omask = mask & (other["result_norm"] == best_r[0]) & (other["side"] == brow["side"])
                        other.loc[omask, "result_key"] = brow["result_key"]
            else:
                other.loc[mask, "result_key"] = other.loc[mask, "side"]

        merge_on = ["match_key", "date", "result_key"]
        merge_cols = merge_on + [f"{name}_odds", f"{name}_line"]
        other_merge = (
            other[other["match_key"].notna() & other["result_key"].notna()][merge_cols]
            .drop_duplicates(subset=merge_on)
        )
        base_df = base_df.merge(other_merge, on=merge_on, how="left")
        odds_cols += [f"{name}_odds", f"{name}_line"]

    # Build output DataFrame
    keep = ["match", "date", "result"] + [c for c in odds_cols if c in base_df.columns]
    df_out = base_df[keep].copy().rename(columns={"match": "Match", "date": "Date", "result": "Result"})
    df_out["Market"] = market_kind.capitalize()
    df_out["Date"] = pd.to_datetime(df_out["Date"]).dt.strftime("%Y-%m-%d")

    odds_only = [c for c in df_out.columns if c.endswith("_odds")]
    df_out = df_out[
        df_out[odds_only].apply(lambda r: any(pd.notna(v) and float(v) > 0 for v in r), axis=1)
    ]
    df_out = df_out.drop_duplicates(subset=["Match", "Date", "Result"]).reset_index(drop=True)
    logger.info(f"{table_name}: {len(df_out)} rows after merge")

    dedupe_keys = [k for k in (upsert_keys or ["Match", "Date", "Result"]) if k in df_out.columns]

    current_table = supabase.table(table_name).select("*").execute()
    current_df = pd.DataFrame(current_table.data)

    if store_closing_odds:
        _store_closing_odds(
            supabase=supabase,
            current_df=current_df,
            latest_df=df_out,
            key_cols=dedupe_keys,
            price_cols=odds_only,
            source_table_name=table_name,
            closing_table_name=closing_table_name,
        )

    # Fluctuation tracking on _odds columns
    shared_odds = [c for c in odds_only if c in (current_df.columns if not current_df.empty else [])]
    if shared_odds and not current_df.empty:
        flucs = pd.merge(
            df_out[dedupe_keys + shared_odds],
            current_df[dedupe_keys + shared_odds],
            on=dedupe_keys,
            suffixes=('_new', '_old'),
            how='outer',
        )
        new_prices = flucs.melt(id_vars=dedupe_keys, value_vars=[f"{c}_new" for c in shared_odds], var_name='Bookie', value_name='New Price')
        new_prices['Bookie'] = new_prices['Bookie'].str.replace('_odds_new', '')
        old_prices = flucs.melt(id_vars=dedupe_keys, value_vars=[f"{c}_old" for c in shared_odds], var_name='Bookie', value_name='Old Price')
        old_prices['Bookie'] = old_prices['Bookie'].str.replace('_odds_old', '')
        flucs_long = pd.merge(new_prices, old_prices, on=dedupe_keys + ['Bookie'])
        flucs_long['Time'] = datetime.now(pytz.timezone("Australia/Brisbane")).strftime('%Y-%m-%d %H:%M:%S')
        flucs_long['Prob Change'] = None
        mask = (flucs_long['New Price'] > 0) & (flucs_long['Old Price'] > 0)
        flucs_long.loc[mask, 'Prob Change'] = (
            1 / flucs_long.loc[mask, 'New Price'] - 1 / flucs_long.loc[mask, 'Old Price']
        )
        flucs_long = flucs_long.replace([np.nan, np.inf, -np.inf], None)
        flucs_long['Sport'] = table_name.replace(" Odds", "")
        flucs_long = make_json_safe(flucs_long)
        records_flucs = flucs_long.to_dict(orient="records")
        if records_flucs:
            try:
                supabase.table("Recent Flucs").insert(records_flucs).execute()
                logger.info(f"Inserted {len(records_flucs)} fluc records.")
            except Exception as e:
                logger.warning(f"Failed to insert flucs: {e}")

        time_threshold = (datetime.now(pytz.timezone("Australia/Brisbane")) - timedelta(hours=3)).isoformat()
        _cleanup_recent_flucs(supabase, time_threshold)

    df_out = make_json_safe(df_out)
    df_out = df_out.replace([np.nan, np.inf, -np.inf], None)
    records = [{k: (None if isinstance(v, float) and (v != v) else v) for k, v in r.items()} for r in df_out.to_dict(orient="records")]

    if upsert:
        if records:
            try:
                supabase.table(table_name).upsert(records, on_conflict=",".join(dedupe_keys)).execute()
                logger.info(f"✅ Upserted {len(records)} records into {table_name}.")
            except Exception as e:
                logger.error(f"❌ Failed upsert into {table_name}: {e}")
    else:
        supabase.table(table_name).delete().neq("Match", "").execute()
        if records:
            try:
                supabase.table(table_name).insert(records).execute()
                logger.info(f"✅ Inserted {len(records)} records into {table_name}.")
            except Exception as e:
                logger.error(f"❌ Failed insert into {table_name}: {e}")

    return df_out


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
    
    
#Polymarket functions

def _get(url: str, params: dict | None = None, retries: int = 3, backoff: float = 0.8):
    """Generic GET with retry."""
    for i in range(retries):
        r = SESSION.get(url, params=params, timeout=20)
        if r.status_code == 200:
            return r.json()
        if r.status_code in (429, 500, 502, 503, 504) and i < retries - 1:
            time.sleep(backoff * (i + 1))
            continue
        raise RuntimeError(f"GET {url} failed [{r.status_code}]: {r.text[:200]}")


def iter_markets(tag_id: Optional[int] = None, limit: int = 100, types: Optional[List[str]] = None) -> Iterable[dict]:
    """Page through /markets (active only)."""
    offset = 0
    while True:
        params = {
            "closed": "false",
            "limit": limit,
            "offset": offset,
            "order": "id",
            "ascending": "false",
        }
        if tag_id is not None:
            params["tag_id"] = tag_id
        if types:
            params["sports_market_types"] = types

        data = _get(f"{BASE_URL}/markets", params=params)
        if not isinstance(data, list) or not data:
            break

        for m in data:
            if str(m.get("category", "")).lower().startswith("sports") or m.get("sportsMarketType"):
                yield m

        if len(data) < limit:
            break
        offset += limit


def parse_event_date(m: dict, tz: str) -> Optional[str]:
    """Convert event start to local date."""
    z = ZoneInfo(tz)
    for key in ["gameStartTime", "eventStartTime", "startDateIso", "startDate"]:
        val = m.get(key)
        if val:
            try:
                dt_utc = datetime.fromisoformat(val.replace("Z", "+00:00"))
                return dt_utc.astimezone(z).date().isoformat()
            except Exception:
                continue
    return None


def get_orderbook_summary(token_id: str) -> tuple[Optional[float], Optional[float]]:
    """
    Fetch tradable buy/sell prices for a YES token.
    On Polymarket, 'asks' are sell offers (usually high),
    'bids' are buy offers (usually low).
    The UI 'YES price' = highest bid.
    """
    try:
        url = f"{CLOB_URL}/book"
        params = {"token_id": token_id}
        data = _get(url, params=params)
        asks = data.get("asks", [])
        bids = data.get("bids", [])
        # reversed: we want to show "ask" = what you can BUY at (highest bid)
        lowest_ask = float(asks[-1]["price"]) if asks else None   # ✅ use last entry
        highest_bid = float(bids[-1]["price"]) if bids else None   # ✅ use first entry

        return lowest_ask, highest_bid
    except Exception:
        return None, None

def extract_outcome_names(m: dict) -> list[str]:
    """Return clean outcome names (handles stringified lists like '["Yes","No"]')."""
    raw = m.get("outcomes")

    # Case 1: list already parsed
    if isinstance(raw, list):
        return [o["name"] if isinstance(o, dict) and "name" in o else str(o) for o in raw]

    # Case 2: JSON string like '["Yes","No"]' or '[{"name": "A"},{"name": "B"}]'
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                return [p["name"] if isinstance(p, dict) and "name" in p else str(p) for p in parsed]
        except Exception:
            pass

    # Fallback
    return []

def build_df(
    sport: str,
    tz: str = "Australia/Brisbane",
    min_liq: Optional[float] = None,
    types: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Return DataFrame of markets with live orderbook prices."""
    rows: List[dict] = []
    seen: set[tuple] = set()

    try:
        tag = _get(f"{BASE_URL}/tags/slug/{sport}")
    except Exception:
        print(f"⚠️ Could not find tag for {sport}")
        return pd.DataFrame()

    for m in iter_markets(tag_id=tag.get("id"), types=types):
        liq = float(m.get("liquidityNum") or 0)
        if min_liq and liq < min_liq:
            continue

        match = m.get("question") or m.get("title") or m.get("slug") or ""
        date_str = parse_event_date(m, tz)

        # Extract outcomes + token IDs
        outcomes = extract_outcome_names(m)
        # Get token IDs robustly (different field names possible)
        token_ids = (
            m.get("clobTokenIds")
            or m.get("outcomeTokenIds")
            or [t.get("token_id") for t in m.get("tokens", []) if isinstance(t, dict)]
            or []
        )
        
        if isinstance(token_ids, str):
            try:
                token_ids = json.loads(token_ids)
            except Exception:
                token_ids = []

        for idx, outcome in enumerate(outcomes):
            name = outcome.get("name") if isinstance(outcome, dict) else str(outcome)
            token_id = str(token_ids[idx]) if idx < len(token_ids) else None
            if not token_id:
                continue

            # Fetch real orderbook prices
            ask, bid = get_orderbook_summary(token_id)
            key = (m.get("id"), token_id)
            if key in seen:
                continue
            seen.add(key)

            rows.append({
                "sport": sport,
                "date": date_str,
                "match": match,
                "team": name,
                "ask": ask,
                "bid": bid
            })

        time.sleep(0.25)  # avoid rate limiting

    return pd.DataFrame(rows, columns=["sport", "date", "match", "team", "ask", "bid"])

def rebuild_match_name(group):
    teams = sorted(group["team"].unique())  # ensure consistent order
    if len(teams) >= 2:
        return f"{teams[0]} vs {teams[1]}"
    elif len(teams) == 1:
        return teams[0]
    else:
        return None
        
# Sports mapping
polymarket_sport_map = {
    "nba": "Basketball",
    "wnba": "Basketball",
    "ncaab": "Basketball",
    "cbb": "Basketball",
    "lal": "Basketball",
    "epl": "Football",
    "mls": "Football",
    "ucl": "Football",
    "uel": "Football",
    "fl1": "Football",
    "bun": "Football",
}

NBA_TEAM_MAP = {
    "Hawks": "Atlanta Hawks",
    "Celtics": "Boston Celtics",
    "Nets": "Brooklyn Nets",
    "Hornets": "Charlotte Hornets",
    "Bulls": "Chicago Bulls",
    "Cavaliers": "Cleveland Cavaliers",
    "Mavericks": "Dallas Mavericks",
    "Nuggets": "Denver Nuggets",
    "Pistons": "Detroit Pistons",
    "Warriors": "Golden State Warriors",
    "Rockets": "Houston Rockets",
    "Pacers": "Indiana Pacers",
    "Clippers": "Los Angeles Clippers",
    "Lakers": "Los Angeles Lakers",
    "Grizzlies": "Memphis Grizzlies",
    "Heat": "Miami Heat",
    "Bucks": "Milwaukee Bucks",
    "Timberwolves": "Minnesota Timberwolves",
    "Pelicans": "New Orleans Pelicans",
    "Knicks": "New York Knicks",
    "Thunder": "Oklahoma City Thunder",
    "Magic": "Orlando Magic",
    "76ers": "Philadelphia 76ers",
    "Sixers": "Philadelphia 76ers",
    "Suns": "Phoenix Suns",
    "Blazers": "Portland Trail Blazers",
    "Trail Blazers": "Portland Trail Blazers",
    "Kings": "Sacramento Kings",
    "Spurs": "San Antonio Spurs",
    "Raptors": "Toronto Raptors",
    "Jazz": "Utah Jazz",
    "Wizards": "Washington Wizards",
}
