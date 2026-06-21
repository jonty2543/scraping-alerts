import argparse
import json
import logging
import math
import os
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path

import pandas as pd
import requests


SOURCE_URL = "https://www.aussportsbetting.com/historical_data/nrl.xlsx"
TABLE_NAME = "nrl_historical_results_odds"
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

COLUMN_MAP = {
    "Date": "date",
    "Kick-off (local)": "kickoff_local",
    "Home Team": "home_team",
    "Away Team": "away_team",
    "Venue": "venue",
    "Home Score": "home_score",
    "Away Score": "away_score",
    "Play Off Game?": "play_off_game",
    "Over Time?": "over_time",
    "Home Odds": "home_odds",
    "Draw Odds": "draw_odds",
    "Away Odds": "away_odds",
    "Bookmakers Surveyed": "bookmakers_surveyed",
    "Home Odds Open": "home_odds_open",
    "Home Odds Min": "home_odds_min",
    "Home Odds Max": "home_odds_max",
    "Home Odds Close": "home_odds_close",
    "Away Odds Open": "away_odds_open",
    "Away Odds Min": "away_odds_min",
    "Away Odds Max": "away_odds_max",
    "Away Odds Close": "away_odds_close",
    "Home Line Open": "home_line_open",
    "Home Line Min": "home_line_min",
    "Home Line Max": "home_line_max",
    "Home Line Close": "home_line_close",
    "Away Line Open": "away_line_open",
    "Away Line Min": "away_line_min",
    "Away Line Max": "away_line_max",
    "Away Line Close": "away_line_close",
    "Home Line Odds Open": "home_line_odds_open",
    "Home Line Odds Min": "home_line_odds_min",
    "Home Line Odds Max": "home_line_odds_max",
    "Home Line Odds Close": "home_line_odds_close",
    "Away Line Odds Open": "away_line_odds_open",
    "Away Line Odds Min": "away_line_odds_min",
    "Away Line Odds Max": "away_line_odds_max",
    "Away Line Odds Close": "away_line_odds_close",
    "Total Score Open": "total_score_open",
    "Total Score Min": "total_score_min",
    "Total Score Max": "total_score_max",
    "Total Score Close": "total_score_close",
    "Total Score Over Open": "total_score_over_open",
    "Total Score Over Min": "total_score_over_min",
    "Total Score Over Max": "total_score_over_max",
    "Total Score Over Close": "total_score_over_close",
    "Total Score Under Open": "total_score_under_open",
    "Total Score Under Min": "total_score_under_min",
    "Total Score Under Max": "total_score_under_max",
    "Total Score Under Close": "total_score_under_close",
    "Notes": "notes",
}

INTEGER_COLUMNS = {"home_score", "away_score", "bookmakers_surveyed"}
BOOLEAN_COLUMNS = {"play_off_game", "over_time"}


def load_local_env(path=".env"):
    env_path = Path(path)
    if not env_path.exists():
        return

    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("'").strip('"'))


def get_workbook_bytes(source):
    source_path = Path(source)
    if source_path.exists():
        logger.info("Reading workbook from %s", source_path)
        return source_path.read_bytes(), str(source_path)

    logger.info("Downloading workbook from %s", source)
    response = requests.get(source, timeout=60)
    response.raise_for_status()
    return response.content, source


def parse_flag(value):
    if pd.isna(value) or value == "":
        return None
    return str(value).strip().lower() in {"y", "yes", "true", "1"}


def parse_time(value):
    if pd.isna(value) or value == "":
        return None
    if hasattr(value, "strftime"):
        return value.strftime("%H:%M:%S")
    parsed = pd.to_datetime(str(value), format="%H:%M:%S", errors="coerce")
    if pd.isna(parsed):
        parsed = pd.to_datetime(str(value), format="%H:%M", errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.strftime("%H:%M:%S")


def json_safe_value(value):
    if value is None:
        return None

    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass

    if hasattr(value, "item"):
        return json_safe_value(value.item())

    if isinstance(value, float) and not math.isfinite(value):
        return None

    return value


def json_safe_records(records):
    return [
        {key: json_safe_value(value) for key, value in record.items()}
        for record in records
    ]


def parse_workbook(workbook_bytes, source_url):
    raw = pd.read_excel(BytesIO(workbook_bytes), sheet_name="Data", header=None)
    headers = raw.iloc[1].tolist()
    df = raw.iloc[2:].copy()
    df.columns = headers
    df = df.dropna(subset=["Date", "Home Team", "Away Team"], how="any")
    df = df.rename(columns=COLUMN_MAP)

    missing = set(COLUMN_MAP.values()) - set(df.columns)
    if missing:
        raise ValueError(f"Workbook is missing expected columns: {sorted(missing)}")

    df = df[list(COLUMN_MAP.values())]
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    df["kickoff_local"] = df["kickoff_local"].apply(parse_time)
    df["match"] = df["home_team"].astype(str).str.strip() + " v " + df["away_team"].astype(str).str.strip()

    for col in INTEGER_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in BOOLEAN_COLUMNS:
        df[col] = df[col].apply(parse_flag)

    numeric_cols = [
        col
        for col in df.columns
        if col not in {"date", "kickoff_local", "match", "home_team", "away_team", "venue", "notes"}
        and col not in BOOLEAN_COLUMNS
        and col not in INTEGER_COLUMNS
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    downloaded_at = datetime.now(timezone.utc).isoformat()
    df["source_url"] = source_url
    df["source_downloaded_at"] = downloaded_at

    return json_safe_records(df.to_dict(orient="records"))


def upsert_records(records, batch_size):
    from supabase import create_client

    supabase_url = os.getenv("SUPABASE_URL", "")
    supabase_key = os.getenv("SUPABASE_KEY", "")
    if not supabase_url or not supabase_key:
        raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY. Add them to .env for local runs.")

    client = create_client(supabase_url, supabase_key)
    for start in range(0, len(records), batch_size):
        batch = records[start:start + batch_size]
        client.table(TABLE_NAME).upsert(
            batch,
            on_conflict="date,home_team,away_team",
        ).execute()
        logger.info("Upserted %s/%s rows", start + len(batch), len(records))


def main():
    parser = argparse.ArgumentParser(
        description="Import Aussportsbetting historical NRL results and odds into Supabase."
    )
    parser.add_argument("--source", default=SOURCE_URL, help="Workbook URL or local .xlsx path.")
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--dry-run", action="store_true", help="Parse and summarize without writing to Supabase.")
    args = parser.parse_args()

    load_local_env()
    workbook_bytes, source_url = get_workbook_bytes(args.source)
    records = parse_workbook(workbook_bytes, source_url)

    if not records:
        raise RuntimeError("No records parsed from workbook.")
    json.dumps(records, allow_nan=False)

    logger.info("Parsed %s rows from %s to %s", len(records), records[-1]["date"], records[0]["date"])
    logger.info("Latest fixture in source: %s on %s", records[0]["match"], records[0]["date"])

    if args.dry_run:
        logger.info("Dry run complete; no rows written.")
        return

    upsert_records(records, args.batch_size)


if __name__ == "__main__":
    main()
