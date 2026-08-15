import argparse
import os
from datetime import datetime
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from supabase import create_client


SOURCE_TABLE = "NRL Tryscorers"
DESTINATION_TABLE = "NRL Closing Odds"
MARKET = "Tryscorer"
PAGE_SIZE = 1000
INSERT_BATCH_SIZE = 250
BOOKMAKERS = ["Sportsbet", "Pointsbet", "Unibet", "Palmerbet", "Betright"]


def fetch_all(query):
    rows = []
    start = 0
    while True:
        page = query.range(start, start + PAGE_SIZE - 1).execute().data or []
        rows.extend(page)
        if len(page) < PAGE_SIZE:
            return rows
        start += PAGE_SIZE


def selection_key(row):
    value = row.get("Value")
    if value is not None:
        value = float(value)
    return (row.get("Match"), row.get("Date"), row.get("Result"), MARKET, value)


def closing_record(row):
    record = {
        "Match": row.get("Match"),
        "Date": row.get("Date"),
        "Result": row.get("Result"),
        "Market": MARKET,
        "Value": row.get("Value"),
        "Best Bookie": row.get("Best Bookie"),
        "Best Price": row.get("Best Price"),
        "Market %": row.get("Market %"),
        "Closed Time": row.get("updated_at") or row.get("created_at"),
        "Source Table": SOURCE_TABLE,
    }
    for bookmaker in BOOKMAKERS:
        record[bookmaker] = row.get(bookmaker)
    return record


def archive_tryscorer_closing_odds(client, before_date, since_date=None, apply=False):
    source_query = (
        client.table(SOURCE_TABLE)
        .select("*")
        .lt("Date", before_date)
        .order("Date")
    )
    existing_query = (
        client.table(DESTINATION_TABLE)
        .select("Match,Date,Result,Market,Value")
        .eq("Market", MARKET)
        .lt("Date", before_date)
    )
    if since_date:
        source_query = source_query.gte("Date", since_date)
        existing_query = existing_query.gte("Date", since_date)

    source_rows = fetch_all(source_query)
    existing_rows = fetch_all(existing_query)
    existing_keys = {selection_key(row) for row in existing_rows}

    records = [
        closing_record(row)
        for row in source_rows
        if selection_key(row) not in existing_keys
    ]
    if not apply or not records:
        return {
            "source_rows": len(source_rows),
            "existing_rows": len(existing_rows),
            "inserted_rows": 0,
            "missing_rows": len(records),
        }

    for start in range(0, len(records), INSERT_BATCH_SIZE):
        batch = records[start:start + INSERT_BATCH_SIZE]
        client.table(DESTINATION_TABLE).insert(batch).execute()
        print(f"Inserted {min(start + len(batch), len(records))}/{len(records)}")

    return {
        "source_rows": len(source_rows),
        "existing_rows": len(existing_rows),
        "inserted_rows": len(records),
        "missing_rows": 0,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Backfill past NRL tryscorer prices into NRL Closing Odds."
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Insert missing rows. Without this flag, only report what would be inserted.",
    )
    args = parser.parse_args()

    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    if not url or not key:
        raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY")

    client = create_client(url, key)
    today = datetime.now(ZoneInfo("Australia/Brisbane")).date().isoformat()
    result = archive_tryscorer_closing_odds(client, before_date=today, apply=args.apply)
    print(
        f"Past source rows: {result['source_rows']}; "
        f"existing closing rows: {result['existing_rows']}; "
        f"missing rows: {result['missing_rows']}; inserted rows: {result['inserted_rows']}"
    )


if __name__ == "__main__":
    main()
