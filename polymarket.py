
import json
import time
from typing import Dict, Iterable, List, Optional
import requests
import pandas as pd
from zoneinfo import ZoneInfo
from datetime import datetime

BASE_URL = "https://gamma-api.polymarket.com"
CLOB_URL = "https://clob.polymarket.com"
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "pm-sports-orderbook/0.1",
    "Accept": "application/json"
})


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
    """Fetch lowest ask and highest bid for a given outcome token_id from the CLOB API."""
    try:
        url = f"{CLOB_URL}/book"
        params = {"token_id": token_id}
        data = _get(url, params=params)
        asks = data.get("asks", [])
        bids = data.get("bids", [])
        lowest_ask = float(asks[0]["price"]) if asks else None
        highest_bid = float(bids[0]["price"]) if bids else None
        return lowest_ask, highest_bid
    except Exception:
        return None, None


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
        print(f"🏟️ {match}")
        date_str = parse_event_date(m, tz)

        # Extract outcomes + token IDs
        outcomes = m.get("outcomes") or []
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


def main():
    dfs = []
    for sport in polymarket_sport_map.keys():
        print(f"\nFetching {sport.upper()} markets...")
        df = build_df(sport=sport, tz="Australia/Brisbane", min_liq=None, types=["moneyline"])
        if not df.empty:
            df["sport"] = polymarket_sport_map[sport]
            df["decimal_price"] = df["ask"].apply(lambda x: 1 / x if x and x > 0 else None)
            dfs.append(df)
            print(f"✅ {sport}: {len(df)} rows")

    if dfs:
        final_df = pd.concat(dfs, ignore_index=True)
        final_df.to_csv("polymarket_orderbook_prices.csv", index=False)
        print("\n💾 Saved to polymarket_orderbook_prices.csv")
    else:
        print("⚠️ No data found.")


if __name__ == "__main__":
    main()