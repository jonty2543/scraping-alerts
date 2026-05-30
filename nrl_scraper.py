import time
import asyncio
import re
from collections import defaultdict
from datetime import datetime

import pytz
import nest_asyncio
from loguru import logger

import scrapers.sportsbet_scrapers as sb
import scrapers.pointsbet_scrapers as pb
import scrapers.unibet_scrapers as ub
import scrapers.PalmerBet_scrapers as palm
import scrapers.betright_scrapers as br

import functions as f

nest_asyncio.apply()
UPSERT_NRL = False

def _extract_market_value(result, market_kind):
    mtch = re.search(r'[-+]?\d+(?:\.\d+)?', str(result))
    if not mtch:
        return None
    value = float(mtch.group(0))
    if market_kind == "line":
        value = abs(value)
    return round(value, 1)

def _extract_signed_value(result):
    mtch = re.search(r'([-+]?\d+(?:\.\d+)?)', str(result))
    if not mtch:
        return None
    try:
        return float(mtch.group(1))
    except ValueError:
        return None

def _extract_side(result, market_kind):
    text = str(result).lower()
    if market_kind == "line":
        signed = _extract_signed_value(result)
        if signed is None:
            return None
        if signed > 0:
            return "plus"
        if signed < 0:
            return "minus"
        return None

    if "over" in text:
        return "over"
    if "under" in text:
        return "under"
    return None

def _select_three_center_values(bookmakers_markets, market_kind, num_values=3, line_window_points=None):
    # key -> value -> bookmaker -> sides
    value_bookies = defaultdict(lambda: defaultdict(lambda: defaultdict(set)))
    value_sides = defaultdict(lambda: defaultdict(set))
    required_sides = {"plus", "minus"} if market_kind == "line" else {"over", "under"}

    for bookie, markets in bookmakers_markets.items():
        for key, odds in markets.items():
            for result in odds.keys():
                value = _extract_market_value(result, market_kind)
                side = _extract_side(result, market_kind)
                if value is None or side is None:
                    continue
                value = round(float(value), 1)
                value_bookies[key][value][bookie].add(side)
                value_sides[key][value].add(side)

    selected_values = {}
    for key, value_map in value_bookies.items():
        if not value_map:
            continue

        candidates = [v for v in value_map.keys() if required_sides.issubset(value_sides[key].get(v, set()))]
        if not candidates:
            candidates = list(value_map.keys())

        if not candidates:
            continue

        candidate_sorted = sorted(candidates)
        median = candidate_sorted[len(candidate_sorted) // 2]

        def full_pair_coverage(v):
            return sum(1 for _, sides in value_map[v].items() if required_sides.issubset(sides))

        def any_coverage(v):
            return len(value_map[v])

        pair_candidates = [v for v in candidates if full_pair_coverage(v) > 0]
        if pair_candidates:
            candidates = pair_candidates

        center = sorted(
            candidates,
            key=lambda v: (
                -full_pair_coverage(v),
                -any_coverage(v),
                abs(v - median),
                v,
            ),
        )[0]

        if market_kind == "line" and line_window_points is not None:
            picked = [v for v in sorted(candidates) if abs(v - center) <= float(line_window_points) + 1e-9]
            if not picked:
                picked = [center]
        else:
            ordered = sorted(
                candidates,
                key=lambda v: (
                    abs(v - center),
                    -full_pair_coverage(v),
                    -any_coverage(v),
                    v,
                ),
            )
            picked = ordered[:max(1, int(num_values))]

        selected_values[key] = set(round(float(v), 1) for v in picked)

    filtered = {}
    for bookie, markets in bookmakers_markets.items():
        new_markets = {}
        for key, odds in markets.items():
            allowed = selected_values.get(key)
            if not allowed:
                continue

            per_value = defaultdict(dict)
            for result, price in odds.items():
                value = _extract_market_value(result, market_kind)
                side = _extract_side(result, market_kind)
                if value is None or side is None:
                    continue
                value = round(float(value), 1)
                if value in allowed:
                    per_value[value][side] = (result, price)

            kept = {}
            for value, side_rows in per_value.items():
                if not required_sides.issubset(set(side_rows.keys())):
                    continue
                for side in required_sides:
                    result, price = side_rows[side]
                    kept[result] = price

            if len(kept) >= 2:
                new_markets[key] = kept

        filtered[bookie] = new_markets

    return filtered, selected_values


async def main():
    chosen_date = datetime.now(pytz.timezone("Australia/Brisbane")).date().strftime("%Y-%m-%d")

    pb_nrl_url = f.pb_nrl_url
    palm_nrl_url = f.palm_nrl_url

    # -------- NRL H2H --------
    logger.info("Scraping Sportsbet NRL H2H data")
    sb_scraper = sb.SBSportsScraper(f.get_sportsbet_url(sportId=23), chosen_date=chosen_date)
    sb_nrl_h2h = await sb_scraper.SPORTSBET_scraper(competition_id=3436)

    time.sleep(3)

    logger.info("Scraping Pointsbet NRL H2H data")
    pb_scraper = pb.PBSportsScraper(pb_nrl_url, chosen_date=chosen_date)
    pb_nrl_h2h = await pb_scraper.POINTSBET_scrape_nrl(market_type='Match Result')

    time.sleep(3)

    logger.info("Scraping Unibet NRL H2H data")
    ub_scraper = ub.UBSportsScraper(f.get_ub_url('rugby_league'), chosen_date=chosen_date)
    ub_nrl_h2h = await ub_scraper.UNIBET_scrape_sport(comp='NRL', market_type='Match')

    time.sleep(3)

    logger.info("Scraping Palmerbet NRL H2H data")
    palm_scraper = palm.PalmerBetSportsScraper(palm_nrl_url, chosen_date=chosen_date)
    palm_nrl_h2h = await palm_scraper.PalmerBet_scrape(comp='Australia National Rugby League', market_type='h2h')

    time.sleep(3)

    logger.info("Scraping Betright NRL H2H data")
    br_scraper = br.BRSportsScraper(f.get_betright_url(102), chosen_date=chosen_date)
    br_nrl_h2h = await br_scraper.BETRIGHT_scraper_masterevent(market_kind='h2h', category_name='NRL')

    bookmakers_h2h = {
        "Sportsbet": sb_nrl_h2h,
        "Pointsbet": pb_nrl_h2h,
        "Unibet": ub_nrl_h2h,
        "Palmerbet": palm_nrl_h2h,
        "Betright": br_nrl_h2h,
    }
    h2h_counts = {k: len(v) for k, v in bookmakers_h2h.items()}
    logger.info(f"NRL h2h market counts: {h2h_counts}")
    price_cols_h2h = ["Sportsbet", "Pointsbet", "Unibet", "Palmerbet", "Betright"]

    f.process_odds(
        bookmakers_h2h,
        price_cols_h2h,
        table_name="NRL Odds",
        match_threshold=80,
        upsert=UPSERT_NRL,
        upsert_keys=["Match", "Date", "Result"],
        store_closing_odds=True,
        closing_table_name="NRL Closing Odds",
    )

    # -------- NRL Line --------
    logger.info("Scraping Sportsbet NRL line data")
    sb_line_total_url = f.get_sportsbet_url(sportId=23).replace("primaryMarketOnly=true", "primaryMarketOnly=false")
    sb_scraper = sb.SBSportsScraper(sb_line_total_url, chosen_date=chosen_date)
    sb_nrl_line = await sb_scraper.SPORTSBET_scraper_lines_totals(market_kind='line', competition_id=3436)
    if not sb_nrl_line:
        logger.info("Retrying Sportsbet NRL line data with primaryMarketOnly=true")
        sb_scraper = sb.SBSportsScraper(f.get_sportsbet_url(sportId=23), chosen_date=chosen_date)
        sb_nrl_line = await sb_scraper.SPORTSBET_scraper_lines_totals(market_kind='line', competition_id=3436)

    time.sleep(3)

    logger.info("Scraping Pointsbet NRL line data")
    pb_scraper = pb.PBSportsScraper(pb_nrl_url, chosen_date=chosen_date)
    pb_nrl_line = await pb_scraper.POINTSBET_scrape_nrl(market_type='Line')

    time.sleep(3)

    logger.info("Scraping Unibet NRL line data")
    ub_scraper = ub.UBSportsScraper(f.get_ub_url('rugby_league'), chosen_date=chosen_date)
    ub_nrl_line = await ub_scraper.UNIBET_scrape_sport(comp='NRL', market_type='Handicap', include_line=True)

    time.sleep(3)

    logger.info("Scraping Palmerbet NRL line data")
    palm_scraper = palm.PalmerBetSportsScraper(palm_nrl_url, chosen_date=chosen_date)
    palm_nrl_line = await palm_scraper.PalmerBet_scrape(comp='Australia National Rugby League', market_type='line')

    time.sleep(3)

    logger.info("Scraping Betright NRL line data")
    br_scraper = br.BRSportsScraper(f.get_betright_url(102), chosen_date=chosen_date)
    br_nrl_line = await br_scraper.BETRIGHT_scraper_masterevent(market_kind='line', category_name='NRL')

    bookmakers_line = {
        "Sportsbet": sb_nrl_line,
        "Pointsbet": pb_nrl_line,
        "Unibet": ub_nrl_line,
        "Palmerbet": palm_nrl_line,
        "Betright": br_nrl_line,
    }
    line_counts = {k: len(v) for k, v in bookmakers_line.items()}
    logger.info(f"NRL line market counts: {line_counts}")

    price_cols_line = ["Sportsbet", "Pointsbet", "Unibet", "Palmerbet", "Betright"]

    f.process_line_total_wide(
        bookmakers_line,
        price_cols_line,
        table_name="NRL Line Odds",
        market_kind="line",
        match_threshold=80,
        upsert=UPSERT_NRL,
        upsert_keys=["Match", "Date", "Result"],
        store_closing_odds=True,
        closing_table_name="NRL Closing Odds",
    )

    # -------- NRL Total --------
    logger.info("Scraping Sportsbet NRL total data")
    sb_scraper = sb.SBSportsScraper(sb_line_total_url, chosen_date=chosen_date)
    sb_nrl_total = await sb_scraper.SPORTSBET_scraper_lines_totals(market_kind='total', competition_id=3436)
    if not sb_nrl_total:
        logger.info("Retrying Sportsbet NRL total data with primaryMarketOnly=true")
        sb_scraper = sb.SBSportsScraper(f.get_sportsbet_url(sportId=23), chosen_date=chosen_date)
        sb_nrl_total = await sb_scraper.SPORTSBET_scraper_lines_totals(market_kind='total', competition_id=3436)

    time.sleep(3)

    logger.info("Scraping Pointsbet NRL total data")
    pb_scraper = pb.PBSportsScraper(pb_nrl_url, chosen_date=chosen_date)
    pb_nrl_total = await pb_scraper.POINTSBET_scrape_nrl(market_type='Total Match Points Over/Under')

    time.sleep(3)

    logger.info("Scraping Unibet NRL total data")
    ub_scraper = ub.UBSportsScraper(f.get_ub_url('rugby_league'), chosen_date=chosen_date)
    ub_nrl_total = await ub_scraper.UNIBET_scrape_sport(
        comp='NRL',
        market_type=['Total', 'Total Points', 'Match Total'],
        market_match_mode='contains',
        include_line=True,
    )

    time.sleep(3)

    logger.info("Scraping Palmerbet NRL total data")
    palm_scraper = palm.PalmerBetSportsScraper(palm_nrl_url, chosen_date=chosen_date)
    palm_nrl_total = await palm_scraper.PalmerBet_scrape(comp='Australia National Rugby League', market_type='total')

    time.sleep(3)

    logger.info("Scraping Betright NRL total data")
    br_scraper = br.BRSportsScraper(f.get_betright_url(102), chosen_date=chosen_date)
    br_nrl_total = await br_scraper.BETRIGHT_scraper_masterevent(market_kind='total', category_name='NRL')

    bookmakers_total = {
        "Sportsbet": sb_nrl_total,
        "Pointsbet": pb_nrl_total,
        "Unibet": ub_nrl_total,
        "Palmerbet": palm_nrl_total,
        "Betright": br_nrl_total,
    }
    total_counts = {k: len(v) for k, v in bookmakers_total.items()}
    logger.info(f"NRL total market counts: {total_counts}")

    price_cols_total = ["Sportsbet", "Pointsbet", "Unibet", "Palmerbet", "Betright"]

    f.process_line_total_wide(
        bookmakers_total,
        price_cols_total,
        table_name="NRL Total Odds",
        market_kind="total",
        match_threshold=80,
        upsert=UPSERT_NRL,
        upsert_keys=["Match", "Date", "Result"],
        store_closing_odds=True,
        closing_table_name="NRL Closing Odds",
    )

    # -------- NRL Tryscorers --------
    logger.info("Scraping Sportsbet NRL tryscorer data")
    sb_scraper = sb.SBSportsScraper(sb_line_total_url, chosen_date=chosen_date)
    sb_nrl_tryscorers = await sb_scraper.SPORTSBET_scraper_tryscorers(competition_id=3436)

    time.sleep(3)

    logger.info("Scraping Pointsbet NRL tryscorer data")
    pb_scraper = pb.PBSportsScraper(pb_nrl_url, chosen_date=chosen_date)
    pb_nrl_tryscorers = await pb_scraper.POINTSBET_scrape_nrl_tryscorers()

    time.sleep(3)

    logger.info("Scraping Unibet NRL tryscorer data")
    ub_scraper = ub.UBSportsScraper(f.get_ub_url('rugby_league'), chosen_date=chosen_date)
    ub_nrl_tryscorers = await ub_scraper.UNIBET_scrape_nrl_tryscorers(comp='NRL')

    time.sleep(3)

    logger.info("Scraping Palmerbet NRL tryscorer data")
    palm_scraper = palm.PalmerBetSportsScraper(palm_nrl_url, chosen_date=chosen_date)
    palm_nrl_tryscorers = await palm_scraper.PalmerBet_scrape_nrl_tryscorers(comp='Australia National Rugby League')

    time.sleep(3)

    logger.info("Scraping Betright NRL tryscorer data")
    br_scraper = br.BRSportsScraper(f.get_betright_url(102), chosen_date=chosen_date)
    br_nrl_tryscorers = await br_scraper.BETRIGHT_scraper_nrl_tryscorers(category_name='NRL')

    bookmakers_tryscorers = {
        "Sportsbet": sb_nrl_tryscorers,
        "Pointsbet": pb_nrl_tryscorers,
        "Unibet": ub_nrl_tryscorers,
        "Palmerbet": palm_nrl_tryscorers,
        "Betright": br_nrl_tryscorers,
    }
    tryscorer_counts = {k: sum(len(vv) for vv in v.values()) for k, v in bookmakers_tryscorers.items()}
    logger.info(f"NRL tryscorer selection counts: {tryscorer_counts}")

    price_cols_tryscorers = ["Sportsbet", "Pointsbet", "Unibet", "Palmerbet", "Betright"]

    f.process_odds(
        bookmakers_tryscorers,
        price_cols_tryscorers,
        table_name="NRL Tryscorers",
        match_threshold=80,
        outcomes=1,
        names=True,
        market="Tryscorer",
        include_value=True,
        min_mkt_percent=0,
        upsert=True,
        upsert_keys=["Match", "Date", "Result", "Value"],
        prune_stale_upsert=True,
        prune_scope_keys=["Match", "Date"],
    )


if __name__ == "__main__":
    asyncio.run(main())
