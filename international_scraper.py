import argparse
import asyncio
import time
from datetime import datetime

import nest_asyncio
import pytz
from loguru import logger

import functions as f
import scrapers.PalmerBet_scrapers as palm
import scrapers.betright_scrapers as br
import scrapers.pointsbet_scrapers as pb
import scrapers.sportsbet_scrapers as sb

nest_asyncio.apply()

SPORTSBET_RLWC_COMPETITION_ID = 28801
UPSERT_INTERNATIONALS = False


async def main(markets=None):
    chosen_date = datetime.now(pytz.timezone("Australia/Brisbane")).date().strftime("%Y-%m-%d")
    selected_markets = set(markets or ["h2h", "line", "total"])

    sportsbet_url = f.get_sportsbet_url(sportId=23)
    sportsbet_line_total_url = sportsbet_url.replace("primaryMarketOnly=true", "primaryMarketOnly=false")
    pointsbet_url = f.pb_rlwc_url
    palmerbet_url = f.palm_nrl_url
    betright_url = f.get_betright_url(102)
    price_cols = ["Sportsbet", "Pointsbet", "Palmerbet", "Betright"]
    results = {}

    if "h2h" in selected_markets:
        logger.info("Scraping rugby league internationals H2H data")
        sb_scraper = sb.SBSportsScraper(sportsbet_url, chosen_date=chosen_date)
        sb_h2h = await sb_scraper.SPORTSBET_scraper(
            competition_id=SPORTSBET_RLWC_COMPETITION_ID
        )

        time.sleep(3)

        pb_scraper = pb.PBSportsScraper(pointsbet_url, chosen_date=chosen_date)
        pb_h2h = await pb_scraper.POINTSBET_scrape_nrl(market_type="Match Result")

        time.sleep(3)

        palm_scraper = palm.PalmerBetSportsScraper(palmerbet_url, chosen_date=chosen_date)
        palm_h2h = await palm_scraper.PalmerBet_scrape(
            comp="Men's World Cup",
            market_type="h2h",
        )

        time.sleep(3)

        br_scraper = br.BRSportsScraper(betright_url, chosen_date=chosen_date)
        br_h2h = await br_scraper.BETRIGHT_scraper_masterevent(
            market_kind="h2h",
            category_name="Rugby League World Cup",
        )

        bookmakers_h2h = {
            "Sportsbet": sb_h2h,
            "Pointsbet": pb_h2h,
            "Palmerbet": palm_h2h,
            "Betright": br_h2h,
        }
        h2h_counts = {k: len(v) for k, v in bookmakers_h2h.items()}
        logger.info(f"Internationals h2h market counts: {h2h_counts}")
        results["h2h"], _ = f.process_odds(
            bookmakers_h2h,
            price_cols,
            table_name="Rugby League Internationals Odds",
            match_threshold=80,
            upsert=UPSERT_INTERNATIONALS,
            upsert_keys=["Match", "Date", "Result"],
        )

    if "line" in selected_markets:
        logger.info("Scraping rugby league internationals line data")
        sb_scraper = sb.SBSportsScraper(sportsbet_line_total_url, chosen_date=chosen_date)
        sb_line = await sb_scraper.SPORTSBET_scraper_lines_totals(
            market_kind="line",
            competition_id=SPORTSBET_RLWC_COMPETITION_ID,
        )
        if not sb_line:
            logger.info("Retrying Sportsbet rugby league internationals line data with primaryMarketOnly=true")
            sb_scraper = sb.SBSportsScraper(sportsbet_url, chosen_date=chosen_date)
            sb_line = await sb_scraper.SPORTSBET_scraper_lines_totals(
                market_kind="line",
                competition_id=SPORTSBET_RLWC_COMPETITION_ID,
            )

        time.sleep(3)

        pb_scraper = pb.PBSportsScraper(pointsbet_url, chosen_date=chosen_date)
        pb_line = await pb_scraper.POINTSBET_scrape_nrl(market_type="Line")

        time.sleep(3)

        palm_scraper = palm.PalmerBetSportsScraper(palmerbet_url, chosen_date=chosen_date)
        palm_line = await palm_scraper.PalmerBet_scrape(
            comp="Men's World Cup",
            market_type="line",
        )

        time.sleep(3)

        br_scraper = br.BRSportsScraper(betright_url, chosen_date=chosen_date)
        br_line = await br_scraper.BETRIGHT_scraper_masterevent(
            market_kind="line",
            category_name="Rugby League World Cup",
        )

        bookmakers_line = {
            "Sportsbet": sb_line,
            "Pointsbet": pb_line,
            "Palmerbet": palm_line,
            "Betright": br_line,
        }
        line_counts = {k: len(v) for k, v in bookmakers_line.items()}
        logger.info(f"Internationals line market counts: {line_counts}")
        results["line"] = f.process_line_total_wide(
            bookmakers_line,
            price_cols,
            table_name="Rugby League Internationals Line Odds",
            market_kind="line",
            match_threshold=80,
            upsert=UPSERT_INTERNATIONALS,
            upsert_keys=["Match", "Date", "Result"],
        )

    if "total" in selected_markets:
        logger.info("Scraping rugby league internationals total data")
        sb_scraper = sb.SBSportsScraper(sportsbet_line_total_url, chosen_date=chosen_date)
        sb_total = await sb_scraper.SPORTSBET_scraper_lines_totals(
            market_kind="total",
            competition_id=SPORTSBET_RLWC_COMPETITION_ID,
        )
        if not sb_total:
            logger.info("Retrying Sportsbet rugby league internationals total data with primaryMarketOnly=true")
            sb_scraper = sb.SBSportsScraper(sportsbet_url, chosen_date=chosen_date)
            sb_total = await sb_scraper.SPORTSBET_scraper_lines_totals(
                market_kind="total",
                competition_id=SPORTSBET_RLWC_COMPETITION_ID,
            )

        time.sleep(3)

        pb_scraper = pb.PBSportsScraper(pointsbet_url, chosen_date=chosen_date)
        pb_total = await pb_scraper.POINTSBET_scrape_nrl(market_type="Total Match Points Over/Under")

        time.sleep(3)

        palm_scraper = palm.PalmerBetSportsScraper(palmerbet_url, chosen_date=chosen_date)
        palm_total = await palm_scraper.PalmerBet_scrape(
            comp="Men's World Cup",
            market_type="total",
        )

        bookmakers_total = {
            "Sportsbet": sb_total,
            "Pointsbet": pb_total,
            "Palmerbet": palm_total,
            "Betright": {},
        }
        total_counts = {k: len(v) for k, v in bookmakers_total.items()}
        logger.info(f"Internationals total market counts: {total_counts}")
        results["total"] = f.process_line_total_wide(
            bookmakers_total,
            price_cols,
            table_name="Rugby League Internationals Total Odds",
            market_kind="total",
            match_threshold=80,
            upsert=UPSERT_INTERNATIONALS,
            upsert_keys=["Match", "Date", "Result"],
        )

    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape rugby league internationals odds.")
    parser.add_argument(
        "--market",
        action="append",
        choices=["h2h", "line", "total"],
        help="Market to scrape. Repeat to scrape multiple markets. Defaults to all match markets.",
    )
    args = parser.parse_args()
    asyncio.run(main(markets=args.market))
