import asyncio
import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

from nba_scraper import parse_args, store_result, load_env
from scrapers.nba_props import NBAPropsScraper, classify, parse_selection, is_nba, select_main_lines

FIXTURES = json.loads((Path(__file__).parent / 'fixtures/nba_props.json').read_text())
NOW = datetime(2026, 10, 31, tzinfo=timezone.utc)
TEAMS = ['Utah Jazz', 'Portland Trail Blazers']


class ParserTests(unittest.TestCase):
    def parse(self, market, outcome, **kwargs):
        return parse_selection({'name': market}, {'name': outcome, 'price': 1.9, **kwargs}, TEAMS)

    def test_player_over_under(self):
        p = self.parse('Player Points Over/Under', 'Ace Bailey Over 15.5')
        self.assertEqual((p['player'], p['stat'], p['line'], p['side']), ('Ace Bailey', 'points', 15.5, 'over'))

    def test_explicit_milestone_overrides_half_point_field(self):
        p = self.parse('Alternate Threes', 'Ace Bailey To Get 2+ Three Pointers', points=1.5)
        self.assertEqual((p['player'], p['line'], p['side']), ('Ace Bailey', 2, 'at_least'))

    def test_handicap_and_player_in_market(self):
        p = self.parse('Ace Bailey - Points', 'Over', unformattedHandicap='15.5')
        self.assertEqual((p['player'], p['line']), ('Ace Bailey', 15.5))

    def test_periods_and_combinations(self):
        for market, expected in [('Player Pts + Rebs + Asts', ('points_rebounds_assists', 'game')),
                                 ('Player Rebs + Asts', ('rebounds_assists', 'game')),
                                 ('Ace Bailey - 1st Qtr Points', ('points', 'q1')),
                                 ('Player Points - First Half', ('points', 'h1')),
                                 ('Player Points Excluding Overtime', ('points', 'game_regulation')),
                                 ('Player 3s 2+', ('three_pointers_made', 'game'))]:
            with self.subTest(market=market):
                self.assertEqual(classify(market), expected)

    def test_early_points_and_first_basket(self):
        p = self.parse('Player to Score in the First 3 Minutes', 'Ace Bailey')
        self.assertEqual((p['period'], p['line'], p['side']), ('first_3_minutes', 1, 'at_least'))
        p = self.parse('First Basket', 'Ace Bailey')
        self.assertEqual((p['stat'], p['side']), ('first_basket', 'win'))

    def test_double_double(self):
        p = self.parse('To Record A Double Double', 'Ace Bailey')
        self.assertEqual((p['line'], p['side']), (None, 'yes'))

    def test_top_scorer_is_a_distinct_categorical_market(self):
        p = self.parse('Player To Score Most Points', 'Ace Bailey')
        self.assertEqual((p['stat'], p['side'], p['line']), ('points_most', 'win', None))

    def test_non_player_markets_rejected(self):
        for market, outcome in [('Total Points - Including Overtime', 'Over'),
                                ('Utah Jazz Total Points Over/Under 113.5', 'Over 113.5'),
                                ('Total Points', 'Over 231.5'),
                                ('Money Line and Total Points Over/Under (Including Overtime)', 'Utah Jazz and Over +231.5'),
                                ('Most Rebounds', 'Utah Jazz'),
                                ('Player Points and Team Win', 'Ace Bailey 20+'),
                                ('Player Points', 'Ace Bailey & Deni Avdija 20+')]:
            with self.subTest(market=market):
                self.assertIsNone(self.parse(market, outcome, points=231.5))

    def test_invalid_and_suspended_prices(self):
        for fields in [{'price': None}, {'price': 0}, {'price': 1}, {'price': float('nan')},
                       {'price': float('inf')}, {'isOpenForBetting': False}, {'statusCode': 'S'},
                       {'isHidden': True}, {'isScratched': True}]:
            with self.subTest(fields=fields):
                self.assertIsNone(self.parse('Player Points', 'Ace Bailey Over 15.5', **fields))

    def test_kambi_scaled_values(self):
        p = parse_selection({'criterion': {'englishLabel': 'Player Points'}},
                            {'participant': 'Ace Bailey', 'label': 'Over', 'line': 15500, 'odds': 1900})
        self.assertEqual((p['line'], p['price']), (15.5, 1.9))

    def test_market_suspension(self):
        self.assertIsNone(parse_selection({'name': 'Player Points', 'suspended': True},
                                         {'name': 'Ace Bailey Over 15.5', 'price': 1.9}))

    def test_strict_nba_competition(self):
        for name in ('WNBA', 'NBA Futures', 'NBA Summer League', 'NBL'):
            self.assertFalse(is_nba(name))
        self.assertTrue(is_nba('NBA'))


class ScraperTests(unittest.TestCase):
    def scraper(self, **kwargs):
        return NBAPropsScraper(None, now=NOW, **kwargs)

    def event(self, scraper, book='Sportsbet'):
        return scraper.event(book, '1', 'Utah Jazz v Portland Trail Blazers', '2026-10-31T19:00:00Z')

    def test_real_feed_fixtures(self):
        for book, fixture in FIXTURES.items():
            with self.subTest(book=book):
                scraper = self.scraper()
                scraper.collect(self.event(scraper), fixture)
                if book == 'unibet':
                    # This live event only had game lines, no player props listed.
                    self.assertEqual(len(scraper.rows), 0)
                else:
                    self.assertGreater(len(scraper.rows), 0)
                    self.assertTrue(all(p['player'] not in TEAMS for p in scraper.rows.values()))
                    json.dumps(list(scraper.rows.values()), allow_nan=False)

    def test_outcome_stacks_and_duplicates(self):
        scraper = self.scraper()
        market = {'id': 123, 'name': 'Player Points 20+', 'outcomes': [], 'outcomeStack': {
            'competitor1': [{'id': 1, 'name': 'Ace Bailey', 'price': 1.9}], 'competitor2': []}}
        event = self.event(scraper)
        scraper.collect(event, [market, market])
        self.assertEqual(len(scraper.rows), 1)
        row = next(iter(scraper.rows.values()))
        self.assertEqual((row['player'], row['line']), ('Ace Bailey', 20))

    def test_side_and_line_identity_not_collapsed(self):
        scraper = self.scraper()
        scraper.collect(self.event(scraper), {'id': 1, 'name': 'Ace Bailey - Points', 'outcomes': [
            {'id': 1, 'name': 'Over 15.5', 'price': 1.9}, {'id': 2, 'name': 'Under 15.5', 'price': 1.9},
            {'id': 3, 'name': 'Over 16.5', 'price': 2.1}]})
        self.assertEqual(len(scraper.rows), 3)

    def test_brisbane_date_and_match_direction(self):
        scraper = self.scraper()
        first = self.event(scraper)
        other = scraper.event('Pointsbet', 2, 'Portland Trail Blazers @ Utah Jazz', '2026-10-31T19:10:00Z')
        self.assertEqual(first['game_date'], '2026-11-01')
        self.assertEqual(first['event_key'], other['event_key'])

    def test_started_live_and_horizon_excluded(self):
        scraper = self.scraper(days=1)
        for start, live in [('2026-10-30T19:00:00Z', False), ('2026-11-02T19:00:00Z', False),
                            ('2026-10-31T19:00:00Z', True), ('invalid', False)]:
            self.assertIsNone(scraper.event('Sportsbet', 1, 'A Team v B Team', start, live=live))

    def test_requests_all_pointsbet_pages_and_event_details(self):
        class Client:
            errors = []
            urls = []
            async def get(self, url):
                self.urls.append(url)
                if url.endswith('competitions'):
                    return {'locales': [{'competitions': [{'key': '7176', 'name': 'NBA'}, {'key': 'x', 'name': 'WNBA'}]}]}
                if 'competition/' in url:
                    page = 1 if url.endswith('page=1') else 2
                    return {'nextPage': 2 if page == 1 else None, 'events': [{'key': page,
                        'name': 'A Team v B Team', 'startsAt': '2026-10-31T19:00:00Z', 'isLive': False}]}
                return {'fixedOddsMarkets': FIXTURES['pointsbet']}
        client = Client()
        scraper = NBAPropsScraper(client, now=NOW)
        result = asyncio.run(scraper.scrape('Pointsbet'))
        self.assertEqual(result['events'], 2)
        self.assertTrue(result['complete'])
        self.assertTrue(any(u.endswith('/events/2') for u in client.urls))
        self.assertFalse(any('competition/x' in u for u in client.urls))

    def test_malformed_feed_is_failure(self):
        class Client:
            errors = []
            async def get(self, url):
                return {'unexpected': []}
        with self.assertRaises(ValueError):
            asyncio.run(NBAPropsScraper(Client(), now=NOW).scrape('Pointsbet'))

    def test_partial_group_failure_finishes_other_requests(self):
        client = MagicMock()
        client.errors = []
        scraper = NBAPropsScraper(client, now=NOW)
        finished = []
        async def fail():
            raise ValueError('missing market prices')
        async def succeed():
            await asyncio.sleep(0)
            finished.append(True)
        asyncio.run(scraper.collect_many([fail(), succeed()]))
        self.assertEqual(finished, [True])
        self.assertEqual(client.errors, [{'error': 'missing market prices'}])

    def test_betr_fetches_every_market_group(self):
        class Client:
            errors = []
            urls = []
            async def get(self, url):
                self.urls.append(url)
                if 'MasterCategory?' in url:
                    return {'MasterCategories': [{'Categories': [{'CategoryName': 'NBA', 'MasterEvents': [{
                        'MasterEventId': 1, 'MasterEventName': 'A Team v B Team',
                        'MaxAdvertisedStartTime': '2026-10-31T19:00:00.0000000Z'}]}]}]}
                if 'GroupTypeCode=' in url:
                    return {'Events': [{'EventId': 100, 'EventName': 'Player Points', 'Outcomes': [
                        {'OutcomeId': 1, 'OutcomeName': 'Ace Bailey Over 20.5', 'Price': 1.9, 'IsOpenForBetting': True},
                        {'OutcomeId': 2, 'OutcomeName': 'Ace Bailey Under 20.5', 'Price': 1.9, 'IsOpenForBetting': True}]}]}
                return {'Events': [], 'GroupLinks': [{'GroupTypeCode': 'G199'}, {'GroupTypeCode': 'G198'}]}
        client = Client()
        result = asyncio.run(NBAPropsScraper(client, now=NOW).scrape('Betr'))
        self.assertTrue(result['complete'])
        self.assertEqual(len(result['rows']), 2)
        self.assertEqual(sum('GroupTypeCode=' in u for u in client.urls), 2)


class MainLineTests(unittest.TestCase):
    def quotes(self, stat='points', period='game', line=20.5, prices=(1.9, 1.9), market='Main'):
        return [dict(bookmaker='Sportsbet', event_id='1', player_key='ace bailey', stat=stat,
                     period=period, line=line, side=side, price=price, market_id=market,
                     source_market=market, fetched_at='2026-10-31T08:00:00+00:00')
                for side, price in zip(('over', 'under'), prices)]

    def test_only_six_full_game_stats(self):
        rows = sum((self.quotes(stat=s) for s in ('points', 'rebounds', 'assists',
                   'three_pointers_made', 'steals', 'blocks', 'points_rebounds_assists', 'turnovers')), [])
        rows += self.quotes(period='q1') + self.quotes(period='h1')
        rows += [dict(self.quotes()[0], side='at_least')]
        selected = select_main_lines(rows)
        self.assertEqual(len(selected), 12)
        self.assertEqual({r['side'] for r in selected}, {'over', 'under'})

    def test_prefers_main_then_balanced_line(self):
        rows = (self.quotes(line=19.5, prices=(1.3, 3), market='Main 19.5')
                + self.quotes(line=20.5, market='Main 20.5')
                + self.quotes(line=21.5, market='Alternate Points'))
        selected = select_main_lines(rows)
        self.assertEqual(len(selected), 2)
        self.assertEqual({r['line'] for r in selected}, {20.5})

    def test_requires_both_sides_at_same_line_and_market(self):
        over, under = self.quotes()
        self.assertEqual(select_main_lines([over]), [])
        self.assertEqual(select_main_lines([over, dict(under, line=21.5)]), [])
        self.assertEqual(select_main_lines([over, dict(under, market_id='Different')]), [])

    def test_rejects_previously_parsed_team_combo_rows(self):
        rows = [dict(r, player_key='utah jazz and') for r in self.quotes()]
        self.assertEqual(select_main_lines(rows), [])


class StorageTests(unittest.TestCase):
    def test_loads_local_backend_credentials_without_overriding_environment(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / '.env.local').write_text('NEXT_PUBLIC_SUPABASE_URL=https://example.supabase.co\nSUPABASE_SERVICE_ROLE_KEY=test-backend-key\n')
            with patch('nba_scraper.__file__', str(root / 'nba_scraper.py')), patch.dict(os.environ, {}, clear=True):
                load_env()
                self.assertEqual(os.environ['SUPABASE_URL'], 'https://example.supabase.co')
                self.assertEqual(os.environ['SUPABASE_KEY'], 'test-backend-key')
            with patch('nba_scraper.__file__', str(root / 'nba_scraper.py')), patch.dict(os.environ, {'SUPABASE_KEY': 'existing-key'}, clear=True):
                load_env()
                self.assertEqual(os.environ['SUPABASE_KEY'], 'existing-key')

    def test_partial_result_keeps_existing_prices_and_sets_expiry(self):
        scraper = NBAPropsScraper(None, now=NOW)
        event = scraper.event('Sportsbet', 1, 'A Team v B Team', '2026-10-31T19:00:00Z')
        scraper.collect(event, FIXTURES['sportsbet'])
        result = {'bookmaker': 'Sportsbet', 'complete': False, 'rows': list(scraper.rows.values())}
        client = MagicMock()
        store_result(client, result, '00000000-0000-0000-0000-000000000001', 30)
        name, payload = client.rpc.call_args[0]
        self.assertEqual(name, 'ingest_nba_player_props')
        self.assertFalse(payload['p_complete'])
        quote = payload['p_quotes'][0]
        self.assertEqual((datetime.fromisoformat(quote['expires_at']) - datetime.fromisoformat(quote['fetched_at'])).total_seconds(), 1800)
        self.assertNotIn('expires_at', result['rows'][0])

    def test_empty_success_retires_missing_quotes(self):
        client = MagicMock()
        store_result(client, {'bookmaker': 'Sportsbet', 'complete': True, 'rows': []}, 'run')
        self.assertTrue(client.rpc.call_args[0][1]['p_complete'])

    def test_cli_dry_run_by_default(self):
        self.assertFalse(parse_args([]).write)
        self.assertEqual(len(parse_args([]).bookmakers), 8)


if __name__ == '__main__':
    unittest.main()
