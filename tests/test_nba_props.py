import asyncio
import io
import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from urllib.error import HTTPError
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from nba_scraper import parse_args, store_result, load_env
from scrapers.nba_props import NBAPropsScraper, FeedClient, FeedHTTPError, retry_after_seconds, classify, parse_selection, is_nba, select_main_lines, with_query, firestore_document

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

    def test_unibet_numbered_period_labels_are_not_full_game(self):
        for label, period in [('Quarter 1', 'q1'), ('Quarter 2', 'q2'), ('Quarter 3', 'q3'),
                              ('Quarter 4', 'q4'), ('Q1', 'q1'), ('Q 1', 'q1'),
                              ('1Q', 'q1'), ('1st Quarter', 'q1'), ('Half 1', 'h1'),
                              ('Half 2', 'h2'), ('H1', 'h1'), ('2H', 'h2')]:
            with self.subTest(label=label):
                self.assertEqual(classify('Points Scored by the Player - ' + label), ('points', period))
        parsed = parse_selection({'criterion': {'label': 'Points Scored by the Player - Quarter 1'}},
                                 {'label': 'Over', 'participant': 'Onyeka Okongwu', 'line': 3500, 'odds': 1830})
        self.assertEqual((parsed['stat'], parsed['period'], parsed['line']), ('points', 'q1', 3.5))
        self.assertEqual(classify('Points Scored by the Player - Including Overtime'), ('points', 'game'))

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


class AdapterCoverageTests(unittest.TestCase):
    def test_palmerbet_discovers_nba_instead_of_general_basketball_match_list(self):
        class Client:
            errors = []
            urls = []
            async def get(self, url):
                self.urls.append(url)
                if '/fixtures/sports/b26e5acc-' in url:
                    return {'sportItem': {'subItems': [{'id': 'nba', 'title': 'NBA'}, {'id': 'wnba', 'title': 'WNBA'}]}}
                if '/nba/matches?' in url:
                    return {'matches': [{'eventId': '1', 'paths': [{'title': 'NBA'}], 'status': 'NotStarted',
                        'homeTeam': {'title': TEAMS[0]}, 'awayTeam': {'title': TEAMS[1]},
                        'startTime': '2026-10-31T19:00:00Z'}]}
                return {'markets': [{'id': 'p', 'name': 'Ace Bailey - Points', 'outcomes': [
                    {'id': 'o', 'name': 'Over 15.5', 'price': 1.9}, {'id': 'u', 'name': 'Under 15.5', 'price': 1.9}]}]}
        client = Client()
        result = asyncio.run(NBAPropsScraper(client, now=NOW).scrape('Palmerbet'))
        self.assertEqual(len(result['rows']), 2)
        self.assertTrue(result['complete'])
        self.assertTrue(any('/nba/matches?' in u for u in client.urls))
        self.assertFalse(any('/b26e5acc-' in u and '/matches?' in u for u in client.urls))
        self.assertFalse(any('/wnba/' in u for u in client.urls))

    def test_betright_requests_separate_player_groups(self):
        class Client:
            errors = []
            urls = []
            async def get(self, url):
                self.urls.append(url)
                if 'MasterCategory?' in url:
                    return {'masterCategories': [{'categories': [{'categoryName': 'NBA', 'categoryId': 54}]}]}
                if '/Category?' in url:
                    return {'masterCategories': [{'masterEvents': [{'masterEventId': 1,
                        'masterEventName': 'Utah Jazz v Portland Trail Blazers',
                        'maxAdvertisedStartTimeUtc': '2026-10-31T19:00:00Z'}]}]}
                if 'groupTypeCode=' in url:
                    return {'events': [{'id': 9, 'name': 'Ace Bailey - Points', 'outcomes': [
                        {'id': 1, 'name': 'Over 15.5', 'price': 1.9}, {'id': 2, 'name': 'Under 15.5', 'price': 1.9}]}]}
                return {'events': [], 'groupLinks': [{'groupTypeCode': 'G199'}]}
        client = Client()
        result = asyncio.run(NBAPropsScraper(client, now=NOW).scrape('Betright'))
        self.assertEqual(len(result['rows']), 2)
        self.assertTrue(any('groupTypeCode=G199' in u for u in client.urls))

    def test_tab_prices_identity_and_suspension(self):
        def market(stat, **extra):
            return {'id': stat, 'name': 'NBA Uta-Por Ace Bailey', 'betOption': stat, 'bettingStatus': 'Open',
                'propositions': [{'id': stat + 'o', 'name': 'Ace Bailey Over 15.5 Pts', 'returnWin': 1.85, 'bettingStatus': 'Open'},
                                 {'id': stat + 'u', 'name': 'Ace Bailey Under 15.5 Pts', 'returnWin': 1.95, 'bettingStatus': 'Open'}], **extra}
        class Client:
            errors = []
            async def get(self, url):
                if '/matches/' in url:
                    suspended = market('Player Assists')
                    suspended['propositions'][1]['isOpen'] = False
                    team_total = market('Home Total Points O/U')
                    for p in team_total['propositions']:
                        p['name'] = p['name'].replace('Ace Bailey', 'New York')
                    return {'markets': [market('Player Points'), market('Player Rebounds', bettingStatus='Suspended'),
                        market('Player Blocks', phoneBettingOnly=True), market('1st Quarter Player Points'), suspended, team_total]}
                return {'matches': [{'id': 'NyPor', 'name': 'New York v Portland', 'competitors': ['New York', 'Portland'],
                    'startTime': '2026-10-31T19:00:00Z', '_links': {'self':
                        'https://api.beta.tab.com.au/v1/tab-info-service/sports/Basketball/competitions/NBA/matches/Utah%20v%20Portland?jurisdiction=QLD'}}]}
        scraper = NBAPropsScraper(Client(), now=NOW)
        result = asyncio.run(scraper.scrape('TAB'))
        self.assertEqual(len(result['rows']), 2)
        self.assertEqual({r['price'] for r in result['rows']}, {1.85, 1.95})
        self.assertEqual(result['rows'][0]['event_key'], 'new york knicks v portland trail blazers|2026-11-01')
        self.assertEqual(result['coverage']['eligible_pairs'], 1)
        self.assertEqual(result['coverage']['source_diagnostics']['player_ou_markets'], 1)

    def test_betdeluxe_branded_player_feed_retains_player_context(self):
        class Client:
            errors = []
            requests = []
            async def get(self, url, headers=None):
                self.requests.append((url, headers))
                if url.endswith('/competitions'):
                    return {'data': {'regions': [{'competitions': [{'id': 5952, 'name': 'NBA'}]}]}}
                if '/competitions/5952/events' in url:
                    return {'data': {'events': [{'id': 1, 'name': 'Utah Jazz v Portland Trail Blazers',
                        'advertisedStartTimeUtc': '2026-10-31T19:00:00Z'}]}}
                if url.endswith('/player-markets'):
                    return {'data': {'playerMarketGroups': [{'name': 'Points', 'isActive': True, 'players': [
                        {'name': 'Ace Bailey', 'selections': [
                            {'id': 1, 'marketId': 9, 'marketFullName': 'Player Points', 'marketName': 'Over 15.5', 'price': 1.9},
                            {'id': 2, 'marketId': 9, 'marketFullName': 'Player Points', 'marketName': 'Under 15.5', 'price': 1.9},
                            {'id': 3, 'marketId': 10, 'marketFullName': 'Player Points 20+', 'marketName': '20+', 'price': 2.5}]}]}]}}
                return {'data': {'marketGroups': []}}
        client = Client()
        result = asyncio.run(NBAPropsScraper(client, now=NOW).scrape('Betdeluxe'))
        self.assertEqual(len(result['rows']), 2)
        self.assertTrue(all(r['player'] == 'Ace Bailey' for r in result['rows']))
        self.assertEqual(result['coverage']['milestone_selections'], 1)
        self.assertTrue(all(h == {'X-NextGen-OrgId': '400'} for u, h in client.requests))

    def test_coverage_distinguishes_milestones_from_failures(self):
        client = MagicMock(errors=[], browser_requests=0)
        scraper = NBAPropsScraper(client, now=NOW)
        event = scraper.event('Betdeluxe', 1, 'Utah Jazz v Portland Trail Blazers', '2026-10-31T19:00:00Z')
        scraper.collect(event, {'id': 1, 'name': 'Player Points 20+', 'outcomes': [{'id': 1, 'name': 'Ace Bailey', 'price': 1.9}]})
        self.assertEqual(scraper.coverage()['status'], 'milestones_only')
        client.errors.append({'error': 'HTTP 403'})
        self.assertEqual(scraper.coverage()['status'], 'feed_error')

    def test_query_parameters_preserve_existing_price_query(self):
        self.assertEqual(with_query('https://example.com/price?id=1&channel=app', channel='website'),
                         'https://example.com/price?id=1&channel=website')

    def test_pickle_decimal_prices_player_identity_and_single_only_markets(self):
        scraper = NBAPropsScraper(MagicMock(errors=[]), now=NOW)
        event = scraper.event('Picklebet', 'g', 'Utah Jazz v Portland Trail Blazers', '2026-10-31T19:00:00Z')
        competitors = {'p': {'name': 'Ace Bailey', 'type': 'PLAYER'}, 't': {'name': 'Utah Jazz', 'type': 'FRANCHISE'}}
        market = {'id': 'p-pts', 'name': 'Player Points', 'status': 'ACTIVE', 'bettingType': 'Prematch',
            'allowedSelectionTypes': ['SingleOutcome'], 'specifiers': {'player': 'p', 'total': '15.5'},
            'outcomes': {'o': {'type': 'OVER', 'odds': 1.85, 'active': True},
                         'u': {'type': 'UNDER', 'odds': 1.95, 'active': True}}}
        scraper.collect_pickle_market(event, market, competitors)
        selected = select_main_lines(scraper.rows.values())
        self.assertEqual(len(selected), 2)
        self.assertEqual({r['price'] for r in selected}, {1.85, 1.95})
        self.assertTrue(all(r['player'] == 'Ace Bailey' and r['line'] == 15.5 for r in selected))
        for override in [{'status': 'SUSPENDED'}, {'bettingType': 'Live'}, {'campaignOnly': True},
                         {'allowedSelectionTypes': ['SameEventMulti']}]:
            scraper.collect_pickle_market(event, dict(market, id='excluded', **override), competitors)
        self.assertEqual(len(scraper.rows), 2)

    def test_pickle_permission_failure_cannot_be_a_complete_empty_scan(self):
        client = MagicMock(errors=[{'error': 'HTTP 403'}])
        client.query_json = AsyncMock(return_value=None)
        with self.assertRaisesRegex(ValueError, 'permission denial'):
            asyncio.run(NBAPropsScraper(client, now=NOW).scrape('Picklebet'))

    def test_missing_nba_category_cannot_retire_existing_prices(self):
        for book in ('Betr', 'Betright', 'Palmerbet'):
            client = MagicMock(errors=[])
            client.get = AsyncMock(return_value={'masterCategories': [], 'sportItem': {'subItems': []}})
            with self.subTest(book=book), self.assertRaisesRegex(ValueError, 'NBA'):
                asyncio.run(NBAPropsScraper(client, now=NOW).scrape(book))

    def test_firestore_decodes_nested_maps_arrays_and_decimal_odds(self):
        result = firestore_document({'name': 'projects/p/databases/d/documents/markets/m', 'fields': {
            'name': {'stringValue': 'Player Points'}, 'outcomes': {'mapValue': {'fields': {
                'o': {'mapValue': {'fields': {'odds': {'doubleValue': 1.85}, 'active': {'booleanValue': True}}}}}}},
            'allowedSelectionTypes': {'arrayValue': {'values': [{'stringValue': 'SingleOutcome'}]}},
            'count': {'integerValue': '2'}}})
        self.assertEqual(result['id'], 'm')
        self.assertEqual(result['outcomes']['o'], {'odds': 1.85, 'active': True})
        self.assertEqual(result['allowedSelectionTypes'], ['SingleOutcome'])
        self.assertEqual(result['count'], 2)


class BrowserFeedTests(unittest.TestCase):
    def test_http_failure_recovers_without_marking_bookmaker_failed(self):
        async def run():
            feed = FeedClient()
            feed.browser_json = AsyncMock(return_value={'events': []})
            with patch('urllib.request.urlopen', side_effect=OSError('HTTP 403')), patch('asyncio.sleep', new=AsyncMock()):
                result = await feed.get('https://web20-api.bluebet.com.au/MasterCategory?EventTypeId=107')
            self.assertEqual(result, {'events': []})
            self.assertEqual(feed.errors, [])
            feed.browser_json.assert_awaited_once()
            with patch('urllib.request.urlopen') as http:
                await feed.get('https://web20-api.bluebet.com.au/MasterEvent?MasterEventId=1')
                http.assert_not_called()
        asyncio.run(run())

    def test_warmed_betr_uses_page_fetch_with_session(self):
        async def run():
            feed = FeedClient()
            page = MagicMock(url='https://www.betr.com.au/')
            page.evaluate = AsyncMock(return_value={'status': 200, 'data': {'Events': []}})
            feed.browser_page = AsyncMock(return_value=page)
            result = await feed.browser_json('https://web20-api.bluebet.com.au/MasterEvent?MasterEventId=1')
            self.assertEqual(result, {'Events': []})
            script, args = page.evaluate.call_args.args
            self.assertIn("credentials: 'include'", script)
            self.assertIn('MasterEventId=1', args['url'])
        asyncio.run(run())


class RateLimitTests(unittest.TestCase):
    def test_retry_after_numeric_date_and_invalid_values(self):
        from email.utils import format_datetime
        self.assertEqual(retry_after_seconds('12'), 12)
        self.assertEqual(retry_after_seconds(format_datetime(NOW + timedelta(seconds=12), usegmt=True), now=NOW), 12)
        for value in (None, 'invalid', 'NaN', 'inf'):
            self.assertIsNone(retry_after_seconds(value))

    def test_http_429_waits_for_retry_after_without_browser_fallback(self):
        async def run():
            feed = FeedClient()
            feed.browser_json = AsyncMock()
            url = 'https://fixture.palmerbet.online/market'
            with patch('urllib.request.urlopen', side_effect=[HTTPError(url, 429, 'Too Many Requests', {'Retry-After': '7'}, None),
                       io.BytesIO(b'{"market": {}}')]), patch('asyncio.sleep', new=AsyncMock()) as sleep:
                result = await feed.get(url)
            self.assertEqual(result, {'market': {}})
            self.assertEqual(feed.errors, [])
            self.assertEqual(feed.rate_limit_retries, 1)
            self.assertGreater(sleep.await_args_list[0].args[0], 6.8)
            feed.browser_json.assert_not_awaited()
        asyncio.run(run())

    def test_long_retry_after_stops_host_without_repeated_requests(self):
        async def run():
            feed = FeedClient()
            feed.browser_json = AsyncMock()
            url = 'https://fixture.palmerbet.online/market'
            with patch('urllib.request.urlopen', side_effect=HTTPError(url, 429, 'Too Many Requests', {'Retry-After': '120'}, None)) as http:
                self.assertIsNone(await feed.get(url))
                self.assertIsNone(await feed.get(url + '2'))
            self.assertEqual(http.call_count, 1)
            self.assertEqual(len(feed.errors), 1)
            feed.browser_json.assert_not_awaited()
        asyncio.run(run())

    def test_persistent_429_has_bounded_retries_and_one_failure(self):
        async def run():
            feed = FeedClient()
            url = 'https://fixture.palmerbet.online/market'
            with patch('urllib.request.urlopen', side_effect=HTTPError(url, 429, 'Too Many Requests', {}, None)) as http, \
                    patch('asyncio.sleep', new=AsyncMock()):
                self.assertIsNone(await feed.get(url))
                self.assertIsNone(await feed.get(url + '2'))
            self.assertEqual(http.call_count, 4)
            self.assertEqual(feed.rate_limit_retries, 3)
            self.assertEqual(len(feed.errors), 1)
        asyncio.run(run())

    def test_browser_429_uses_same_backoff_and_keeps_successful_session(self):
        async def run():
            feed = FeedClient()
            feed._browser_hosts.add('web20-api.bluebet.com.au')
            feed.browser_json = AsyncMock(side_effect=[FeedHTTPError(429, '3'), {'Events': []}])
            with patch('urllib.request.urlopen') as http, patch('asyncio.sleep', new=AsyncMock()) as sleep:
                result = await feed.get('https://web20-api.bluebet.com.au/MasterEvent?MasterEventId=1')
            self.assertEqual(result, {'Events': []})
            self.assertEqual(feed.errors, [])
            self.assertGreater(sleep.await_args_list[0].args[0], 2.8)
            http.assert_not_called()
        asyncio.run(run())

    def test_palmerbet_requests_are_serial_and_spaced(self):
        async def run():
            feed = FeedClient(palmerbet_interval=1.5)
            url = 'https://fixture.palmerbet.online/market'
            with patch('urllib.request.urlopen', side_effect=[io.BytesIO(b'{}'), io.BytesIO(b'{}')]), \
                    patch('asyncio.sleep', new=AsyncMock()) as sleep:
                await feed.get(url)
                await feed.get(url + '2')
            self.assertGreater(sleep.await_args_list[0].args[0], 1.3)
            active = peak = 0
            async def request(*args):
                nonlocal active, peak
                active += 1
                peak = max(peak, active)
                await asyncio.sleep(0)
                active -= 1
                return {}
            feed._get = request
            await asyncio.gather(*(feed.get(url + str(i)) for i in range(4)))
            self.assertEqual(peak, 1)
        asyncio.run(run())


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
        self.assertEqual(parse_args([]).ttl_minutes, 60)
        self.assertEqual(len(parse_args([]).bookmakers), 9)
        self.assertNotIn('Picklebet', parse_args([]).bookmakers)
        self.assertEqual(parse_args(['--bookmakers', 'Picklebet']).bookmakers, ['Picklebet'])


if __name__ == '__main__':
    unittest.main()
