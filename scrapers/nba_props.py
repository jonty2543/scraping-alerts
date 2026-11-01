"""NBA prop feed adapters. Imports have no network, browser or database side effects."""
import hashlib
import math
import re
import time
import unicodedata
from datetime import datetime, timezone
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

BRISBANE = ZoneInfo('Australia/Brisbane')
BOOKMAKERS = ('Sportsbet', 'Pointsbet', 'Unibet', 'Palmerbet', 'Betright', 'Betdeluxe', 'SurgeBet', 'Betr')
TARGET_STATS = {'points', 'rebounds', 'assists', 'three_pointers_made', 'steals', 'blocks'}
SB = 'https://www.sportsbet.com.au/apigw/sportsbook-sports/Sportsbook/Sports/'
PB = 'https://api.au.pointsbet.com/'
PALM = 'https://fixture.palmerbet.online'
BR = 'https://next-api.betright.com.au/Sports/'
BS = 'https://api.blackstream.com.au/api/sports/v1/'
UB = 'https://www.unibet.com.au/sportsbook-feeds/views/filter/'


def text(value):
    return re.sub(r'\s+', ' ', unicodedata.normalize('NFKD', str(value or '')).encode('ascii', 'ignore').decode().lower()).strip()


def walk(node):
    if isinstance(node, dict):
        yield node
        for value in node.values():
            yield from walk(value)
    elif isinstance(node, list):
        for value in node:
            yield from walk(value)


def lower_keys(value):
    """BlueBet uses PascalCase for the same event/outcome fields."""
    if isinstance(value, dict):
        return {k[0].lower() + k[1:]: lower_keys(v) for k, v in value.items()}
    return [lower_keys(v) for v in value] if isinstance(value, list) else value


def number(value):
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (ValueError, TypeError):
        return None


def timestamp(value):
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value / 1000 if value > 1e12 else value, timezone.utc)
    iso = str(value).replace('Z', '+00:00')
    # .NET feeds use seven fractional digits; Python 3.9 accepts three or six.
    iso = re.sub(r'(T\d{2}:\d{2}:\d{2})\.(\d+)',
                 lambda m: m.group(1) + '.' + (m.group(2) + '000000')[:6], iso)
    result = datetime.fromisoformat(iso)
    if result.tzinfo is None:
        raise ValueError('Start time must include a timezone')
    return result.astimezone(timezone.utc)


def is_nba(name):
    return text(name) in {'nba', 'usa nba', 'united states nba', 'national basketball association', 'us nba'}


def available(node):
    for flag in ('isOpenForBetting', 'isDisplayed'):
        if flag in node and str(node[flag]).lower() == 'false':
            return False
    if any(str(node.get(k, '')).lower() == 'true' for k in ('suspended', 'isSuspended', 'isScratched', 'isHidden')):
        return False
    return all(text(node.get(k)) not in {'suspended', 'closed', 'inactive', 'settled', 'resulted', 's', 'c', 'i'}
               for k in ('status', 'statusCode', 'state'))


def market_name(market):
    criterion = market.get('criterion') or {}
    return str(market.get('eventName') or market.get('title') or market.get('name')
               or criterion.get('englishLabel') or criterion.get('label') or '')


def classify(name):
    """Return a stat and period without collapsing distinct settlement conditions."""
    n = text(name).replace('-', ' ')
    if 'first team basket' in n:
        return 'first_team_basket', 'game'
    if 'first basket' in n or 'first field goal' in n:
        return 'first_basket', 'game'
    if 'to score in the first' in n:
        stat = 'points'
    elif any(w in n for w in ('fantasy', 'performance')):
        stat = 'fantasy_points'
    elif 'triple double' in n:
        stat = 'triple_double'
    elif 'double double' in n:
        stat = 'double_double'
    else:
        patterns = [('points', r'\b(?:points?|pts)\b'), ('rebounds', r'\b(?:rebounds?|rebs?|reb)\b'),
                    ('assists', r'\b(?:assists?|asts?|ast)\b'), ('steals', r'\b(?:steals?|stls?)\b'),
                    ('blocks', r'\b(?:blocks?|blks?)\b'), ('turnovers', r'\b(?:turnovers?|tos)\b')]
        stats = [s for s, p in patterns if re.search(p, n)]
        if re.search(r'\b(?:3 pointers?|three pointers?|threes|3s|3pt|3pm)\b', n):
            stat = 'three_pointers_made'
        elif 'free throw' in n:
            stat = 'free_throws_made'
        elif 'field goal' in n:
            stat = 'field_goals_made'
        elif re.search(r'\bpra\b', n):
            stat = 'points_rebounds_assists'
        else:
            stat = '_'.join(stats) if stats else None
    if not stat:
        return None, None
    # These are separate markets, never ordinary full-game counting props.
    qualifier = ''
    for token, suffix in [('first basket', 'first_basket'), ('first scorer', 'first_scorer'),
                          ('top ', 'top'), ('most ', 'most'), ('exactly', 'exact'),
                          ('odd/even', 'odd_even'), ('odd even', 'odd_even')]:
        if token in n:
            qualifier = '_' + suffix
            break
    if re.search(r'\b(?:first|1st)\s+\d+\s+minutes?\b', n):
        period = 'first_' + re.search(r'\b(?:first|1st)\s+(\d+)\s+minutes?\b', n).group(1) + '_minutes'
    else:
        period = 'game'
        for i, word in enumerate(('first', 'second', 'third', 'fourth'), 1):
            ordinal = ('1st', '2nd', '3rd', '4th')[i - 1]
            if re.search(r'\b(?:' + word + '|' + ordinal + r')\s+(?:quarter|qtr)\b|\bq' + str(i) + r'\b', n):
                period = 'q' + str(i)
            if i <= 2 and re.search(r'\b(?:' + word + '|' + ordinal + r')\s+half\b', n):
                period = 'h' + str(i)
    if 'excluding overtime' in n or 'regulation' in n:
        period += '_regulation'
    if any(w in n for w in ('attempts', 'attempted')):
        stat = stat.replace('_made', '') + '_attempted'
    return stat + qualifier, period


def clean_player(value):
    value = re.sub(r'\s*\([A-Z]{2,4}\)\s*$', '', str(value)).strip(' -:')
    return re.sub(r'\s+', ' ', value)


def select_main_lines(rows):
    """Keep a complete O/U pair at one full-game line per bookmaker/player/stat."""
    groups = {}
    for row in rows:
        if (row['stat'] not in TARGET_STATS or row['period'] != 'game'
                or row['side'] not in {'over', 'under'} or row['line'] is None
                or re.search(r'\b(?:and|or|team)\b', row['player_key'])):
            continue
        key = tuple(row[k] for k in ('bookmaker', 'event_id', 'player_key', 'stat', 'period'))
        candidate = groups.setdefault(key, {}).setdefault((row['line'], row['market_id']), {})
        previous = candidate.get(row['side'])
        if previous is None or row['fetched_at'] > previous['fetched_at']:
            candidate[row['side']] = row
    selected = []
    for candidates in groups.values():
        pairs = [sides for sides in candidates.values() if {'over', 'under'} <= sides.keys()]
        if not pairs:
            continue
        def rank(pair):
            over, under = pair['over'], pair['under']
            alternate = any(re.search(r'\b(?:alternate|alternative)\b', text(r['source_market']))
                            for r in (over, under))
            return (alternate, abs(1 / over['price'] - 1 / under['price']),
                    over['line'], over['market_id'])
        pair = min(pairs, key=rank)
        selected.extend((pair['over'], pair['under']))
    return selected


def parse_selection(market, outcome, teams=()):
    name = market_name(market)
    stat, period = classify(name)
    if not stat or not available(market) or not available(outcome):
        return None
    if any(w in text(name) for w in ('team to win', 'team win', 'winning margin', 'winner /', 'line /', ' & ', ' and win', 'money line and', 'moneyline and')):
        return None
    label = str(outcome.get('outcomeName') or outcome.get('title') or outcome.get('name')
                or outcome.get('englishLabel') or outcome.get('label') or '')
    player = outcome.get('playerName') or outcome.get('participant') or market.get('playerName')
    if isinstance(player, dict):
        player = player.get('name')
    side_match = re.search(r'\b(over|under|yes|no|odd|even)\b', label, re.I)
    side = side_match.group(1).lower() if side_match else None
    # Explicit text takes precedence: some feeds express 10+ as points=9.5.
    threshold = re.search(r'(\d+(?:\.\d+)?)\s*\+', label) or re.search(r'(\d+(?:\.\d+)?)\s*\+', name)
    line = None
    if threshold:
        side, line = 'at_least', float(threshold.group(1))
    elif side in {'over', 'under'}:
        numeric = re.search(r'\b(?:over|under)\s*([-+]?\d+(?:\.\d+)?)', label, re.I)
        if numeric:
            line = float(numeric.group(1))
        else:
            for source in (outcome, market):
                for key in ('unformattedHandicap', 'handicap', 'points', 'line', 'value'):
                    if source.get(key) is not None:
                        line = number(source[key])
                        if line is not None:
                            # Kambi integer line values are scaled by 1000.
                            if key == 'line' and 'criterion' in market:
                                line /= 1000
                            break
                if line is not None:
                    break
    if not player:
        candidate = re.split(r'\b(?:over|under|to get|to score|to record|yes|no|odd|even)\b|\d+(?:\.\d+)?\s*\+', label, maxsplit=1, flags=re.I)[0]
        candidate = clean_player(candidate)
        if len(candidate.split()) >= 2:
            player = candidate
    if not player:
        # Player name may appear before or after the stat in the market title.
        for part in re.split(r'\s+[-:]\s+', name):
            if classify(part)[0] is None and len(clean_player(part).split()) >= 2:
                player = clean_player(part)
                break
    if not player:
        return None
    player = clean_player(player)
    p = text(player)
    if p in {text(t) for t in teams} or any(word in p for word in ('total', 'player', 'quarter', 'half', 'other', 'no scorer', 'overtime', 'regulation', 'including', 'excluding')):
        return None
    # Never treat game/team totals or combo bets as player props.
    if len(player.split()) < 2 or re.search(r'\d|\s[&/]\s', player) or re.search(r'\b(?:and|or|team)\b', p):
        return None
    if side is None and stat in {'double_double', 'triple_double'}:
        side = 'yes'
    if side is None and (stat in {'first_basket', 'first_team_basket'} or stat.endswith(('_top', '_most'))):
        side = 'win'
    if side is None and 'to score in the first' in text(name):
        side, line = 'at_least', 1.0
        stat = 'points'
    if side is None:
        return None
    if side in {'over', 'under', 'at_least'} and (line is None or line < 0):
        return None
    price = outcome.get('price')
    if isinstance(price, dict):
        price = price.get('winPrice')
    if price is None:
        price = outcome.get('oddsDecimal')
    if price is None and outcome.get('odds') is not None:
        odds = number(outcome['odds'])
        price = odds / 1000 if odds is not None else None
    if price is None:
        for snapshot in outcome.get('prices', []):
            if snapshot.get('name', 'Fixed') == 'Fixed':
                price = (snapshot.get('priceSnapshot') or {}).get('current')
                break
    price = number(price)
    if price is None or price <= 1:
        return None
    return {'player': player, 'player_key': text(player), 'stat': stat, 'period': period,
            'line': line, 'side': side, 'price': price, 'source_market': name, 'source_selection': label}


class FeedClient:
    """Bounded HTTP retries plus optional browser fallback, with visible failures."""
    def __init__(self, browser_fallback=True):
        import asyncio
        self.browser_fallback = browser_fallback
        self.errors = []
        self._playwright = self._browser = self._context = None
        self._gate = asyncio.Semaphore(4)
        self._browser_lock = asyncio.Lock()

    async def get(self, url, optional=False):
        async with self._gate:
            return await self._get(url, optional)

    async def _get(self, url, optional=False):
        import asyncio
        from urllib.request import Request, urlopen
        def request():
            with urlopen(Request(url, headers={'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json',
                                              'Cache-Control': 'no-cache'}), timeout=20) as response:
                import json
                return json.load(response)
        error = None
        for attempt in range(2):
            try:
                return await asyncio.to_thread(request)
            except Exception as exc:
                error = exc
                if attempt == 0:
                    await asyncio.sleep(0.5)
        if self.browser_fallback:
            try:
                async with self._browser_lock:
                    if self._context is None:
                        from playwright.async_api import async_playwright
                        self._playwright = await async_playwright().start()
                        self._browser = await self._playwright.chromium.launch(headless=True)
                        self._context = await self._browser.new_context(user_agent='Mozilla/5.0')
                response = await self._context.request.get(url, timeout=20000)
                if response.ok:
                    return await response.json()
            except Exception as exc:
                error = str(error) + '; browser fallback: ' + str(exc)
        if not optional:
            self.errors.append({'url': url, 'error': str(error)})
        return None

    async def close(self):
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()


class NBAPropsScraper:
    def __init__(self, client, now=None, days=7, max_events=None):
        self.client = client
        self.now = now or datetime.now(timezone.utc)
        self.days = days
        self.max_events = max_events
        self.seen_events = 0
        self.rows = {}

    def event(self, book, event_id, name, start, teams=(), live=False):
        from datetime import timedelta
        if str(live).lower() == 'true':
            return None
        try:
            starts_at = timestamp(start)
        except (ValueError, TypeError, OverflowError):
            if self.client is not None:
                self.client.errors.append({'event_id': str(event_id), 'error': 'Invalid event start time: ' + str(start)})
            return None
        if not self.now < starts_at <= self.now + timedelta(days=self.days):
            return None
        if self.max_events is not None and self.seen_events >= self.max_events:
            return None
        self.seen_events += 1
        if not teams:
            teams = re.split(r'\s+(?:v(?:s)?\.?|at|@|-)\s+', name, flags=re.I)
        if len(teams) != 2:
            return None
        canonical_match = ' v '.join(sorted(text(t) for t in teams))
        return {'bookmaker': book, 'event_id': str(event_id), 'match': name,
                'event_key': canonical_match + '|' + starts_at.astimezone(BRISBANE).date().isoformat(),
                'starts_at': starts_at.isoformat(), 'game_date': starts_at.astimezone(BRISBANE).date().isoformat(),
                '_teams': teams}

    def collect(self, event, payload):
        if event is None:
            return
        for market in walk(payload):
            outcomes = list(market.get('outcomes') or market.get('selections') or [])
            for value in (market.get('outcomeStack') or {}).values():
                if isinstance(value, list):
                    outcomes.extend(value)
            for outcome in outcomes:
                prop = parse_selection(market, outcome, event['_teams'])
                if prop is None:
                    continue
                row = {k: v for k, v in event.items() if not k.startswith('_')}
                row.update(prop)
                row['market_id'] = str(market.get('id') or market.get('key') or market.get('eventId') or market_name(market))
                row['selection_id'] = str(outcome.get('id') or outcome.get('key') or outcome.get('outcomeId') or prop['source_selection'])
                identity = [row[k] for k in ('bookmaker', 'event_id', 'market_id', 'selection_id')]
                row['source_key'] = hashlib.sha256('\x1f'.join(identity).encode()).hexdigest()
                row['fetched_at'] = datetime.now(timezone.utc).isoformat()
                self.rows[row['source_key']] = row

    async def sportsbet(self):
        from datetime import timedelta
        url = (SB + 'Events?primaryMarketOnly=true&sportsId=16&numEventsPerClass=2000&detailsLevel=O'
               + '&fromDate=' + self.now.astimezone(BRISBANE).date().isoformat() + 'T00:00:00'
               + '&toDate=' + (self.now + timedelta(days=self.days)).astimezone(BRISBANE).date().isoformat() + 'T23:59:59')
        data = await self.client.get(url)
        self.require(data, list, 'Sportsbet events')
        for game in data or []:
            if not is_nba(game.get('competitionName')) or game.get('eventSort') != 'MTCH':
                continue
            event = self.event('Sportsbet', game['id'], game.get('displayName') or game['name'], game['startTime'], live=game.get('hasBIRStarted'))
            if event:
                markets = await self.client.get(SB + 'Events/' + str(game['id']) + '/Markets')
                self.require(markets, list, 'Sportsbet markets')
                self.collect(event, markets)

    async def pointsbet(self):
        data = await self.client.get(PB + 'api/v2/sports/basketball/competitions')
        self.require((data or {}).get('locales'), list, 'Pointsbet competitions')
        ids = {c['key'] for n in walk(data) for c in n.get('competitions', []) if is_nba(c.get('name'))}
        if not ids:
            raise ValueError('Pointsbet NBA competition not found')
        for comp in ids:
            url = PB + 'api/mes/v3/events/featured/competition/' + str(comp) + '?page=1'
            visited = set()
            while url and url not in visited:
                visited.add(url)
                feed = await self.client.get(url)
                self.require((feed or {}).get('events'), list, 'Pointsbet events')
                for game in (feed or {}).get('events', []):
                    event = self.event('Pointsbet', game['key'], game['name'], game['startsAt'], live=game.get('isLive'))
                    if event:
                        detail = await self.client.get(PB + 'api/mes/v3/events/' + str(game['key']))
                        self.require((detail or {}).get('fixedOddsMarkets'), list, 'Pointsbet event markets')
                        self.collect(event, detail)
                next_page = (feed or {}).get('nextPage')
                if isinstance(next_page, int):
                    url = PB + 'api/mes/v3/events/featured/competition/' + str(comp) + '?page=' + str(next_page)
                elif isinstance(next_page, str):
                    url = urljoin(url, next_page)
                else:
                    url = None

    async def palmerbet(self):
        url = PALM + '/fixtures/sports/b26e5acc-02ff-4b22-ae69-0491fbd2500e/matches?sportType=basketball&pageSize=1000&channel=website'
        feed = await self.client.get(url)
        self.require((feed or {}).get('matches'), list, 'Palmerbet matches')
        for game in (feed or {}).get('matches', []):
            if not any(is_nba(p.get('title')) for p in game.get('paths', [])) or game.get('status') != 'NotStarted':
                continue
            teams = [game['homeTeam']['title'], game['awayTeam']['title']]
            event = self.event('Palmerbet', game['eventId'], ' v '.join(teams), game['startTime'], teams)
            if not event:
                continue
            markets = await self.client.get(PALM + '/fixtures/sports/matches/' + game['eventId'] + '/markets?sportType=Basketball&pageSize=1000&channel=website')
            self.require((markets or {}).get('markets'), list, 'Palmerbet markets')
            async def collect_market(market):
                stat, period = classify(market_name(market))
                if (stat not in TARGET_STATS or period != 'game' or not available(market)
                        or re.search(r'\d\s*\+', market_name(market))):
                    return
                if market.get('outcomes'):
                    self.collect(event, market)
                    return
                for link in market.get('_links', []):
                    if link.get('rel') == 'price' and link.get('method', 'GET') == 'GET':
                        detail = await self.client.get(urljoin(PALM, link['href']) + '&channel=website')
                        self.require((detail or {}).get('market', {}).get('outcomes'), list, 'Palmerbet market prices')
                        self.collect(event, detail)
                        break
            # The HTTP client limits concurrency to four requests.
            await self.collect_many(collect_market(m) for m in (markets or {}).get('markets', []))

    async def category_feed(self, book, base):
        def field(node, name, default=None):
            return node.get(name, default)
        feed = await self.client.get(base + 'MasterCategory?eventTypeId=107')
        self.require(field(feed or {}, 'masterCategories'), list, book + ' categories')
        for master in field(feed or {}, 'masterCategories', []):
            for category in field(master, 'categories', []):
                if not is_nba(field(category, 'categoryName')):
                    continue
                detail = await self.client.get(base + 'Category?categoryId=' + str(field(category, 'categoryId')))
                self.require(field(detail or {}, 'masterCategories'), list, book + ' league events')
                for node in walk(detail):
                    for game in field(node, 'masterEvents', []):
                        event = self.event(book, field(game, 'masterEventId'), field(game, 'masterEventName'), field(game, 'maxAdvertisedStartTimeUtc'))
                        if not event:
                            continue
                        payload = await self.client.get(base + 'MasterEventEvents?masterEventId=' + str(field(game, 'masterEventId')))
                        self.require(field(payload or {}, 'events'), list, book + ' event markets')
                        self.collect(event, payload)

    async def betright(self):
        await self.category_feed('Betright', BR)

    async def betr(self):
        base = 'https://web20-api.bluebet.com.au/'
        feed = lower_keys(await self.client.get(base + 'MasterCategory?EventTypeId=107&WithLevelledMarkets=true'))
        self.require((feed or {}).get('masterCategories'), list, 'Betr categories')
        for master in feed['masterCategories']:
            for category in master.get('categories', []):
                if not is_nba(category.get('categoryName')):
                    continue
                for game in category.get('masterEvents', []):
                    event = self.event('Betr', game['masterEventId'], game['masterEventName'], game['maxAdvertisedStartTime'])
                    if not event:
                        continue
                    url = base + 'MasterEvent?MasterEventId=' + str(game['masterEventId'])
                    detail = lower_keys(await self.client.get(url))
                    self.require((detail or {}).get('events'), list, 'Betr event markets')
                    self.collect(event, detail)
                    async def collect_group(group):
                        payload = lower_keys(await self.client.get(url + '&GroupTypeCode=' + group['groupTypeCode']))
                        self.require((payload or {}).get('events'), list, 'Betr group markets')
                        self.collect(event, payload)
                    self.require(detail.get('groupLinks'), list, 'Betr market groups')
                    await self.collect_many(collect_group(g) for g in detail['groupLinks'])

    async def blackstream(self, book):
        feed = await self.client.get(BS + 'sports/basketball/competitions')
        self.require((feed or {}).get('data', {}).get('regions'), list, book + ' competitions')
        ids = {c['id'] for n in walk(feed) for c in n.get('competitions', []) if is_nba(c.get('name'))}
        if not ids:
            raise ValueError(book + ' NBA competition not found')
        for comp in ids:
            base = BS + 'sports/basketball/competitions/' + str(comp) + '/events'
            events = await self.client.get(base)
            self.require((events or {}).get('data', {}).get('events'), list, book + ' events')
            for game in (events or {}).get('data', {}).get('events', []):
                event = self.event(book, game['id'], game['name'], game['advertisedStartTimeUtc'])
                if event:
                    self.collect(event, game)
                    detail = await self.client.get(BS + 'events/' + str(game['id']) + '?includeMarkets=true')
                    self.require((detail or {}).get('data', {}).get('marketGroups'), list, book + ' event markets')
                    self.collect(event, detail)

    async def unibet(self):
        feed = await self.client.get(UB + 'basketball/nba/matches?includeParticipants=true&useCombined=true')
        self.require((feed or {}).get('layout'), dict, 'Unibet layout')
        seen = set()
        for node in walk(feed):
            game = node.get('event')
            if not isinstance(game, dict) or game.get('id') in seen:
                continue
            if not (is_nba(game.get('group')) or any(is_nba(p.get('englishName') or p.get('name')) for p in game.get('path', []))):
                continue
            seen.add(game['id'])
            if game.get('state') != 'NOT_STARTED':
                continue
            event = self.event('Unibet', game['id'], game.get('englishName') or game['name'], game['start'], [game['homeName'], game['awayName']])
            if not event:
                continue
            self.collect(event, node)
            self.collect(event, await self.client.get(UB + 'basketball/nba/matches/' + str(game['id']) + '?includeParticipants=true&useCombined=true'))
            offers = await self.client.get('https://eu-offering-api.kambicdn.com/offering/v2018/ub/betoffer/event/' + str(game['id']) + '.json?lang=en_AU&market=AU&client_id=2&channel_id=1')
            self.require((offers or {}).get('betOffers'), list, 'Unibet Kambi offers')
            self.collect(event, offers)

    @staticmethod
    def require(value, expected, context):
        if not isinstance(value, expected):
            raise ValueError('Missing or unexpected ' + context + ' feed structure')

    async def collect_many(self, coroutines):
        import asyncio
        # Finish all in-flight requests before reporting partial coverage or closing.
        for result in await asyncio.gather(*coroutines, return_exceptions=True):
            if isinstance(result, Exception):
                self.client.errors.append({'error': str(result)})

    async def scrape(self, bookmaker):
        started = time.monotonic()
        if bookmaker in {'SurgeBet', 'Betdeluxe'}:
            await self.blackstream(bookmaker)
        else:
            await getattr(self, bookmaker.lower())()
        return {'bookmaker': bookmaker, 'rows': select_main_lines(self.rows.values()), 'errors': list(self.client.errors),
                'events': self.seen_events, 'complete': not self.client.errors and self.max_events is None,
                'elapsed_seconds': round(time.monotonic() - started, 2)}
