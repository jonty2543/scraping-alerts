"""NBA prop feed adapters. Imports have no network, browser or database side effects."""
import hashlib
import math
import re
import time
import unicodedata
from datetime import datetime, timezone
from urllib.parse import urljoin, urlsplit, quote, urlencode, parse_qsl, urlunsplit
from zoneinfo import ZoneInfo

BRISBANE = ZoneInfo('Australia/Brisbane')
BOOKMAKERS = ('Sportsbet', 'Pointsbet', 'Unibet', 'Palmerbet', 'Betright', 'Betdeluxe', 'SurgeBet', 'Betr', 'TAB', 'Picklebet')
TARGET_STATS = {'points', 'rebounds', 'assists', 'three_pointers_made', 'steals', 'blocks'}
SB = 'https://www.sportsbet.com.au/apigw/sportsbook-sports/Sportsbook/Sports/'
PB = 'https://api.au.pointsbet.com/'
PALM = 'https://fixture.palmerbet.online'
BR = 'https://next-api.betright.com.au/Sports/'
BS = 'https://api.blackstream.com.au/api/sports/v1/'
UB = 'https://www.unibet.com.au/sportsbook-feeds/views/filter/'
TAB = 'https://api.beta.tab.com.au/v1/tab-info-service/sports/Basketball/competitions/NBA'
BETR_SITE = 'https://www.betr.com.au/sports/Basketball/107/United-States-of-America/NBA/39251'
PICKLE = 'https://firestore.googleapis.com/v1/projects/skrilla-c517f/databases/skrilla-sydney/documents'
PICKLE_NBA = '8bf64da7-214f-42c1-979e-3251a9f91f67'


def with_query(url, **params):
    parts = urlsplit(url)
    query = dict(parse_qsl(parts.query))
    query.update(params)
    return urlunsplit(parts._replace(query=urlencode(query)))


def firestore_document(document):
    """Decode Firestore's public REST wire format without altering decimal odds."""
    def value(node):
        if 'mapValue' in node:
            return {k: value(v) for k, v in node['mapValue'].get('fields', {}).items()}
        if 'arrayValue' in node:
            return [value(v) for v in node['arrayValue'].get('values', [])]
        for kind in ('stringValue', 'timestampValue', 'booleanValue', 'doubleValue', 'nullValue'):
            if kind in node:
                return node[kind]
        if 'integerValue' in node:
            return int(node['integerValue'])
        raise ValueError('Unexpected Firestore field type')
    result = {k: value(v) for k, v in document['fields'].items()}
    result['id'] = document['name'].rsplit('/', 1)[-1]
    return result


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
    for flag in ('isOpenForBetting', 'isDisplayed', 'isOpen'):
        if flag in node and str(node[flag]).lower() == 'false':
            return False
    if any(str(node.get(k, '')).lower() == 'true' for k in ('suspended', 'isSuspended', 'isScratched', 'isHidden')):
        return False
    return all(text(node.get(k)) not in {'suspended', 'closed', 'inactive', 'settled', 'resulted', 's', 'c', 'i'}
               for k in ('status', 'statusCode', 'state', 'bettingStatus'))


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
            period_number = '(?:' + word + '|' + ordinal + '|' + str(i) + ')'
            if re.search(r'\b' + period_number + r'\s*(?:quarter|qtr|q)\b|\b(?:quarter|qtr|q)\s*' + period_number + r'\b', n):
                period = 'q' + str(i)
            if i <= 2 and re.search(r'\b' + period_number + r'\s*(?:half|h)\b|\b(?:half|h)\s*' + period_number + r'\b', n):
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
    def __init__(self, browser_fallback=True, headless=True):
        import asyncio
        self.browser_fallback = browser_fallback
        self.headless = headless
        self.errors = []
        self._playwright = self._browser = self._context = None
        self._gate = asyncio.Semaphore(4)
        self._browser_lock = asyncio.Lock()
        self._pages = {}
        self._page_locks = {}
        self.browser_requests = 0
        self._browser_hosts = set()

    async def get(self, url, optional=False, headers=None):
        async with self._gate:
            return await self._get(url, optional, headers)

    async def _get(self, url, optional=False, headers=None):
        import asyncio
        from urllib.request import Request, urlopen
        def request():
            with urlopen(Request(url, headers={'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json',
                                              'Cache-Control': 'no-cache', **(headers or {})}), timeout=20) as response:
                import json
                return json.load(response)
        error = None
        host = urlsplit(url).netloc
        if host in self._browser_hosts:
            try:
                return await self.browser_json(url, headers=headers)
            except Exception as exc:
                if not optional:
                    self.errors.append({'url': url, 'error': 'Browser feed: ' + str(exc)})
                return None
        for attempt in range(2):
            try:
                return await asyncio.to_thread(request)
            except Exception as exc:
                error = exc
                if attempt == 0:
                    await asyncio.sleep(0.5)
        if self.browser_fallback:
            try:
                result = await self.browser_json(url, headers=headers)
                self._browser_hosts.add(host)
                return result
            except Exception as exc:
                error = str(error) + '; browser fallback: ' + str(exc)
        if not optional:
            self.errors.append({'url': url, 'error': str(error)})
        return None

    async def query_json(self, url, body):
        """Read-only Firestore runQuery uses POST; no auth or App Check bypass."""
        import asyncio
        import json
        from urllib.request import Request, urlopen
        def request():
            req = Request(url, data=json.dumps(body).encode(), method='POST', headers={
                'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json', 'Content-Type': 'application/json'})
            with urlopen(req, timeout=20) as response:
                return json.load(response)
        async with self._gate:
            try:
                return await asyncio.to_thread(request)
            except Exception as exc:
                self.errors.append({'url': url, 'error': str(exc)})
                return None

    async def browser_page(self, key):
        """Fresh, isolated browser; never attaches to a user's logged-in profile."""
        async with self._browser_lock:
            if self._context is None:
                from playwright.async_api import async_playwright
                self._playwright = await async_playwright().start()
                self._browser = await self._playwright.chromium.launch(headless=self.headless, channel='chromium')
                self._context = await self._browser.new_context()
            if key not in self._pages:
                self._pages[key] = await self._context.new_page()
            return self._pages[key]

    async def browser_json(self, url, headers=None):
        """Execute requests in a real page, including the site's normal session setup."""
        import asyncio
        host = urlsplit(url).netloc
        lock = self._page_locks.setdefault(host, asyncio.Lock())
        async with lock:
            page = await self.browser_page(host)
            self.browser_requests += 1
            if host == 'web20-api.bluebet.com.au':
                if page.url == 'about:blank':
                    # The NBA page makes this request itself and establishes the public session.
                    async with page.expect_response(lambda r: r.url == url and r.ok, timeout=30000) as pending:
                        await page.goto(BETR_SITE, wait_until='domcontentloaded', timeout=30000)
                    return await (await pending.value).json()
                result = await page.evaluate('''async ({url, headers}) => {
                    const response = await fetch(url, {credentials: 'include', headers,
                        signal: AbortSignal.timeout(20000)});
                    if (!response.ok) throw new Error(`HTTP ${response.status}`);
                    return await response.json();
                }''', {'url': url, 'headers': headers or {'Accept': 'application/json'}})
                return result
            # Navigating to the public JSON endpoint uses Chromium's network stack,
            # unlike APIRequestContext, which was only repeating the failed HTTP request.
            await page.set_extra_http_headers(headers or {})
            response = await page.goto(url, wait_until='domcontentloaded', timeout=30000)
            if response is None or not response.ok:
                raise ValueError('Browser feed HTTP ' + str(response.status if response else 'no response'))
            return await response.json()

    async def close(self):
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()
        self._pages.clear()


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
        root = PALM + '/fixtures/sports/b26e5acc-02ff-4b22-ae69-0491fbd2500e'
        sport = await self.client.get(root + '?sportType=Basketball&channel=website')
        self.require((sport or {}).get('sportItem', {}).get('subItems'), list, 'Palmerbet competitions')
        competitions = [c for c in sport['sportItem']['subItems'] if is_nba(c.get('title'))]
        if not competitions:
            raise ValueError('Palmerbet NBA competition not found')
        games = {}
        for comp in competitions:
            url = PALM + '/fixtures/sports/' + comp['id'] + '/matches?sportType=Basketball&pageSize=1000&channel=website'
            visited = set()
            while url:
                if url in visited:
                    raise ValueError('Palmerbet repeated pagination link')
                visited.add(url)
                feed = await self.client.get(url)
                self.require((feed or {}).get('matches'), list, 'Palmerbet NBA matches')
                games.update((g['eventId'], g) for g in feed['matches'])
                next_link = next((l['href'] for l in feed.get('_links', []) if l.get('rel') == 'next'), None)
                url = with_query(urljoin(PALM, next_link), channel='website') if next_link else None
        for game in games.values():
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
                        detail = await self.client.get(with_query(urljoin(PALM, link['href']), channel='website'))
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
        found_nba = False
        for master in field(feed or {}, 'masterCategories', []):
            for category in field(master, 'categories', []):
                if not is_nba(field(category, 'categoryName')):
                    continue
                found_nba = True
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
                        async def collect_group(group):
                            detail = await self.client.get(base + 'MasterEventEvents?masterEventId=' +
                                str(game['masterEventId']) + '&groupTypeCode=' + quote(group['groupTypeCode']))
                            self.require((detail or {}).get('events'), list, book + ' group markets')
                            self.collect(event, detail)
                        self.require(payload.get('groupLinks'), list, book + ' market groups')
                        await self.collect_many(collect_group(g) for g in payload['groupLinks'])
        if not found_nba:
            raise ValueError(book + ' NBA category not found')

    async def betright(self):
        await self.category_feed('Betright', BR)

    async def betr(self):
        base = 'https://web20-api.bluebet.com.au/'
        feed = lower_keys(await self.client.get(base + 'MasterCategory?EventTypeId=107&WithLevelledMarkets=true'))
        self.require((feed or {}).get('masterCategories'), list, 'Betr categories')
        found_nba = False
        for master in feed['masterCategories']:
            for category in master.get('categories', []):
                if not is_nba(category.get('categoryName')):
                    continue
                found_nba = True
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
        if not found_nba:
            raise ValueError('Betr NBA category not found')

    async def blackstream(self, book):
        # The branded website's public runtime config identifies BetDeluxe as org 400.
        headers = {'X-NextGen-OrgId': '400'} if book == 'Betdeluxe' else {}
        async def get(path):
            return await self.client.get(BS + path, headers=headers)
        feed = await get('sports/basketball/competitions')
        self.require((feed or {}).get('data', {}).get('regions'), list, book + ' competitions')
        ids = {c['id'] for n in walk(feed) for c in n.get('competitions', []) if is_nba(c.get('name'))}
        if not ids:
            raise ValueError(book + ' NBA competition not found')
        for comp in ids:
            base = BS + 'sports/basketball/competitions/' + str(comp) + '/events'
            events = await self.client.get(base, headers=headers)
            self.require((events or {}).get('data', {}).get('events'), list, book + ' events')
            for game in (events or {}).get('data', {}).get('events', []):
                event = self.event(book, game['id'], game['name'], game['advertisedStartTimeUtc'])
                if event:
                    self.collect(event, game)
                    detail = await get('events/' + str(game['id']) + '?includeMarkets=true')
                    self.require((detail or {}).get('data', {}).get('marketGroups'), list, book + ' event markets')
                    self.collect(event, detail)
                    players = await get('events/' + str(game['id']) + '/player-markets')
                    self.require((players or {}).get('data', {}).get('playerMarketGroups'), list, book + ' player markets')
                    for group in players['data']['playerMarketGroups']:
                        if not available(group) or group.get('isActive') is False:
                            continue
                        for player in group.get('players', []):
                            if not available(player):
                                continue
                            for selection in player.get('selections', []):
                                market = {'id': selection.get('marketId'),
                                          'name': selection.get('marketFullName') or (group['name'] + ' ' + selection.get('marketName', '')),
                                          'outcomes': [dict(selection, name=player['name'] + ' ' + selection.get('marketName', ''), playerName=player['name'])]}
                                self.collect(event, market)

    async def tab(self):
        feed = await self.client.get(TAB + '?jurisdiction=QLD&numTopMarkets=5')
        self.require((feed or {}).get('matches'), list, 'TAB NBA matches')
        # TAB's abbreviated team labels must match the full names in other feeds.
        names = {
            'Atlanta': 'Atlanta Hawks', 'Boston': 'Boston Celtics', 'Brooklyn': 'Brooklyn Nets',
            'Charlotte': 'Charlotte Hornets', 'Chicago': 'Chicago Bulls', 'Cleveland': 'Cleveland Cavaliers',
            'Dallas': 'Dallas Mavericks', 'Denver': 'Denver Nuggets', 'Detroit': 'Detroit Pistons',
            'Golden State': 'Golden State Warriors', 'Houston': 'Houston Rockets',
            'Indiana': 'Indiana Pacers', 'LA Clippers': 'Los Angeles Clippers',
            'LA Lakers': 'Los Angeles Lakers', 'Memphis': 'Memphis Grizzlies', 'Miami': 'Miami Heat',
            'Milwaukee': 'Milwaukee Bucks', 'Minnesota': 'Minnesota Timberwolves',
            'New Orleans': 'New Orleans Pelicans', 'New York': 'New York Knicks',
            'Oklahoma City': 'Oklahoma City Thunder', 'Orlando': 'Orlando Magic',
            'Philadelphia': 'Philadelphia 76ers', 'Phoenix': 'Phoenix Suns', 'Portland': 'Portland Trail Blazers',
            'Sacramento': 'Sacramento Kings', 'San Antonio': 'San Antonio Spurs', 'Toronto': 'Toronto Raptors',
            'Utah': 'Utah Jazz', 'Washington': 'Washington Wizards',
        }
        for game in feed['matches']:
            teams = [names.get(t, t) for t in game.get('competitors', [])]
            event = self.event('TAB', game['id'], game['name'], game['startTime'], teams, live=game.get('inPlay'))
            if not event:
                continue
            # Reject both shortened and full team names as possible "players".
            event['_teams'] = teams + game.get('competitors', [])
            link = game.get('_links', {}).get('self')
            if not link or urlsplit(link).netloc not in {'api.beta.tab.com.au', 'webapi.tab.com.au'}:
                raise ValueError('Missing or unexpected TAB match URL')
            detail = await self.client.get(link)
            self.require((detail or {}).get('markets'), list, 'TAB match markets')
            for market in detail['markets']:
                if (market.get('isFuture') or market.get('phoneBettingOnly') or market.get('inPlay')
                        or market.get('bettingStatus') != 'Open'):
                    continue
                option = market.get('betOption') or market['name']
                if text(option).startswith(('home total ', 'away total ', 'total ')):
                    continue
                normalized = dict(market, name=option, outcomes=[])
                for prop in market.get('propositions', []):
                    if prop.get('bettingStatus') == 'Open' and prop.get('isOpen', True):
                        normalized['outcomes'].append(dict(prop, price=prop.get('returnWin')))
                self.collect(event, normalized)

    async def picklebet(self):
        # The tournament UUID and document shapes are published by Picklebet's
        # own NBA page. This adapter stays opt-in while public REST access is denied.
        filters = [('tournamentId', PICKLE_NBA), ('eventType', 'MATCH'), ('visible', True), ('tab', 'next')]
        query = {'structuredQuery': {'from': [{'collectionId': 'bettingEvents'}],
            'where': {'compositeFilter': {'op': 'AND', 'filters': [
                {'fieldFilter': {'field': {'fieldPath': field}, 'op': 'EQUAL',
                    'value': {'booleanValue' if isinstance(val, bool) else 'stringValue': val}}}
                for field, val in filters]}}, 'limit': 1000}}
        feed = await self.client.query_json(PICKLE + ':runQuery', query)
        if feed is None:
            raise ValueError('Picklebet public Firestore access failed; a permission denial is not an empty NBA board')
        self.require(feed, list, 'Picklebet NBA query')
        if any('error' in row for row in feed):
            raise ValueError('Picklebet NBA query returned an error')
        games = [firestore_document(r['document']) for r in feed if 'document' in r]
        if len(games) >= 1000:
            raise ValueError('Picklebet query limit reached; refusing to mark a truncated scan complete')
        for game in games:
            if game.get('eventType') != 'MATCH' or not game.get('visible') or game.get('tournamentId') != PICKLE_NBA:
                continue
            competitors = game.get('competitors') or {}
            teams = [c['name'] for c in competitors.values() if c.get('type') == 'FRANCHISE']
            event = self.event('Picklebet', game['id'], game['eventName'], game['scheduledStartTime'], teams,
                               live=game.get('tab') != 'next')
            if not event:
                continue
            root = PICKLE + '/bettingEvents/' + quote(game['id'], safe='')
            if not any(c.get('type') == 'PLAYER' for c in competitors.values()):
                for player in await self.firestore_collection(root + '/competitorsV2'):
                    competitors[player['id']] = player
            markets = await self.firestore_collection(root + '/markets')
            for market in markets:
                self.collect_pickle_market(event, market, competitors)

    async def firestore_collection(self, url):
        documents, seen = [], set()
        while url:
            if url in seen:
                raise ValueError('Picklebet repeated pagination token')
            seen.add(url)
            feed = await self.client.get(with_query(url, pageSize=300))
            self.require(feed, dict, 'Picklebet collection')
            if 'documents' not in feed and set(feed) - {'nextPageToken'}:
                raise ValueError('Unexpected Picklebet collection response')
            self.require(feed.get('documents', []), list, 'Picklebet documents')
            documents.extend(firestore_document(d) for d in feed.get('documents', []))
            token = feed.get('nextPageToken')
            url = with_query(url, pageToken=token) if token else None
        return documents

    def collect_pickle_market(self, event, market, competitors):
        if (market.get('status') != 'ACTIVE' or market.get('bettingType') != 'Prematch'
                or market.get('campaignOnly') or market.get('promotional')
                or 'SingleOutcome' not in market.get('allowedSelectionTypes', [])):
            return
        specifiers = market.get('specifiers') or {}
        outcomes = []
        for identity, outcome in market.get('outcomes', {}).items():
            if outcome.get('active') is False:
                continue
            player_id = outcome.get('competitorId') or specifiers.get('player') or specifiers.get('competitor')
            player = competitors.get(player_id, {})
            label = outcome.get('name') or outcome.get('type', '').title()
            normalized = dict(outcome, id=identity, name=label, price=outcome.get('odds'))
            if player.get('type') == 'PLAYER' and not player.get('scratched'):
                normalized['playerName'] = player['name']
            elif player.get('type') in {'FRANCHISE', 'FLAG'} or player.get('scratched'):
                continue
            if str(outcome.get('type')).upper() in {'OVER', 'UNDER'}:
                normalized['name'] = outcome['type'].title() + ' ' + str(specifiers.get('total', ''))
            elif not re.search(r'\d\s*\+', label):
                thresholds = {v['criteria']['GTE'] for v in (outcome.get('winCriteria') or {}).values()
                              if isinstance(v.get('criteria'), dict) and 'GTE' in v['criteria']}
                if len(thresholds) == 1:
                    normalized['name'] = label + ' ' + str(next(iter(thresholds))) + '+'
            outcomes.append(normalized)
        self.collect(event, dict(market, outcomes=outcomes))

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
                'coverage': self.coverage(),
                'elapsed_seconds': round(time.monotonic() - started, 2)}

    def coverage(self):
        from collections import Counter
        parsed = list(self.rows.values())
        selected = select_main_lines(parsed)
        sides = Counter(r['side'] for r in parsed)
        if self.client.errors:
            status = 'feed_error'
        elif selected:
            status = 'available'
        elif not self.seen_events:
            status = 'no_upcoming_events'
        elif parsed and sides['at_least'] == len(parsed):
            status = 'milestones_only'
        else:
            status = 'no_eligible_pairs'
        return {'status': status, 'parsed_selections': len(parsed), 'eligible_pairs': len(selected) // 2,
                'milestone_selections': sides['at_least'], 'parsed_sides': dict(sides),
                'browser_requests': getattr(self.client, 'browser_requests', 0)}
