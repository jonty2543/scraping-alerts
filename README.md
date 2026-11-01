# Betting odds scrapers

The existing sport runners (`sports_alerts.py`, `nrl_scraper.py`, and
`international_scraper.py`) call the bookmaker adapters in `scrapers/`. Their
usual output is `{(match, Brisbane date): {selection: decimal odds}}`.
`functions.py::process_odds` combines bookmaker columns, matches event and
selection names, calculates best prices and market percentages, and writes
Supabase sport tables. It also records price movements in `Recent Flucs`,
prices in `Historical Odds`, and configured opening/closing/daily snapshots.
The existing SQL definitions are in `sql/`.

## NBA player props

`nba_scraper.py` is a separate runner for upcoming NBA games. Its adapters and
normalization live in `scrapers/nba_props.py`. It discovers the NBA competition,
then fetches each game's complete market details instead of using the primary
moneyline market. It excludes WNBA, futures, started games, suspended selections,
invalid prices, team totals, and player-plus-team-win combination bets.

The runner stores only **full-game points, rebounds, assists, made threes, steals
and blocks**, with one line and both over/under odds per player/stat/bookmaker.
It prefers a main market over explicitly named alternate markets. If multiple
lines remain, it chooses the complete pair with the most balanced implied
probabilities. Bookmakers can offer different main lines; their lines are retained.

Milestones (20+), combined stats, quarter/half props, doubles, first-basket bets
and incomplete one-sided markets are excluded. A bookmaker with only milestones
will return zero eligible pairs. The internal parser still recognizes those feed
shapes, but they are filtered from both exports and database writes. Availability
of steals and blocks depends on the bookmaker's published over/under markets.

### Install and run

Python 3.9+ is needed. The existing project dependencies include Supabase and
Playwright. HTTP-only dry runs use Python's standard library.

```sh
python3 -m pip install -r requirements.txt
python3 -m playwright install chromium

# Fetch all eight bookmaker adapters and export quotes, coverage and errors.
python3 nba_scraper.py --output nba_props.json

# Quick HTTP-only test of one upcoming event per bookmaker.
python3 nba_scraper.py --bookmakers Sportsbet Pointsbet Betright --max-events 1 --no-browser --output nba_props_smoke.json
```

The default horizon is seven days; override it with `--days`. `--max-events`
limits each bookmaker and always marks the result as incomplete. Each bookmaker
prints event/quote counts, normalized stats, errors, and whether the scan completed.
A feed or storage error gives exit status 1; configuration errors give status 2.
Zero listed props is reported as zero, separately from request failures.

### Supabase setup

1. Run `sql/create_nba_player_props.sql` in the Supabase SQL editor.
2. Set `SUPABASE_URL` and a backend **service role** `SUPABASE_KEY` in the
   environment or the repository's `.env` (see `.env.example`). The runner also
   reads `.env.local` and recognizes `NEXT_PUBLIC_SUPABASE_URL` and
   `SUPABASE_SERVICE_ROLE_KEY` as aliases.
3. Run:

```sh
python3 nba_scraper.py --write --output nba_props.json
```

Dry run is the default. The NBA runner loads credentials only for `--write` and
does not import `functions.py`, whose import initializes the existing database
client. No existing sport tables or alert webhooks are modified.

The new tables use one row per bookmaker selection, because the existing wide
sport tables cannot uniquely describe a player's stat, period, side, and line:

- `nba_player_props`: latest prices, source event/market/selection IDs, source
  labels, normalized player/stat/period/side/line, Brisbane game date, UTC start
  time, fetch time, expiration, and active flag.
- `nba_player_prop_snapshots`: immutable quote JSON for each run and source key.
- `nba_player_props_current`: active, unexpired prices for games yet to start.
- `nba_player_props_best`: highest price for each exact normalized selection.
- `nba_player_props_lines`: one row per selected line, with `over_odds` and
  `under_odds` together. Apply the SQL file again to add this view if already set up.

Query the **current** view for usable odds. Rows expire 30 minutes after fetch;
change that with `--ttl-minutes` to suit your existing scheduler. A complete scan
retires missing selections for that bookmaker. A failed/partial/limited scan can
refresh successful quotes but cannot retire other selections. A transactional RPC
writes both tables together, deduplicates retries, and prevents older quotes from
overwriting newer ones. Snapshot retention can be managed separately as needed.
RLS is enabled; these objects require trusted backend access by default.

Only genuine over/under selections are retained; milestone prices are never
converted into missing under odds. Exact normalized names and teams are used for
comparison, without fuzzy player matching. Source aliases such as
`Lauri Elias Markkanen` versus `Lauri Markkanen` remain separate. Source IDs and
labels are retained. A successful full scrape retires older broad-market rows.

### Bookmaker coverage

Before narrowing to the six over/under stats, live HTTP smoke checks on 31 October 2026, limited to one upcoming game per bookmaker, returned 3,343 selections: Sportsbet 873, Pointsbet 539, Palmerbet 810, Betright 148, Betr 371, and 301 each through the shared SurgeBet/Betdeluxe feed. Unibet listed no player props for the tested event. All eight requests completed without feed errors. These counts are selections, not distinct market types.

| Bookmaker | Feed and behavior |
| --- | --- |
| Sportsbet | NBA events → all event markets; hundreds of player selections per game |
| Pointsbet | NBA competition discovery → paginated events → full fixed-odds markets, including alternates |
| Palmerbet | NBA match paths → market list → individual price links, with four requests at a time |
| Betright | NBA category → master events → complete event outcomes |
| Unibet | NBA view → event detail + Kambi bet offers; the tested event had only game lines and no player props |
| SurgeBet / Betdeluxe | Both existing adapters use the same Blackstream API. The NBA runner follows that shared feed, including nested outcome stacks. These are not independently verified branded prices. |
| Betr | BlueBet NBA master category → `MasterEvent` → each `GroupTypeCode`; handles PascalCase fields and .NET timestamps. The obsolete category/detail endpoints are not used. |

Polymarket is currently integrated only for NBA moneylines in this repository;
it is not represented as a player-prop bookmaker. Browser fallback is optional
and requires installed Playwright/Chromium; HTTP errors remain visible if it
cannot retrieve the feed. No credentials are embedded in the new scraper.

### Validation

```sh
python3 -m unittest discover -s tests -p test_nba_props.py -v
```

The compact fixtures in `tests/fixtures/nba_props.json` are excerpts of public
bookmaker responses captured on 31 October 2026. Tests cover real feed shapes,
outcome stacks, Kambi scaling, milestones, periods, game/team-total rejection,
suspension, timestamps, source identities, pagination, malformed feeds and RPC
payloads. `tests/test_nba_props_storage.sql` exercises PostgreSQL ingestion,
snapshots, freshness, partial scans, old-worker protection and transaction rollback;
run it after the migration on a test database. It rolls back all test writes.
