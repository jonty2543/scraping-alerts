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

# Fetch the nine default adapters and export quotes, coverage and errors.
# Picklebet is opt-in because its public feed currently denies access.
python3 nba_scraper.py --output nba_props.json

# Quick HTTP-only test of one upcoming event per bookmaker.
python3 nba_scraper.py --bookmakers Sportsbet Pointsbet Betright --max-events 1 --no-browser --output nba_props_smoke.json
```

The default horizon is seven days; override it with `--days`. `--max-events`
limits each bookmaker and always marks the result as incomplete. Each bookmaker
prints event/quote counts, normalized stats, errors, and whether the scan completed.
A feed or storage error gives exit status 1; configuration errors give status 2.
Zero listed props is reported as zero, separately from request failures.
The `coverage` object reports `available`, `milestones_only`, `no_eligible_pairs`,
`no_upcoming_events`, or `feed_error`, plus eligible pair counts, parsed sides,
milestone counts, browser request counts, and rate-limit retries. TAB additionally
reports `source_diagnostics.player_ou_markets` so an absence of published O/U
markets can be distinguished from parser problems. Storage failures report `storage_error`.

Palmerbet requests run one at a time, with at least one second between requests.
Its full scan can take several minutes. Increase the spacing on EC2 if needed:

```sh
bash scripts/run_nba_props_ec2.sh --palmerbet-interval 2
```

HTTP 429 responses keep the current transport and respect `Retry-After` (seconds
or an HTTP date). Without that header, retries back off for 5, 10, then 20 seconds.
The client also increases subsequent request spacing. After three recovery
attempts, or a requested delay longer than 60 seconds, the remaining requests for
that host are stopped and the scan is marked incomplete. Successful partial
prices can still be refreshed with `--write`; missing prices are not retired.
Rate limits never trigger switching to a browser, and skipped requests do not
produce a flood of repeated missing-market errors.

If the first Palmerbet request fails with zero retries, its `Retry-After` may be
longer than the 60-second in-run wait limit. The error includes the original
`retry_after` header, `wait_seconds`, and `retry_at_utc` (the earliest retry time,
not a guarantee of access). Leave Palmerbet out of runs until that time rather
than repeatedly probing it. Other bookmakers can still refresh independently:

```sh
bash scripts/run_nba_props_ec2.sh --bookmakers Pointsbet Unibet Betright Betr --write
```

### Browser feeds and EC2

betr's fallback now opens its public NBA page to establish a session, then fetches
market groups inside that page. It reuses the session instead of retrying blocked
HTTP requests for every group. TAB's public JSON feed also uses Chromium's network
stack when plain HTTP fails. No account login is needed for these public feeds.

Local checks on 21 November 2026 found TAB accessible in normal Chromium but
blocked in headless Chromium. Use `--headed-browser` locally. On a Linux server,
run that mode inside Xvfb (install the distribution's `xvfb` package and Playwright's
browser dependencies first). For the EC2 job, omit Sportsbet if that host is blocked:

```sh
python3 -m playwright install --with-deps chromium

# Dry run first; add --write after verifying the JSON coverage and errors.
xvfb-run -a python3 nba_scraper.py --headed-browser \
  --bookmakers Pointsbet Unibet Palmerbet Betright Betr TAB Betdeluxe \
  --output nba_props.json
```

Use the same virtual environment/interpreter as the scheduled job. Keep a run
interval shorter than the configured TTL and prevent overlapping runs. Browser
mode cannot guarantee access from an EC2 IP; verify coverage on that host before
switching the scheduled job. Failed and limited scans do not retire older prices.

`scripts/run_nba_props_ec2.sh` packages this command for the existing scheduler.
It uses an existing `DISPLAY`, `xvfb-run`, or a private authenticated `Xvfb` display
on Amazon Linux, prevents overlapping runs using `flock`, and writes coverage
to `.nba-run/latest.json`. A display it starts is stopped when the run exits.
It runs in dry-run mode unless `--write` is supplied:

```sh
bash scripts/run_nba_props_ec2.sh            # Validate feed access on EC2
bash scripts/run_nba_props_ec2.sh --write    # Refresh Supabase
```

On Amazon Linux 2 or 2023, install the native virtual display packages once:

```sh
sudo yum install -y xorg-x11-server-Xvfb xorg-x11-xauth util-linux
```

Amazon Linux need not provide the `xvfb-run` shell wrapper. If native Xvfb startup
fails, inspect `.nba-run/xvfb.log`. Browser installation must use the interpreter
selected by the runner (`NBA_PYTHON` or `.venv/bin/python`, falling back to `python3`).

`NBA_PYTHON`, `NBA_STATE_DIR`, and `NBA_BOOKMAKERS` override the interpreter,
output/lock directory, and space-separated bookmaker list. The script does not
install packages, change cron, or configure a new scheduler automatically.

Picklebet has an opt-in Firestore adapter (`--bookmakers Picklebet`) based on its
published NBA tournament ID and frontend schema. It decodes player identities,
decimal odds and paginated markets, and excludes live, suspended, promotional
and SGM-only markets. **Live public REST access currently returns permission
denials**, including in a fresh browser. This adapter is not enabled by default
and is not verified as a working live source. A denial is a failed scan, never a
successful empty scan. No credentials or access-control workarounds are used.

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

Query the **current** view for usable odds. Rows expire 60 minutes after fetch;
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
| Palmerbet | Discover the NBA competition first → its own match list → individual market price links. The general basketball list can omit NBA games. Requests are serial and paced, with rate-limit backoff. |
| Betright | NBA category → master events → default markets plus every advertised `groupTypeCode` |
| Unibet | NBA view → event detail + Kambi bet offers; the tested event had only game lines and no player props |
| Betdeluxe | Blackstream event and dedicated player-market endpoints, with the website's public `X-NextGen-OrgId: 400` header on every request. Milestone-only single-stat feeds still produce no eligible O/U pairs. |
| SurgeBet | Existing shared Blackstream feed, including nested outcome stacks and dedicated player markets. Its branded identity remains unverified; it is omitted from the EC2 command above. |
| Betr | BlueBet NBA master category → `MasterEvent` → each `GroupTypeCode`; handles PascalCase fields and .NET timestamps. The obsolete category/detail endpoints are not used. |
| TAB | NBA competition → full match details → propositions. Uses `returnWin` decimal prices, preserves source market/selection IDs, and maps abbreviated teams to full franchise names. |
| Picklebet (opt-in) | Firestore NBA query → paginated player/market documents. Live access currently denied; parser tests are not evidence of live availability. |

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
