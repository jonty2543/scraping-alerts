"""Scrape upcoming NBA player props. Dry-run by default; --write enables Supabase."""
import argparse
import asyncio
import json
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from scrapers.nba_props import BOOKMAKERS, FeedClient, NBAPropsScraper, select_main_lines


def load_env():
    # Match the existing repository's SUPABASE_URL / SUPABASE_KEY configuration.
    root = Path(__file__).resolve().parent
    for path in (root / '.env', root / '.env.local'):
        if not path.exists():
            continue
        for line in path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                os.environ.setdefault(key.strip(), value.strip().strip('\"\''))
    for target, alias in (('SUPABASE_URL', 'NEXT_PUBLIC_SUPABASE_URL'),
                          ('SUPABASE_KEY', 'SUPABASE_SERVICE_ROLE_KEY')):
        if not os.getenv(target) and os.getenv(alias):
            os.environ[target] = os.environ[alias]


def store_result(client, result, run_id, ttl_minutes=30):
    """One transactional RPC per bookmaker: latest prices + immutable snapshots."""
    rows = []
    for quote in select_main_lines(result['rows']):
        row = dict(quote)
        row['run_id'] = run_id
        row['active'] = True
        row['expires_at'] = (datetime.fromisoformat(row['fetched_at']) + timedelta(minutes=ttl_minutes)).isoformat()
        rows.append(row)
    # A partial scrape may refresh the quotes it did fetch, but cannot retire others.
    return client.rpc('ingest_nba_player_props', {'p_bookmaker': result['bookmaker'],
                      'p_run_id': run_id, 'p_quotes': rows, 'p_complete': result['complete']}).execute()


async def run(args):
    client = None
    if args.write:
        load_env()
        if not os.getenv('SUPABASE_URL') or not os.getenv('SUPABASE_KEY'):
            raise ValueError('--write requires SUPABASE_URL and SUPABASE_KEY (environment or .env)')
        from supabase import create_client
        client = create_client(os.environ['SUPABASE_URL'], os.environ['SUPABASE_KEY'])
    run_id = str(uuid.uuid4())
    results = []
    # Bookmakers and events are independent; sequential execution keeps feed load bounded.
    for book in args.bookmakers:
        feed = FeedClient(browser_fallback=not args.no_browser)
        scraper = NBAPropsScraper(feed, days=args.days, max_events=args.max_events)
        try:
            result = await scraper.scrape(book)
        except Exception as exc:
            result = {'bookmaker': book, 'rows': select_main_lines(scraper.rows.values()),
                      'events': scraper.seen_events, 'complete': False,
                      'errors': feed.errors + [{'error': str(exc)}]}
        finally:
            await feed.close()
        if client is not None:
            try:
                store_result(client, result, run_id, args.ttl_minutes)
            except Exception as exc:
                result['errors'].append({'storage_error': str(exc)})
                result['complete'] = False
        results.append(result)
        print(json.dumps({k: v for k, v in result.items() if k != 'rows'} | {
            'quotes': len(result['rows']), 'stats': sorted({r['stat'] for r in result['rows']})}), flush=True)
    if args.output:
        Path(args.output).write_text(json.dumps({'run_id': run_id, 'results': results}, indent=2, allow_nan=False) + '\n')
    return 1 if any(r['errors'] for r in results) else 0


def positive_int(value):
    result = int(value)
    if result <= 0:
        raise argparse.ArgumentTypeError('must be positive')
    return result


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bookmakers', nargs='+', choices=BOOKMAKERS, default=list(BOOKMAKERS))
    parser.add_argument('--days', type=positive_int, default=7, help='Upcoming horizon, default 7 days')
    parser.add_argument('--max-events', type=positive_int, help='Limit each bookmaker for smoke tests; never retires older quotes')
    parser.add_argument('--no-browser', action='store_true', help='Use HTTP only')
    parser.add_argument('--write', action='store_true', help='Persist using the SQL migration and Supabase credentials')
    parser.add_argument('--ttl-minutes', type=positive_int, default=30)
    parser.add_argument('--output', help='Write full quote data and coverage/errors as JSON')
    return parser.parse_args(argv)


if __name__ == '__main__':
    try:
        sys.exit(asyncio.run(run(parse_args())))
    except (ValueError, ImportError) as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(2)
