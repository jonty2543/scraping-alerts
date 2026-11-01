-- Run after the migration, using a backend/database owner connection. Always rolls back.
begin;
create function pg_temp.nba_test_quote(source text, price_value float8, run uuid, observed timestamptz)
returns jsonb language sql as $$
select jsonb_build_object(
 'source_key', source, 'bookmaker', '__nba_test__', 'event_id', '1', 'market_id', source,
 'selection_id', '1', 'match', 'A Team v B Team', 'event_key', 'a team v b team|test',
 'starts_at', now() + interval '1 day', 'game_date', current_date + 1,
 'player', 'Test Player', 'player_key', 'test player', 'stat', 'points', 'period', 'game',
 'line', 20.5, 'side', 'over', 'price', price_value, 'source_market', 'Player Points',
 'source_selection', 'Test Player Over 20.5', 'fetched_at', observed,
 'expires_at', observed + interval '30 minutes', 'active', true, 'run_id', run)
$$;
do $$
declare
 a uuid := '00000000-0000-0000-0000-000000000001';
 b uuid := '00000000-0000-0000-0000-000000000002';
 c uuid := '00000000-0000-0000-0000-000000000003';
 q jsonb;
 observed timestamptz := now();
begin
 q := jsonb_build_array(pg_temp.nba_test_quote('__nba_test_1__', 1.9, a, observed),
                        pg_temp.nba_test_quote('__nba_test_2__', 2.1, a, observed));
 perform public.ingest_nba_player_props('__nba_test__', a, q, true);
 perform public.ingest_nba_player_props('__nba_test__', a, q, true);
 assert (select count(*) = 2 from nba_player_props where bookmaker = '__nba_test__'), 'latest dedupe';
 assert (select count(*) = 2 from nba_player_prop_snapshots where bookmaker = '__nba_test__'), 'snapshot idempotency';
 q := jsonb_build_array(pg_temp.nba_test_quote('__nba_test_1__', 2.0, b, observed + interval '1 second'));
 perform public.ingest_nba_player_props('__nba_test__', b, q, false);
 assert (select count(*) = 2 from nba_player_props_current where bookmaker = '__nba_test__'), 'partial scrape must preserve other prices';
 perform public.ingest_nba_player_props('__nba_test__', b, q, true);
 assert (select count(*) = 1 from nba_player_props_current where bookmaker = '__nba_test__'), 'complete scrape retires missing prices';
 assert (select best_price = 2.0 from nba_player_props_best where event_key = 'a team v b team|test'), 'best view';
 q := jsonb_build_array(pg_temp.nba_test_quote('__nba_test_1__', 1.5, a, observed - interval '1 minute'));
 perform public.ingest_nba_player_props('__nba_test__', a, q, false);
 assert (select price = 2.0 from nba_player_props where source_key = '__nba_test_1__'), 'older worker cannot overwrite new odds';
 begin
  perform public.ingest_nba_player_props('wrong_bookmaker', a, q, true);
  raise exception 'Expected bookmaker validation to fail';
 exception when raise_exception then
  if sqlerrm <> 'Quote bookmaker/run mismatch' then raise; end if;
 end;
 q := jsonb_build_array(pg_temp.nba_test_quote('__nba_test_invalid__', 0.5, c, observed));
 begin
  perform public.ingest_nba_player_props('__nba_test__', c, q, true);
  raise exception 'Expected invalid price to fail';
 exception when check_violation then null;
 end;
 assert not exists (select 1 from nba_player_prop_snapshots where run_id = c), 'RPC must roll back snapshots on invalid quote';
 update nba_player_props set expires_at = now() - interval '1 second' where bookmaker = '__nba_test__';
 assert not exists (select 1 from nba_player_props_current where bookmaker = '__nba_test__'), 'expired odds hidden';
end;
$$;
rollback;
