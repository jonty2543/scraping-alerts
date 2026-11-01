-- Run in the Supabase SQL editor before using nba_scraper.py --write.
create table if not exists public.nba_player_props (
    source_key text primary key,
    bookmaker text not null,
    event_id text not null,
    market_id text not null,
    selection_id text not null,
    match text not null,
    event_key text not null,
    starts_at timestamptz not null,
    game_date date not null,
    player text not null,
    player_key text not null,
    stat text not null,
    period text not null,
    line double precision,
    side text not null check (side in ('over', 'under', 'at_least', 'yes', 'no', 'odd', 'even', 'win')),
    price double precision not null check (price > 1 and price < 'Infinity'::float8),
    source_market text not null,
    source_selection text not null,
    fetched_at timestamptz not null,
    expires_at timestamptz not null,
    active boolean not null default true,
    run_id uuid not null,
    check (line is null or (line >= 0 and line < 'Infinity'::float8)),
    check (side not in ('over', 'under', 'at_least') or line is not null),
    unique (bookmaker, event_id, market_id, selection_id)
);
create index if not exists nba_props_event_idx on public.nba_player_props (event_key, player_key, stat, period);
create index if not exists nba_props_freshness_idx on public.nba_player_props (bookmaker, active, expires_at);

create table if not exists public.nba_player_prop_snapshots (
    run_id uuid not null,
    source_key text not null,
    bookmaker text not null,
    fetched_at timestamptz not null,
    quote jsonb not null,
    primary key (run_id, source_key)
);
create index if not exists nba_prop_snapshots_time_idx on public.nba_player_prop_snapshots (bookmaker, fetched_at);

-- Invoker privileges and RLS restrict writes to trusted backend credentials.
alter table public.nba_player_props enable row level security;
alter table public.nba_player_prop_snapshots enable row level security;

create or replace function public.ingest_nba_player_props(
    p_bookmaker text, p_run_id uuid, p_quotes jsonb, p_complete boolean default false
) returns integer language plpgsql security invoker set search_path = public as $$
declare n integer;
begin
    if p_quotes is null or jsonb_typeof(p_quotes) <> 'array' then
        raise exception 'p_quotes must be an array';
    end if;
    if exists (select 1 from jsonb_array_elements(p_quotes) q
               where q->>'bookmaker' is distinct from p_bookmaker
                  or (q->>'run_id')::uuid is distinct from p_run_id) then
        raise exception 'Quote bookmaker/run mismatch';
    end if;
    -- Serialize overlapping workers for the same bookmaker.
    perform pg_advisory_xact_lock(hashtextextended('nba_props:' || p_bookmaker, 0));
    insert into public.nba_player_prop_snapshots (run_id, source_key, bookmaker, fetched_at, quote)
        select p_run_id, q->>'source_key', p_bookmaker, (q->>'fetched_at')::timestamptz, q
        from jsonb_array_elements(p_quotes) q
        on conflict (run_id, source_key) do nothing;
    insert into public.nba_player_props
        select * from jsonb_populate_recordset(null::public.nba_player_props, p_quotes)
        on conflict (source_key) do update set
            price = excluded.price,
            fetched_at = excluded.fetched_at,
            expires_at = excluded.expires_at,
            active = true,
            run_id = excluded.run_id,
            starts_at = excluded.starts_at,
            game_date = excluded.game_date,
            event_key = excluded.event_key,
            match = excluded.match,
            player = excluded.player,
            player_key = excluded.player_key,
            stat = excluded.stat,
            period = excluded.period,
            line = excluded.line,
            side = excluded.side,
            source_market = excluded.source_market,
            source_selection = excluded.source_selection
        where excluded.fetched_at >= nba_player_props.fetched_at;
    get diagnostics n = row_count;
    if p_complete then
        update public.nba_player_props set active = false
        where bookmaker = p_bookmaker and run_id <> p_run_id
          and fetched_at <= coalesce((select max((q->>'fetched_at')::timestamptz)
                                     from jsonb_array_elements(p_quotes) q), now());
    end if;
    return n;
end;
$$;
revoke all on function public.ingest_nba_player_props(text, uuid, jsonb, boolean) from public, anon, authenticated;
grant execute on function public.ingest_nba_player_props(text, uuid, jsonb, boolean) to service_role;
grant select, insert, update on public.nba_player_props to service_role;
grant select, insert on public.nba_player_prop_snapshots to service_role;

-- Query this view for usable prices; suspended, missing, expired or started bets disappear.
create or replace view public.nba_player_props_current with (security_invoker = true) as
select * from public.nba_player_props
where active and expires_at > now() and starts_at > now();

-- Exact names only: deliberately no fuzzy player matching or conversion of N+ into O/U.
create or replace view public.nba_player_props_best with (security_invoker = true) as
select distinct on (event_key, player_key, stat, period, line, side)
    event_key, game_date, player_key, player, stat, period, line, side,
    price as best_price, bookmaker as best_bookie, fetched_at
from public.nba_player_props_current
order by event_key, player_key, stat, period, line, side, price desc, fetched_at desc;

grant select on public.nba_player_props_current, public.nba_player_props_best to service_role;
