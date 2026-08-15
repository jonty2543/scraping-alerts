create table if not exists public.nrl_odds_snapshots (
    id bigint generated always as identity primary key,
    snapshot_date date not null,
    snapshot_slot text not null,
    captured_at timestamp with time zone not null,
    match text not null,
    event_date date not null,
    market text not null,
    result text not null,
    value double precision,
    value_key double precision generated always as (coalesce(value, '-999999'::double precision)) stored,
    best_bookie text,
    best_price double precision,
    market_percent double precision,
    sportsbet double precision,
    pointsbet double precision,
    unibet double precision,
    palmerbet double precision,
    betright double precision,
    source_table text not null,
    created_at timestamp with time zone not null default now(),
    constraint nrl_odds_snapshots_slot_check
        check (snapshot_slot in ('10am', '6pm')),
    constraint nrl_odds_snapshots_selection_key
        unique (
            snapshot_date,
            snapshot_slot,
            match,
            event_date,
            market,
            result,
            value_key
        )
);

create index if not exists idx_nrl_odds_snapshots_event
    on public.nrl_odds_snapshots (event_date, match, market);

create index if not exists idx_nrl_odds_snapshots_captured_at
    on public.nrl_odds_snapshots (captured_at desc);

create index if not exists idx_nrl_odds_snapshots_slot
    on public.nrl_odds_snapshots (snapshot_date desc, snapshot_slot);

grant select on public.nrl_odds_snapshots to anon, authenticated, service_role;
grant insert, update, delete on public.nrl_odds_snapshots to service_role;
grant usage, select on sequence public.nrl_odds_snapshots_id_seq to service_role;
