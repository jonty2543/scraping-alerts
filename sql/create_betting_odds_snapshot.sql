create schema if not exists summary;

create table if not exists summary.betting_odds_snapshot (
    id text primary key default 'current',
    h2h jsonb not null default '[]'::jsonb,
    line jsonb not null default '[]'::jsonb,
    total jsonb not null default '[]'::jsonb,
    tryscorer jsonb not null default '[]'::jsonb,
    generated_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

alter table summary.betting_odds_snapshot
    add column if not exists h2h jsonb not null default '[]'::jsonb,
    add column if not exists line jsonb not null default '[]'::jsonb,
    add column if not exists total jsonb not null default '[]'::jsonb,
    add column if not exists tryscorer jsonb not null default '[]'::jsonb,
    add column if not exists generated_at timestamptz not null default now(),
    add column if not exists updated_at timestamptz not null default now();

create or replace function summary.set_betting_odds_snapshot_updated_at()
returns trigger
language plpgsql
as $$
begin
    new.updated_at = now();
    return new;
end;
$$;

drop trigger if exists trg_betting_odds_snapshot_updated_at on summary.betting_odds_snapshot;

create trigger trg_betting_odds_snapshot_updated_at
before update on summary.betting_odds_snapshot
for each row
execute function summary.set_betting_odds_snapshot_updated_at();

grant usage on schema summary to anon, authenticated, service_role;
grant select on summary.betting_odds_snapshot to anon, authenticated, service_role;
grant insert, update, delete on summary.betting_odds_snapshot to service_role;
