create table if not exists public."NRL Margin Odds" (
    "Match" text not null,
    "Date" date not null,
    "Result" text not null,
    "Market" text not null default 'Margin',
    "Best Bookie" text,
    "Best Price" double precision,
    "Market %" double precision,
    "Sportsbet" double precision,
    "Pointsbet" double precision,
    "Unibet" double precision,
    "Palmerbet" double precision,
    "Betright" double precision,
    "created_at" timestamp with time zone not null default now(),
    "updated_at" timestamp with time zone not null default now(),
    constraint nrl_margin_odds_match_date_result_key
        unique ("Match", "Date", "Result")
);

alter table public."NRL Margin Odds"
    add column if not exists "Market" text not null default 'Margin',
    add column if not exists "Best Bookie" text,
    add column if not exists "Best Price" double precision,
    add column if not exists "Market %" double precision,
    add column if not exists "Sportsbet" double precision,
    add column if not exists "Pointsbet" double precision,
    add column if not exists "Unibet" double precision,
    add column if not exists "Palmerbet" double precision,
    add column if not exists "Betright" double precision,
    add column if not exists "created_at" timestamp with time zone not null default now(),
    add column if not exists "updated_at" timestamp with time zone not null default now();

create unique index if not exists idx_nrl_margin_odds_match_date_result
    on public."NRL Margin Odds" ("Match", "Date", "Result");

create index if not exists idx_nrl_margin_odds_match_date
    on public."NRL Margin Odds" ("Match", "Date");

create or replace function public.set_nrl_margin_odds_updated_at()
returns trigger
language plpgsql
as $$
begin
    new."updated_at" = now();
    return new;
end;
$$;

drop trigger if exists trg_nrl_margin_odds_updated_at on public."NRL Margin Odds";

create trigger trg_nrl_margin_odds_updated_at
before update on public."NRL Margin Odds"
for each row
execute function public.set_nrl_margin_odds_updated_at();

grant select on public."NRL Margin Odds" to anon, authenticated, service_role;
grant insert, update, delete on public."NRL Margin Odds" to service_role;

-- The summary snapshot now exposes margin odds alongside h2h, line, total and tryscorer.
create schema if not exists summary;

create table if not exists summary.betting_odds_snapshot (
    id text primary key default 'current',
    h2h jsonb not null default '[]'::jsonb,
    line jsonb not null default '[]'::jsonb,
    total jsonb not null default '[]'::jsonb,
    margin jsonb not null default '[]'::jsonb,
    tryscorer jsonb not null default '[]'::jsonb,
    generated_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

alter table summary.betting_odds_snapshot
    add column if not exists margin jsonb not null default '[]'::jsonb;

grant usage on schema summary to anon, authenticated, service_role;
grant select on summary.betting_odds_snapshot to anon, authenticated, service_role;
grant insert, update, delete on summary.betting_odds_snapshot to service_role;
