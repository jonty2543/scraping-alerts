create table if not exists public."NRL Open Odds" (
    "Match" text not null,
    "Date" date not null,
    "Result" text not null,
    "Market" text not null,
    "Value" double precision,
    "Best Bookie" text,
    "Best Price" double precision,
    "Market %" double precision,
    "Sportsbet" double precision,
    "Pointsbet" double precision,
    "Unibet" double precision,
    "Palmerbet" double precision,
    "Betright" double precision,
    "Opened Time" timestamp with time zone default now(),
    "Source Table" text
);

alter table public."NRL Open Odds"
    add column if not exists "Market" text,
    add column if not exists "Value" double precision,
    add column if not exists "Best Bookie" text,
    add column if not exists "Best Price" double precision,
    add column if not exists "Market %" double precision,
    add column if not exists "Sportsbet" double precision,
    add column if not exists "Pointsbet" double precision,
    add column if not exists "Unibet" double precision,
    add column if not exists "Palmerbet" double precision,
    add column if not exists "Betright" double precision,
    add column if not exists "Opened Time" timestamp with time zone default now(),
    add column if not exists "Source Table" text;

create unique index if not exists idx_nrl_open_odds_unique_market_selection
    on public."NRL Open Odds" (
        "Match",
        "Date",
        "Result",
        "Market",
        coalesce("Value", -999999.0)
    );

create index if not exists idx_nrl_open_odds_market_match_date_result
    on public."NRL Open Odds" ("Market", "Match", "Date", "Result");

grant select on public."NRL Open Odds" to anon, authenticated, service_role;
grant insert, update, delete on public."NRL Open Odds" to service_role;
