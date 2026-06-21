create table if not exists public."NRL Closing Odds" (
    "Match" text not null,
    "Date" date not null,
    "Result" text not null,
    "Market" text,
    "Value" double precision,
    "Best Bookie" text,
    "Best Price" double precision,
    "Market %" double precision,
    "Sportsbet" double precision,
    "Pointsbet" double precision,
    "Unibet" double precision,
    "Palmerbet" double precision,
    "Betright" double precision,
    "Closed Time" timestamp with time zone default now(),
    "Source Table" text
);

alter table public."NRL Closing Odds"
    add column if not exists "Market" text,
    add column if not exists "Value" double precision,
    add column if not exists "Closed Time" timestamp with time zone default now(),
    add column if not exists "Source Table" text;

update public."NRL Closing Odds"
set "Market" = case
    when "Source Table" = 'NRL Odds' then 'H2H'
    when "Source Table" = 'NRL Line Odds' then 'Line'
    when "Source Table" = 'NRL Total Odds' then 'Total'
    else "Market"
end
where "Market" is null
  and "Source Table" in ('NRL Odds', 'NRL Line Odds', 'NRL Total Odds');

create index if not exists idx_nrl_closing_odds_match_date_result
    on public."NRL Closing Odds" ("Match", "Date", "Result");

create index if not exists idx_nrl_closing_odds_market_match_date_result
    on public."NRL Closing Odds" ("Market", "Match", "Date", "Result");
