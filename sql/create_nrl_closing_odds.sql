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

create index if not exists idx_nrl_closing_odds_match_date_result
    on public."NRL Closing Odds" ("Match", "Date", "Result");
