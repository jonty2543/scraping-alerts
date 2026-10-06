do $$
begin
    if to_regclass('public."Rugby League World Cup Odds"') is not null
       and to_regclass('public."Rugby League Internationals Odds"') is null then
        alter table public."Rugby League World Cup Odds"
            rename to "Rugby League Internationals Odds";
    end if;

    if to_regclass('public."Rugby League World Cup Line Odds"') is not null
       and to_regclass('public."Rugby League Internationals Line Odds"') is null then
        alter table public."Rugby League World Cup Line Odds"
            rename to "Rugby League Internationals Line Odds";
    end if;

    if to_regclass('public."Rugby League World Cup Total Odds"') is not null
       and to_regclass('public."Rugby League Internationals Total Odds"') is null then
        alter table public."Rugby League World Cup Total Odds"
            rename to "Rugby League Internationals Total Odds";
    end if;
end $$;

create table if not exists public."Rugby League Internationals Odds" (
    "Match" text not null,
    "Date" date not null,
    "Result" text not null,
    "Best Bookie" text,
    "Best Price" double precision,
    "Market %" double precision,
    "Sportsbet" double precision,
    "Pointsbet" double precision,
    "Palmerbet" double precision,
    "Betright" double precision,
    "created_at" timestamp with time zone default now(),
    "updated_at" timestamp with time zone default now(),
    constraint rugby_league_internationals_odds_match_date_result_key
        unique ("Match", "Date", "Result")
);

create table if not exists public."Rugby League Internationals Line Odds" (
    "Match" text not null,
    "Date" date not null,
    "Result" text not null,
    "Market" text not null default 'Line',
    "Sportsbet_odds" double precision,
    "Sportsbet_line" double precision,
    "Pointsbet_odds" double precision,
    "Pointsbet_line" double precision,
    "Palmerbet_odds" double precision,
    "Palmerbet_line" double precision,
    "Betright_odds" double precision,
    "Betright_line" double precision,
    "created_at" timestamp with time zone default now(),
    "updated_at" timestamp with time zone default now(),
    constraint rugby_league_internationals_line_odds_match_date_result_key
        unique ("Match", "Date", "Result")
);

create table if not exists public."Rugby League Internationals Total Odds" (
    "Match" text not null,
    "Date" date not null,
    "Result" text not null,
    "Market" text not null default 'Total',
    "Sportsbet_odds" double precision,
    "Sportsbet_line" double precision,
    "Pointsbet_odds" double precision,
    "Pointsbet_line" double precision,
    "Palmerbet_odds" double precision,
    "Palmerbet_line" double precision,
    "Betright_odds" double precision,
    "Betright_line" double precision,
    "created_at" timestamp with time zone default now(),
    "updated_at" timestamp with time zone default now(),
    constraint rugby_league_internationals_total_odds_match_date_result_key
        unique ("Match", "Date", "Result")
);

alter table public."Rugby League Internationals Odds"
    add column if not exists "Palmerbet" double precision,
    add column if not exists "Betright" double precision;

alter table public."Rugby League Internationals Line Odds"
    add column if not exists "Palmerbet_odds" double precision,
    add column if not exists "Palmerbet_line" double precision,
    add column if not exists "Betright_odds" double precision,
    add column if not exists "Betright_line" double precision;

alter table public."Rugby League Internationals Total Odds"
    add column if not exists "Palmerbet_odds" double precision,
    add column if not exists "Palmerbet_line" double precision,
    add column if not exists "Betright_odds" double precision,
    add column if not exists "Betright_line" double precision;

create or replace function public.set_rugby_league_internationals_odds_updated_at()
returns trigger
language plpgsql
as $$
begin
    new."updated_at" = now();
    return new;
end;
$$;

drop trigger if exists trg_rugby_league_internationals_odds_updated_at
    on public."Rugby League Internationals Odds";
drop trigger if exists trg_rugby_league_world_cup_odds_updated_at
    on public."Rugby League Internationals Odds";
create trigger trg_rugby_league_internationals_odds_updated_at
before update on public."Rugby League Internationals Odds"
for each row
execute function public.set_rugby_league_internationals_odds_updated_at();

drop trigger if exists trg_rugby_league_internationals_line_odds_updated_at
    on public."Rugby League Internationals Line Odds";
drop trigger if exists trg_rugby_league_world_cup_line_odds_updated_at
    on public."Rugby League Internationals Line Odds";
create trigger trg_rugby_league_internationals_line_odds_updated_at
before update on public."Rugby League Internationals Line Odds"
for each row
execute function public.set_rugby_league_internationals_odds_updated_at();

drop trigger if exists trg_rugby_league_internationals_total_odds_updated_at
    on public."Rugby League Internationals Total Odds";
drop trigger if exists trg_rugby_league_world_cup_total_odds_updated_at
    on public."Rugby League Internationals Total Odds";
create trigger trg_rugby_league_internationals_total_odds_updated_at
before update on public."Rugby League Internationals Total Odds"
for each row
execute function public.set_rugby_league_internationals_odds_updated_at();

grant select on public."Rugby League Internationals Odds" to anon, authenticated, service_role;
grant insert, update, delete on public."Rugby League Internationals Odds" to service_role;

grant select on public."Rugby League Internationals Line Odds" to anon, authenticated, service_role;
grant insert, update, delete on public."Rugby League Internationals Line Odds" to service_role;

grant select on public."Rugby League Internationals Total Odds" to anon, authenticated, service_role;
grant insert, update, delete on public."Rugby League Internationals Total Odds" to service_role;
