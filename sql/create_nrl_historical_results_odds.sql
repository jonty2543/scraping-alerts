create table if not exists public.nrl_historical_results_odds (
    date date not null,
    kickoff_local time,
    match text not null,
    home_team text not null,
    away_team text not null,
    venue text,
    home_score integer,
    away_score integer,
    play_off_game boolean,
    over_time boolean,
    home_odds double precision,
    draw_odds double precision,
    away_odds double precision,
    bookmakers_surveyed integer,
    home_odds_open double precision,
    home_odds_min double precision,
    home_odds_max double precision,
    home_odds_close double precision,
    away_odds_open double precision,
    away_odds_min double precision,
    away_odds_max double precision,
    away_odds_close double precision,
    home_line_open double precision,
    home_line_min double precision,
    home_line_max double precision,
    home_line_close double precision,
    away_line_open double precision,
    away_line_min double precision,
    away_line_max double precision,
    away_line_close double precision,
    home_line_odds_open double precision,
    home_line_odds_min double precision,
    home_line_odds_max double precision,
    home_line_odds_close double precision,
    away_line_odds_open double precision,
    away_line_odds_min double precision,
    away_line_odds_max double precision,
    away_line_odds_close double precision,
    total_score_open double precision,
    total_score_min double precision,
    total_score_max double precision,
    total_score_close double precision,
    total_score_over_open double precision,
    total_score_over_min double precision,
    total_score_over_max double precision,
    total_score_over_close double precision,
    total_score_under_open double precision,
    total_score_under_min double precision,
    total_score_under_max double precision,
    total_score_under_close double precision,
    notes text,
    source_url text not null,
    source_downloaded_at timestamp with time zone not null default now(),
    created_at timestamp with time zone not null default now(),
    updated_at timestamp with time zone not null default now(),
    constraint nrl_historical_results_odds_pkey
        primary key (date, home_team, away_team)
);

create index if not exists idx_nrl_historical_results_odds_match
    on public.nrl_historical_results_odds (match);

create index if not exists idx_nrl_historical_results_odds_teams
    on public.nrl_historical_results_odds (home_team, away_team);

create or replace function public.set_nrl_historical_results_odds_updated_at()
returns trigger
language plpgsql
as $$
begin
    new.updated_at = now();
    return new;
end;
$$;

drop trigger if exists trg_nrl_historical_results_odds_updated_at
    on public.nrl_historical_results_odds;

create trigger trg_nrl_historical_results_odds_updated_at
before update on public.nrl_historical_results_odds
for each row
execute function public.set_nrl_historical_results_odds_updated_at();

grant select on public.nrl_historical_results_odds to anon, authenticated, service_role;
grant insert, update, delete on public.nrl_historical_results_odds to service_role;
