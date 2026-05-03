create table if not exists public."NRL Tryscorers" (
    "Match" text not null,
    "Date" date not null,
    "Result" text not null,
    "Value" double precision not null,
    "Market" text default 'Tryscorer',
    "Best Bookie" text,
    "Best Price" double precision,
    "Market %" double precision,
    "Sportsbet" double precision,
    "Pointsbet" double precision,
    "Unibet" double precision,
    "Palmerbet" double precision,
    "Betright" double precision,
    "created_at" timestamp with time zone default now(),
    "updated_at" timestamp with time zone default now(),
    constraint nrl_tryscorers_match_date_result_value_key
        unique ("Match", "Date", "Result", "Value")
);

create index if not exists idx_nrl_tryscorers_match_date
    on public."NRL Tryscorers" ("Match", "Date");

create index if not exists idx_nrl_tryscorers_result
    on public."NRL Tryscorers" ("Result");

create or replace function public.set_nrl_tryscorers_updated_at()
returns trigger
language plpgsql
as $$
begin
    new."updated_at" = now();
    return new;
end;
$$;

drop trigger if exists trg_nrl_tryscorers_updated_at on public."NRL Tryscorers";

create trigger trg_nrl_tryscorers_updated_at
before update on public."NRL Tryscorers"
for each row
execute function public.set_nrl_tryscorers_updated_at();
