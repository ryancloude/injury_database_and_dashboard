{{ config(
    materialized='table',
    schema='silver',
    alias='projections',
    unique_key=['person_id', 'season']
) }}


with pro as (
  select * from {{ ref('stg_projections') }}
),

pl as (
  select * from {{ ref('stg_players') }}
),

base as (SELECT pro.*, games_started::NUMERIC / (games) as games_started_ratio
from pl
left join pro as pro
on pl.person_id = pro.person_id
/*Drops rows for players that have projections as two way players (ex. Noah McLean) but are not listed as two way players*/
where (primaryposition_abbreviation = 'P' and position = 'pitcher') 
or (primaryposition_abbreviation != 'P' and position = 'position player')
or primaryposition_abbreviation = 'TWP'
),

fs as (
select *,
CASE
when games > 162
then 162
when position = 'position player'
then 162
when position = 'pitcher' and games_started = games and games_started < 33
then 33
when position = 'pitcher' and games_started = 0 and games < 65
then 65
when position = 'pitcher' and games_started > 1
then round((33 * games_started_ratio) + (65 * (1-games_started_ratio)),0)
else games
end as full_season_games
from base
)

select season, person_id, max(position) as position, sum(pa) as pa, sum(games) as games, sum(games_started) as games_started, sum(war) as war, 
sum(war/games) as war_per_g, 
sum(full_season_games) as full_season_games, 
sum(war/games * full_season_games) as war_per_162
from fs
group by person_id, season