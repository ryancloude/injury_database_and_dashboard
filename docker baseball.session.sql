with base as (SELECT pr.*, games_started::NUMERIC / (games) as games_started_ratio
from silver.players pl
left join bronze.stg_projections as pr
on pl.person_id = pr.person_id
where (primaryposition_code = '1' and position = 'pitcher') 
or (primaryposition_code != '1' and position = 'position player')
or primaryposition_code = 'Y'
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

select name, season, person_id, pa ,games, games_started, war, war/games as war_per_g, 
full_season_games, war/games * full_season_games as war_per_162, full_season_games
from fs
order by war_per_162
desc;