with opening_days as (
select season, team, min(opening_day) as opening_day from (
select EXTRACT(year from game_date::date) as season, home_team as team,
min(game_date) as opening_day
from games 
where game_type = 'R'
group by season, team
union ALL
select EXTRACT(year from game_date::date) as season, away_team as team,
min(game_date) as opening_day
from games 
where game_type = 'R'
group by season, team
) as t
group by season, team
order by season),

season_end as (
select EXTRACT(year from game_date::date) as season, 
max(game_date::date) as final_postseason
from games 
where game_type = 'W'
group by season
)

select opening_days.season as season, opening_days.team as team, opening_day, final_postseason
from opening_days
left join season_end
on opening_days.season = season_end.season;