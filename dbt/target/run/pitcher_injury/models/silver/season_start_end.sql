
  create view "baseball"."silver"."season_start_end__dbt_tmp"
    
    
  as (
    


with games as (
  -- Reuse staging instead of re-writing transforms
  select * from "baseball"."bronze"."stg_games"
),

teams as (
    select DISTINCT team_id
    from "baseball"."bronze"."stg_teams"
    where sport_id = 1
),

all_games AS (
    -- All home games in the regular season
    SELECT 
        season,
        home_team_id AS team_id,
        gamedate
    FROM games
    WHERE gametype != 'S' and gametype != 'E' and home_team_id in (select * from teams)

    UNION ALL

    -- All away games in the regular season
    SELECT 
        season,
        away_team_id AS team_id,
        gamedate
    FROM games
    WHERE gametype != 'S' and gametype != 'E' and away_team_id in (select * from teams)
),

season_dates as (
select team_id, season, min(gamedate) as first_game, max(gamedate) as last_game
from all_games
group by team_id, season
)

select team_id, season, first_game,
CASE
when season = extract(year from current_date) and extract(month from current_date) < 11
then Null
else last_game
end as last_game
from season_dates
  );