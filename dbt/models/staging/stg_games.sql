{{config(materialized='view',
        schema='bronze')}}

with src as (
    select * from {{source('bronze','games')}}
)

select
    gamepk,
    gametype,
    season,
    gamedate,
    officialdate,
    status_abstractgamestate as game_state,
    teams_away_team_id as away_team_id,
    teams_away_score as away_score,
    teams_home_team_id as home_team_id,
    teams_home_score as home_score,
    venue_id,
    gamenumber


    
from src