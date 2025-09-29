{{config(materialized='view',
        schema='int')}}

with po as (
    select * from {{ ref('pitcher_outings') }}
),

games as (
    select * from {{ ref('stg_games' )}}
),


team_games_base as (
    SELECT g.season,
    g.gamedate,
    g.away_team_id as team_id
    from games g
    where gametype in ('R', 'D', 'L', 'W','C','P')
    and game_state = 'Final'
    UNION ALL
    SELECT g.season,
    g.gamedate,
    g.home_team_id as team_id
    from games g
    where gametype in ('R', 'D', 'L', 'W','C','P')
    and game_state = 'Final'
),

team_games as (
    SELECT season,
    team_id,
    gamedate,
    row_number() over (PARTITION by season, team_id order by gamedate) as team_game_num
    from team_games_base
)

select po.*, team_game_num
from po
join team_games
on team_games.gamedate::date = po.game_date::date
and team_games.team_id = po.pitcher_team