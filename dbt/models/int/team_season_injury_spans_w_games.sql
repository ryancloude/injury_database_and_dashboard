{{config(materialized='view',
        schema='int')}}

with team_season_injury_spans as (
    select * from {{ ref('team_season_injury_spans') }}
),

games as (
    select * from {{ref('stg_games')}}
),

temp as (
select injury_team_season_span_sk, i.start_date, i.end_date, g.gamepk
from team_season_injury_spans i
join bronze.games g
on g.season = i.season
and (g.teams_home_team_id = i.team_id or g.teams_away_team_id = i.team_id)
and g.gamedate > i.start_date
and g.gamedate <= i.end_date
where gametype in ('R','F', 'D', 'L', 'W')
),

injury_games_missed as (
select injury_team_season_span_sk, count(DISTINCT gamepk) as games_missed
from temp
group by injury_team_season_span_sk
order by games_missed desc
)

select person_id,
        team_id,
        season,
        injury_span_sk,
        injury_team_span_sk,
        i.injury_team_season_span_sk,
        raw_injury,
        side,
        body_part,
        injury_type,
        body_part_group,
        second_body_part,
        second_injury_type,
        currently_injured,
        start_date,
        end_date,
        games_missed
from int.team_season_injury_spans i
join injury_games_missed g
on i.injury_team_season_span_sk = g.injury_team_season_span_sk