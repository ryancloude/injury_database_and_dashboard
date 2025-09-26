{{config(materialized='table',
        schema='gold')}}

with team_injury_spans as (
    select * from {{ ref('team_season_injury_spans_w_games') }}
),

players as (
    select * from {{ ref('players') }}
),

teams as (
    select * from {{ ref('stg_teams') }}
)

select fullname as name,
        t.name as team,
        i.season as season, 
        raw_injury, 
        side,
        body_part,
        injury_type,
        body_part_group,
        second_body_part,
        second_injury_type,
        end_date::date - start_date::date as days_injured,
        start_date,
        case
        when end_date = current_date then null
        else end_date
        end as end_date,
        games_missed,
        currently_injured
from team_injury_spans i
left join players p
on i.person_id = p.person_id
left join teams t 
on i.team_id = t.team_id and i.season = t.season
