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
    where season = (select max(season) from {{ ref('stg_teams') }})
),

salary_table as (
    select * from {{ ref('salary')}}
),

league_min_salary as (
  select season, min_salary
  from {{ ref('league_min_salary') }}
),

projections as (
    select * from {{ ref('projections')}}
)

select fullname as name,
        i.person_id,
        i.injury_span_sk,
        t.name as team,
        extract(year from p.mlb_debut_date) as mlb_debut_year,
        extract(year from p.last_played_date) as last_year,
        i.season as season,
        case
        when p.primaryposition_code = '1' then 'pitcher'
        else 'position player'
        end as position,
        raw_injury, 
        side,
        body_part,
        injury_type,
        body_part_group,
        second_body_part,
        second_injury_type,
        end_date::date - start_date::date as days_injured,
        start_date::date,
        case
        when end_date = current_date then null
        else end_date
        end as end_date,
        games_missed,
        case when games_missed > 162 then round(greatest(coalesce(salary,0), m.min_salary) / 1000000,2)
        else round(greatest(coalesce(salary,0), m.min_salary) * (games_missed::numeric/162) / 1000000,2)
        end as injured_salary,
        round(coalesce(war_per_162,.5) * (games_missed::numeric/162),1) as war_missed,
        currently_injured
from team_injury_spans i
left outer join players p
on i.person_id = p.person_id
left outer join teams t 
on i.team_id = t.team_id
left join salary_table s
on i.person_id = s.person_id and i.season = s.season
left join projections pro
on i.person_id = pro.person_id and i.season = pro.season
left join league_min_salary m
on i.season = m.season
