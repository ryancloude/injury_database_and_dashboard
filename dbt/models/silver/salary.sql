{{ config(
    materialized='table',
    schema='silver',
    alias='salary',
    unique_key=['person_id', 'season']
) }}


with teams as (
  select distinct name, team_id
  from {{ ref('stg_teams') }}
  where season = (select max(season) from {{ ref('stg_teams') }})
),

salary as (
    select *
    from {{ ref('stg_salary') }}
    where person_id is not NULL
),

league_min_salary as (
  select season, min_salary
  from {{ ref('league_min_salary') }}
),

base as (
    select person_id, season, sum(salary) as salary
    from salary
    join teams
    on lower(salary.team) = lower(teams.name)
    where salary > 0
    group by person_id, season
)




select person_id, b.season as season,
case
when b.salary is null or b.salary < m.min_salary then m.min_salary
else salary
end as salary
from base b
left join league_min_salary m
on b.season = m.season