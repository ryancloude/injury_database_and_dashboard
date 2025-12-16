{{ config(
  materialized='table',
  schema='gold'
) }}

with tsi as (
  select *
  from {{ ref('team_season_injuries') }}
)

select max(name) as name, 
max(person_id) as person_id,
injury_span_sk,
min(start_date) as start_date,
max(end_date) as end_date,
max(position) as position,
max(body_part) as body_part,
max(injury_type) as injury_type, 
max(side) as side,
max(second_body_part) as second_body_part,
max(second_injury_type) as second_injury_type,
sum(games_missed) as games_missed,
sum(injured_salary) as injured_salary,
sum(war_missed) as war_missed
from tsi
group by injury_span_sk