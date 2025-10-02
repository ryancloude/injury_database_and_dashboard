{{config(materialized='ephemeral',
        schema='int',
        unique_key = ['outing_id'])}}

with pitcher_outings as (
  select * from {{ ref('pitcher_outings') }}
),

players as (
    select * from {{ ref('players')}}
)

select pitcher_outings.*, pitchhand_code ,game_date::date - birthdate::date as age,
  (split_part(height, '''', 1)::int * 12) +
  split_part(split_part(height, '''', 2), '"', 1)::int as height,
  weight
from pitcher_outings
join players
on pitcher_outings.pitcher = players.person_id