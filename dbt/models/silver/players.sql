{{ config(
    materialized='table',
    schema='silver',
    alias='players',
    unique_key=['person_id']
) }}


with pl as (
  select * from {{ ref('stg_players') }}
),

pr as (
  select distinct person_id, nameascii from {{ ref('stg_projections') }}
)


select pl.*, pr.nameascii
from pl
left join pr
on pl.person_id = pr.person_id