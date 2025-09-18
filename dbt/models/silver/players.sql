{{ config(
    materialized='table',
    schema='silver',
    alias='players',
    unique_key=['person_id']
) }}


with src as (
  -- Reuse staging instead of re-writing transforms
  select * from {{ ref('stg_players') }}
)

select *
from src