{{config(materialized='view',
        schema='silver')}}

with src as (
    select * from {{source('silver','injury_spans')}}
)

select
    {{dbt_utils.generate_surrogate_key(['person_id','il_place_date']) }} as injury_span_sk,
    il_place_trans_id,
    il_place_date,
    person_id,
    il_place_team,
    raw_injury,
    side,
    body_part,
    injury_type,
    body_part_group,
    second_raw_injury,
    second_body_part,
    second_injury_type,
    return_trans_id,
    return_date,
    return_team,
    injury_length,
    currently_injured
from src