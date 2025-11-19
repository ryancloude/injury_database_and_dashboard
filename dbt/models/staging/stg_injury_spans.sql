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
    case when second_injury_type in ('tear','surgery') and injury_type not in ('tear','surgery') then second_raw_injury
    else raw_injury
    end as raw_injury,
    side,
    case when second_injury_type in ('tear','surgery') and injury_type not in ('tear','surgery') then second_injury_type
    else injury_type
    end as injury_type,
    case when second_injury_type in ('tear','surgery') and injury_type not in ('tear','surgery') then second_body_part
    else body_part
    end as body_part,
    body_part_group,
    case when second_injury_type in ('tear','surgery') and injury_type not in ('tear','surgery') then raw_injury
    else second_raw_injury
    end as second_raw_injury,
    case when second_injury_type in ('tear','surgery') and injury_type not in ('tear','surgery') then body_part
    else second_body_part
    end as second_body_part,
        case when second_injury_type in ('tear','surgery') and injury_type not in ('tear','surgery') then injury_type
    else second_injury_type
    end as second_injury_type,
    return_trans_id,
    return_date,
    return_team,
    injury_length,
    currently_injured
from src