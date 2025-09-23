{{config(materialized='view',
        schema='silver')}}

with src as (
    select * from {{source('bronze','roster_entries')}}
)

select
    {{dbt_utils.generate_surrogate_key(['person_id','team_id','start_date','status_code']) }} as roster_sk,
    person_id,
    team_id,
    start_date,
    status_date,
    end_date,
    is_active,
    status_code,
    status_desc,
    parent_org_id
from src