{{config(materialized='view',
        schema='bronze')}}

with src as (
    select * from {{source('bronze','salary')}}
)

select
    player::TEXT as player_name,
    person_id::INT,
    salary::NUMERIC,
    season::INT,
    team::TEXT
from src