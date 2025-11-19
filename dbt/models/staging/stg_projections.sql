{{config(materialized='view',
        schema='bronze')}}

with src as (
    select * from {{source('bronze','projections')}}
)

select
    name::TEXT,
    player_id::INT as person_id,
    case
    when pa::Numeric > 700 then 700
    else pa::NUMERIC end as pa,
    case
    when games > 162 then games::NUMERIC 
    when games is Null then ROUND(pa::NUMERIC/4.4,0)
    else games::Numeric
    end as games,
    games_started::NUMERIC,
    war::NUMERIC,
    nameascii::TEXT,
    position::TEXT,
    season::INT
from src