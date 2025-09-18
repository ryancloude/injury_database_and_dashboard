{{config(materialized='view',
        schema='bronze')}}

with src as (
    select * from {{source('bronze','transactions')}}
)

select
    id as trans_id,
    date,
    effectivedate, 
    resolutiondate,
    typecode,
    typedesc,
    description, 
    person_id,  
    toteam_id,
    fromteam_id   
from src