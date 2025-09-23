{{config(materialized='view',
        schema='gold')}}

with injury_spans as (
    select * from {{ ref('stg_injury_spans') }}
),

roster_entries as (
    select * from {{ ref('stg_roster_entries') }}
),

i as (
    select person_id, injury_sk ,raw_injury, il_place_date, side, body_part, injury_type,
    second_raw_injury, second_body_part, second_injury_type,
    COALESCE(return_date::date, current_date) as return_date, currently_injured
    from injury_spans
),

r as (
    select person_id, team_id, roster_sk, start_date, COALESCE(end_date::date, current_date) as end_date, is_active
    from roster_entries
    where status_code != 'RA'
)


select i.person_id, 
        team_id, 
        roster_sk, 
        injury_sk, 
        raw_injury, 
        side, 
        body_part, 
        injury_type, 
        second_raw_injury, 
        second_body_part, 
        second_injury_type,
currently_injured, greatest(il_place_date, r.start_date) as start_date, least(return_date, r.end_date) as end_date
from i
join r 
on i.person_id = r.person_id
and i.il_place_date <= r.end_date
and i.return_date >= r.start_date
