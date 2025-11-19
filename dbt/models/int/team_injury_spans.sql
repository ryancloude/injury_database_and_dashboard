{{config(materialized='view',
        schema='int')}}

with injury_spans as (
    select * from {{ ref('stg_injury_spans') }}
),

roster_entries as (
    select * from {{ ref('stg_roster_entries') }}
),

i as (
    select person_id, il_place_team, injury_span_sk ,raw_injury, il_place_date, side, body_part, injury_type, body_part_group,
    second_raw_injury, second_body_part, second_injury_type,
    COALESCE(return_date::date, current_date::date) as return_date, currently_injured
    from injury_spans
),

r as (
    select person_id, team_id, roster_sk, start_date, COALESCE(end_date::date, current_date::date) as end_date, is_active
    from roster_entries
    where status_code != 'RA'
),


team_spans as (
select i.person_id,
        Case 
        when team_id is null then il_place_team
        else team_id
        end as team_id, 
        roster_sk, 
        injury_span_sk, 
        raw_injury, 
        side, 
        body_part, 
        injury_type,
        body_part_group, 
        second_raw_injury, 
        second_body_part, 
        second_injury_type,
        currently_injured, 
        greatest(il_place_date, r.start_date) as start_date,
        least(return_date, r.end_date) as end_date
from i
left join r 
on i.person_id = r.person_id
and i.il_place_date < r.end_date
and i.return_date >= r.start_date
)

select *, {{dbt_utils.generate_surrogate_key(['person_id','start_date', 'team_id']) }} as injury_team_span_sk
from team_spans