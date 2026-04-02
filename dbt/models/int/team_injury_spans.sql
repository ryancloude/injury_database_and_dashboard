{{config(materialized='view',
        schema='int')}}

with injury_spans as (
    select * from {{ ref('stg_injury_spans') }}
),

roster_entries as (
    select * from {{ ref('stg_roster_entries') }}
),

i as (
    select
        person_id,
        il_place_team,
        injury_span_sk,
        raw_injury,
        il_place_date,
        side,
        body_part,
        injury_type,
        body_part_group,
        second_raw_injury,
        second_body_part,
        second_injury_type,
        coalesce(return_date::date, current_date::date) as return_date,
        currently_injured
    from injury_spans
),

r as (
    select
        person_id,
        team_id,
        roster_sk,
        start_date,
        status_date,
        coalesce(end_date::date, current_date::date) as end_date,
        is_active
    from roster_entries
    where status_code != 'RA'
),

team_spans_raw as (
    select
        i.person_id,
        case
            when r.team_id is null then i.il_place_team
            else r.team_id
        end as team_id,
        r.roster_sk,
        i.injury_span_sk,
        i.raw_injury,
        i.side,
        i.body_part,
        i.injury_type,
        i.body_part_group,
        i.second_raw_injury,
        i.second_body_part,
        i.second_injury_type,
        i.currently_injured,
        greatest(i.il_place_date, r.start_date) as start_date,
        least(i.return_date, r.end_date) as end_date,
        r.status_date
    from i
    left join r
      on i.person_id = r.person_id
     and i.il_place_date < r.end_date
     and i.return_date >= r.start_date
),

team_spans_deduped as (
    select *
    from (
        select
            *,
            row_number() over (
                partition by person_id, injury_span_sk, team_id, start_date, end_date
                order by status_date desc nulls last, roster_sk desc nulls last
            ) as rn
        from team_spans_raw
    ) x
    where rn = 1
)

select
    person_id,
    team_id,
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
    start_date,
    end_date,
    {{ dbt_utils.generate_surrogate_key(['person_id','start_date','team_id']) }} as injury_team_span_sk
from team_spans_deduped