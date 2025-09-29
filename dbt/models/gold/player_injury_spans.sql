{{config(materialized='table',
        schema='gold')}}

with injury_spans_w_games as (
    select * from {{ ref('team_season_injury_spans_w_games') }}
),

injury_spans_base as (
    select * from {{ ref('stg_injury_spans' )}}
),

ig as (select injury_span_sk, min(start_date) as il_date, 
case when sum(case when end_date is null then 1 else 0 end) > 0
     then null
     else max(end_date)
     end as return_date,
     sum(games_missed) as games_missed
from injury_spans_w_games
group by injury_span_sk
),

injury_spans as (
    select distinct injury_span_sk,
    person_id,
    raw_injury,
    side,
    injury_type,
    body_part_group,
    second_injury_type,
    second_body_part
    from injury_spans_base
)

select injury_spans.*,
il_date,
return_date,
games_missed
from ig
left join injury_spans
on ig.injury_span_sk = injury_spans.injury_span_sk