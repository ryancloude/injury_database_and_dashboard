
{{config(materialized='view',
        schema='int')}}

with team_injury_spans as (
    select * from {{ ref('team_injury_spans') }}
),


sliced AS (
  SELECT
    *,
    GREATEST(t.start_date, make_date(y, 1, 1))   AS slice_start_date,
    LEAST(t.end_date,   make_date(y, 12, 31))    AS slice_end_date
  FROM team_injury_spans t
  JOIN LATERAL generate_series(
         EXTRACT(YEAR FROM t.start_date)::int,
         EXTRACT(YEAR FROM t.end_date)::int,
         1
       ) y ON TRUE
),

injury_team_season_span as (
select person_id,
       team_id, 
       y as season,
       roster_sk, 
       injury_span_sk,
       injury_team_span_sk,
       raw_injury, 
       side, 
       body_part, 
       injury_type,
       body_part_group, 
       second_raw_injury,
       second_body_part,
       second_injury_type,
       currently_injured,
       slice_start_date as start_date,
       slice_end_date as end_date
    from sliced
)

select *, {{dbt_utils.generate_surrogate_key(['person_id','start_date', 'team_id', 'season']) }} as injury_team_season_span_sk
from injury_team_season_span
