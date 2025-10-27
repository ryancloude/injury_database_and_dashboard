{{
  config(
    materialized = 'incremental',
    schema = 'int',
    incremental_strategy = 'merge',
    unique_key = ['outing_id'],
    on_schema_change = 'sync_all_columns',
    post_hook = [
      "create index if not exists idx_il_labels_outing_id on {{ this }} (outing_id)",
      "create index if not exists idx_il_labels_pitcher_date on {{ this }} (pitcher, game_date)"
    ]
  )
}}

with pitcher_outings as (
  select
    o.outing_id,
    o.pitcher,
    o.game_date,
    o.game_pk
  from {{ ref('pitcher_outings') }} o
  {% if is_incremental() %}
    where o.game_date >= (
      select coalesce(max(game_date), '2015-01-01'::date) - interval '35 days'
      from {{ this }}
    )
  {% endif %}
),

-- keep injury spans broad; planner will use the person/date predicates from the LATERAL
injury_spans as (
  select
    i.person_id,
    i.il_place_date,
    i.injury_span_sk,
    i.body_part_group
  from {{ ref('stg_injury_spans') }} i
),

-- Fast "next IL date" per outing using LATERAL + min()
next_il as (
  select
    po.outing_id,
    po.pitcher,
    po.game_date,
    po.game_pk,
    n.next_il_date,
    n.injury_span_sk,
    (coalesce(n.body_part_group in ('other_arm','elbow','shoulder'), false)) as arm_injury
  from pitcher_outings po
  left join lateral (
    select i.il_place_date as next_il_date, i.injury_span_sk, i.body_part_group
    from injury_spans i
    where i.person_id = po.pitcher
      and i.il_place_date >= po.game_date
    order by next_il_date asc
    limit 1
  ) n on true
),

outings_with_labels as (
  select
    *,
    row_number() over (
      partition by pitcher, next_il_date
      order by game_date desc
    ) as next_il_rn,
    (next_il_date is not null and next_il_date <= game_date + interval '7 day')::boolean  as injured_within_7_days,
    (next_il_date is not null and next_il_date <= game_date + interval '30 day')::boolean as injured_within_30_days
  from next_il
)

select
  outing_id,
  pitcher,
  game_date,
  game_pk,
  (next_il_rn = 1 and next_il_date is not null and injured_within_30_days) as last_app_before_injury,
  (next_il_rn = 1 and next_il_date is not null and injured_within_30_days and arm_injury) as last_app_before_injury_arm,
  injured_within_7_days,
  (injured_within_7_days and arm_injury) as injured_within_7_days_arm,
  injured_within_30_days,
  (injured_within_30_days and arm_injury) as injured_within_30_days_arm,
  case
  when injured_within_30_days then next_il_date
  else null 
  end as next_il_date,
  case
  when injured_within_30_days then injury_span_sk
  else null 
  end as injury_span_sk
from outings_with_labels