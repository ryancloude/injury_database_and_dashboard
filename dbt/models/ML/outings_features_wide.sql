{{ config(
  materialized='incremental',
  unique_key='outing_id',
  schema='gold',
  on_schema_change='sync_all_columns',
  post_hook=["create index if not exists idx_gold_pitcher_outing on {{ this }} (outing_id)"]
) }}

--Pitch type groups
{% set pitch_types = var('pitch_types', ['ch','cu','fc','ff','fs','ot','si','sl','st']) %}

--Metrics tracked for each pitch_type for each outing
{% set lag_metrics = [
  'pitch_count',
  'avg_velo','avg_horz_rel','avg_vert_rel','avg_rel_ext',
  'avg_spin_rate',                     
  'stddev_velo','stddev_horz_rel','stddev_vert_rel','stddev_rel_ext',
  'stddev_spin_rate',             
  'stddev_rel_total',
] %}

--Amount of outings to look back
{% set last_n = var('last_n', 10) %}


with targets as (
  select
    outing_id,
    pitcher,
    game_date::date as as_of_date,
    game_pk,
    pitcher_team,
    pitchhand_code,
    season,
    team_game_num,
    days_since_last_outing,
    age,
    height,
    weight,
    (weight*703) / (height*height) as bmi,
    role,
    role_switch
  from {{ ref('pitcher_outings_w_static_labels') }}
  {% if is_incremental() %}
  where game_date::date >= (
    select coalesce(max(as_of_date), date '1900-01-01') - interval '60 days' from {{ this }}
  )
  {% endif %}
),


prior_injuries as (
  select
    t.outing_id            as target_outing_id,
    t.pitcher,
    t.as_of_date,
    i.il_date::date        as il_date,
    greatest(0, (t.as_of_date - i.il_date::date))::int as days_since_injury,
    coalesce(i.games_missed, 0) as games_missed,
    i.body_part_group,
    i.injury_type,
    i.second_injury_type,
    i.second_body_part,
    row_number() over (partition by t.outing_id order by i.il_date desc) as rn
  from targets t
  join {{ ref('player_injury_spans') }} i
    on i.person_id = t.pitcher
   and i.il_date::date < t.as_of_date
  {% if is_incremental() %}
  where i.il_date::date >= (
    select coalesce(max(as_of_date), date '1900-01-01') - interval '60 days' from {{ this }}
  )
  {% endif %}
),


injuries_agg as (
  select
    target_outing_id as outing_id,
    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'rn', rn,
          'days_since_injury', days_since_injury,
          'games_missed', games_missed,
          'body_part_group', body_part_group,
          'injury_type', injury_type,
          'second_injury_type', second_injury_type,
          'second_body_part', second_body_part
        )
        order by rn
      ) filter (where rn <= 5),
      '[]'::jsonb
    ) as injuries_last5_json,
    min(case when rn = 1 then days_since_injury end) as days_since_last_injury
  from prior_injuries
  group by target_outing_id
),


prior_outings as (
  select
    t.outing_id as target_outing_id,
    w.game_date as prior_game_date,
    w.game_pk   as prior_game_pk,
    s.role      as prior_role,
    s.role_switch as prior_role_switch,
    s.days_since_last_outing as prior_days_since_last_outing,
    w.outing_pitch_count as prior_pitch_count,
    w.outing_pitch_count_delta as prior_pitch_count_delta_prev,

{%- for pt in pitch_types -%}
  {%- set outer_loop = loop -%}
  {%- for m in lag_metrics -%}
    w.{{ pt }}_{{ m }} as {{ pt }}_{{ m }},
    w.{{ pt }}_{{ m }}_delta as {{ pt }}_{{ m }}_delta_prev{% if not (loop.last and outer_loop.last) %},{% endif %}
  {%- endfor -%}
{%- endfor -%}
,
    row_number() over (
      partition by t.outing_id
      order by w.game_date desc, w.game_pk desc
    ) as rn
  from targets t
  join {{ ref('pitcher_outings_w_pitch_info') }} w
    on w.pitcher = t.pitcher
   and w.game_date::date < t.as_of_date
  join {{ ref('pitcher_outings_w_static_labels') }} s
    on s.outing_id  = w.outing_id
  {% if is_incremental() %}
  where w.game_date::date >= (
    select coalesce(max(as_of_date), date '1900-01-01') - interval '60 days' from {{ this }}
  )
  {% endif %}
),


pivoted_outings as (
  select
    t.outing_id as p_outing_id

    -- Presence flags
    {% for n in range(1, last_n + 1) %}
      , CASE WHEN bool_or(o.rn = {{ n }}) THEN 1 ELSE 0 END as has_outing_t_{{ n }}
    {% endfor %}

    -- Total Outing Pitch Count rollups
    , avg(o.prior_pitch_count)              filter (where o.rn between 1 and {{ last_n }})
    as prior_pitch_count_last{{ last_n }}_mean
    , regr_slope(o.prior_pitch_count, o.rn) filter (where o.rn between 1 and {{ last_n }})
    as prior_pitch_count_last{{ last_n }}_slope
    , avg(o.prior_pitch_count_delta_prev)   filter (where o.rn between 1 and {{ last_n - 1 }})
    as prior_pitch_count_last{{ last_n }}_avg_delta
    ,
    STDDEV_POP(o.prior_pitch_count)   filter (where o.rn between 1 and {{ last_n - 1 }})
    as prior_pitch_count_last{{ last_n }}_stddev

    {% for pt in pitch_types %}
      {% for m in lag_metrics %}
        , avg(o.{{ pt }}_{{ m }})              filter (where o.rn between 1 and {{ last_n }})     as {{ pt }}_{{ m }}_last{{ last_n }}_mean
        , regr_slope(o.{{ pt }}_{{ m }}, o.rn) filter (where o.rn between 1 and {{ last_n }})     as {{ pt }}_{{ m }}_last{{ last_n }}_slope
        , avg(o.{{ pt }}_{{ m }}_delta_prev)   filter (where o.rn between 1 and {{ last_n - 1 }}) as {{ pt }}_{{ m }}_last{{ last_n }}_avg_delta
      {% endfor %}
    {% endfor %}

    -- Role switch count across last N priors (adjacent changes only; up to N-1 transitions)
    , coalesce(
        sum( case when o.prior_role_switch then 1 else 0 end )
          filter (where o.rn between 1 and {{ last_n - 1 }}),
        0
      ) as role_switch_count

    -- Ordered JSON of last N prior outings. Deltas embedded alongside values
    , coalesce(
        jsonb_agg(
          jsonb_strip_nulls(
            jsonb_build_object(
              'rn', o.rn,
              'game_date', o.prior_game_date,
              'game_pk', o.prior_game_pk,
              'role', o.prior_role,
              'role_switch', o.prior_role_switch,         -- <-- comma added here
              'days_since_last_outing', o.prior_days_since_last_outing,
              'pitch_count', o.prior_pitch_count,
              'pitch_count_delta_prev', o.prior_pitch_count_delta_prev
              {% for pt in pitch_types %}
              , '{{ pt }}', jsonb_build_object(
                  {% for m in lag_metrics %}
                    '{{ m }}',            o.{{ pt }}_{{ m }},
                    '{{ m }}_delta_prev', o.{{ pt }}_{{ m }}_delta_prev{% if not loop.last %}, {% endif %}
                  {% endfor %}
                )
              {% endfor %}
            )
          )
          order by o.rn
        ) filter (where o.rn <= {{ last_n }}),
        '[]'::jsonb
      ) as last{{ last_n }}_outings_json

  from targets t
  left join prior_outings o
    on o.target_outing_id = t.outing_id
  group by t.outing_id
)

select
  -- target outing spine
  t.outing_id,
  t.pitcher,
  t.as_of_date,
  t.game_pk,
  t.pitcher_team,
  t.pitchhand_code,
  t.season,
  t.team_game_num,
  t.days_since_last_outing,
  t.age,
  t.height,
  t.weight,
  t.role,
  t.role_switch,
  t.bmi,
  -- everything computed in pivoted_outings 
  p.*,

  -- recent injury context
  coalesce(i.injuries_last5_json, '[]'::jsonb) as injuries_last5_json,
  i.days_since_last_injury

from targets t
left join pivoted_outings p
  on p.p_outing_id = t.outing_id
left join injuries_agg i
  on i.outing_id = t.outing_id