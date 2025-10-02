{{ config(
    materialized='table',
    schema='gold'
) }}

{% set lookback_days = var('incremental_lookback_days', 35) %}

{% set pitch_types = var('pitch_types', ['CH','CU','FC','FF','FS','OT','SI','SL','ST']) %}
{% set metrics = [
  'pitch_count',
  'avg_velo','stddev_velo',
  'avg_spin_rate','stddev_spin_rate',
  'avg_horz_mov','stddev_horz_mov',
  'avg_vert_mov','stddev_vert_mov',
  'avg_horz_rel','stddev_horz_rel',
  'avg_vert_rel','stddev_vert_rel',
  'avg_rel_ext','stddev_rel_ext'
] %}

{% if is_incremental() %}
with cutoff as (
  select (coalesce(max(game_date), '2015-01-01'::date) - interval '{{ lookback_days }} days') as since
  from {{ this }}
),

{% else %}
with
{% endif %}

po as (
  select *
  from {{ ref('pitcher_outings') }} po
  {% if is_incremental() %}
  where po.game_date >= (select since from cutoff)
  {% endif %}
),

popt as (
  select *
  from {{ ref('pitcher_outings_pitch_type') }} popt
  {% if is_incremental() %}
  where popt.game_date >= (select since from cutoff)
  {% endif %}
),

base as (
  select
      po.outing_id,
      po.pitcher,
      po.game_date,
      po.game_pk,
      po.pitcher_team,
      po.pitch_count       as outing_pitch_count,
      po.days_since_last_outing,
      po.avg_horz_rel      as outing_avg_horz_rel,
      po.stddev_horz_rel   as outing_stddev_horz_rel,
      po.avg_vert_rel      as outing_avg_vert_rel,
      po.stddev_vert_rel   as outing_stddev_vert_rel,
      po.avg_rel_ext       as outing_avg_rel_ext,
      po.stddev_rel_ext    as outing_stddev_rel_ext,
      popt.pitch_type,
      {%- for m in metrics -%}
      popt.{{ m }}{% if not loop.last %}, {% endif %}
      {%- endfor %}
  from po
  left join popt
    on po.pitcher   = popt.pitcher
   and po.game_date = popt.game_date
)


select
  pitcher,
  game_date,
  max(outing_id) as outing_id,
  max(game_pk) as game_pk,
  max(pitcher_team)             as pitcher_team,
  max(outing_pitch_count)       as outing_pitch_count,
  max(days_since_last_outing)   as days_since_last_outing,
  max(outing_avg_horz_rel)      as outing_avg_horz_rel,
  max(outing_stddev_horz_rel)   as outing_stddev_horz_rel,
  max(outing_avg_vert_rel)      as outing_avg_vert_rel,
  max(outing_stddev_vert_rel)   as outing_stddev_vert_rel,
  max(outing_avg_rel_ext)       as outing_avg_rel_ext,
  max(outing_stddev_rel_ext)    as outing_stddev_rel_ext,

  {%- for pt in pitch_types -%}
    {%- for m in metrics -%}
      max({{ m }}) filter (where pitch_type = '{{ pt }}') as {{ pt }}_{{ m }}{% if not loop.last %}, {% endif %}
    {%- endfor -%}
    {% if not loop.last %}, {% endif %}
  {%- endfor %}

from base
group by pitcher, game_date