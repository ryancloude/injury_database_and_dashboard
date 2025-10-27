{{config(
  materialized = 'incremental',
  schema='gold',
  incremental_strategy = 'merge',
  unique_key = ['pitcher','game_pk'],
  on_schema_change = 'sync_all_columns',
  post_hook=[
  "create index if not exists idx_gold_pitcher_gamepk on {{ this }} (outing_id)"])}}

{% if is_incremental() %}
with cutoff as (
    select coalesce(max(game_date), '2015-01-01'::date) - interval '35 days' as since
    from {{ this }}
),
{% endif %}


{% if not is_incremental() %}
with
{% endif %}
team_games as (
  select outing_id, pitcher, game_date, game_pk, pitcher_team, season, days_since_last_outing, team_game_num 
  from {{ ref('pitcher_outings_w_team_games') }}
    {% if is_incremental() %}
  where game_date >= (select since from cutoff)
  {% endif %}
),

bio as (
    select outing_id, pitchhand_code, age, height, weight
    from {{ ref('pitcher_outings_w_bio')}}
    {% if is_incremental() %}
  where game_date >= (select since from cutoff)
  {% endif %}
), 

pitcher_role as (
  select outing_id, role, role_switch
  from {{ ref('pitcher_outings_w_role') }}
    {% if is_incremental() %}
  where game_date >= (select since from cutoff)
  {% endif %}
),

labels as (
    select outing_id, last_app_before_injury, last_app_before_injury_arm, injured_within_7_days, injured_within_7_days_arm, 
    injured_within_30_days, injured_within_30_days_arm, next_il_date, injury_span_sk
    from {{ ref('pitcher_outings_w_labels')}}
    {% if is_incremental() %}
  where game_date >= (select since from cutoff)
  {% endif %}
)


select 
tg.outing_id,
tg.pitcher, 
tg.game_date, 
tg.game_pk,
tg.pitcher_team,
tg.season, 
tg.team_game_num,
case
when tg.days_since_last_outing > 30 then null
else tg.days_since_last_outing
end as days_since_last_outing,
bio.pitchhand_code,
bio.age,
bio.height,
bio.weight,
bio.weight / (bio.height * bio.height) * 703 as bmi,
pitcher_role.role,
pitcher_role.role_switch,
labels.last_app_before_injury,
labels.last_app_before_injury_arm,
labels.injured_within_7_days,
labels.injured_within_7_days_arm,
labels.injured_within_30_days,
labels.injured_within_30_days_arm,
labels.next_il_date,
labels.injury_span_sk


from team_games tg
left join bio
on tg.outing_id = bio.outing_id
left join pitcher_role
on tg.outing_id = pitcher_role.outing_id
left join labels
on tg.outing_id = labels.outing_id


