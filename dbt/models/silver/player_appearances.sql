{{ config(
    materialized='incremental',
    schema='silver',
    alias='player_appearances',
    incremental_strategy='merge',
    unique_key=['person_id', 'game_date', 'team_id'],
    post_hook=[
      "create index if not exists ix_{{ this.name }}__person_game_date on {{ this }} (person_id, game_date)"
    ]
) }}

with src as (

    select
        pitcher as person_id,
        game_date,
        pitcher_team as team_id
    from {{ ref('statcast') }}
    where pitcher is not null

    union all

    select
        batter as person_id,
        game_date,
        batter_team as team_id
    from {{ ref('statcast') }}
    where batter is not null
),

deduped as (
    select distinct
        person_id,
        game_date,
        team_id
    from src
    where team_id is not null
)

select *
from deduped

{% if is_incremental() %}
  where game_date >= current_date - interval '7 day'
{% endif %}