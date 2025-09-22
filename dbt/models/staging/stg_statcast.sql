{{config(materialized='view',
        schema='bronze')}}

with src as (
    select * from {{source('bronze','statcast')}}
),

teams as (
        select DISTINCT team_id, abbreviation
        from {{ref('stg_teams')}}
)

select
        game_pk,
        at_bat_number,
        pitch_number,
        {{ dbt_utils.generate_surrogate_key(['game_pk','at_bat_number','pitch_number']) }} as pitch_id,
        game_date,
        game_year,
        pitcher,
        batter,
        description,
        events,
        pitch_type,
        release_speed,
        release_spin_rate,
        release_pos_x,
        release_pos_z,
        release_extension,
        pfx_x,
        pfx_z,
        plate_x,
        plate_z,
        t1.team_id as home_team,
        t2.team_id as away_team,
        inning_topbot
from src
left join teams t1
on home_team = t1.abbreviation
left join teams t2
on away_team = t2.abbreviation