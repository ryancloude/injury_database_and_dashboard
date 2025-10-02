{{
  config(
    materialized = 'incremental',
    schema = 'int',
    incremental_strategy = 'merge',
    unique_key = ['outing_id'],
    on_schema_change = 'sync_all_columns',
    post_hook = [
      "create index if not exists idx_pitcher_outings_outing_id on {{ this }} (outing_id)"
    ]
  )
}}

with statcast as (
    select
        s.pitcher,
        s.pitcher_team,
        s.game_date,
        s.game_pk,
        s.pitch_id,
        s.release_pos_x,
        s.release_pos_z,
        s.release_extension
    from {{ ref('statcast') }} as s
    {% if is_incremental() %}
      where s.game_date >= (
        select coalesce(max(game_date), '2015-01-01'::date) - interval '35 days'
        from {{ this }}
      )
    {% endif %}
),

players as (
    select
        person_id,
        primaryposition_code
    from {{ ref('players') }}
),

games as (
    select 
        gamepk,
        gametype,
        away_team_id,
        home_team_id
    from {{ ref('stg_games') }}
),

teams as (
  select distinct team_id
  from {{ref('stg_teams')}}
),

outings as (
    select
        s.pitcher                               as pitcher,
        s.pitcher_team                          as pitcher_team,
        s.game_date                             as game_date,
        s.game_pk                               as game_pk,
        count(distinct s.pitch_id)              as pitch_count,
        {{ dbt_utils.generate_surrogate_key(['s.pitcher','s.game_date','s.game_pk']) }} as outing_id,
        lag(s.game_date) over (
            partition by s.pitcher
            order by s.game_date asc
        )                                       as last_outing,
        round(avg(s.release_pos_x)::numeric, 2)     as avg_horz_rel,
        round(stddev_pop(s.release_pos_x)::numeric, 2) as stddev_horz_rel,
        round(avg(s.release_pos_z)::numeric, 2)     as avg_vert_rel,
        round(stddev_pop(s.release_pos_z)::numeric, 2) as stddev_vert_rel,
        round(avg(s.release_extension)::numeric, 2) as avg_rel_ext,
        round(stddev_pop(s.release_extension)::numeric, 2) as stddev_rel_ext
    from statcast s
    inner join games g
    on s.game_pk = g.gamepk
    and g.gametype in ('R','W','D','L')
    inner join players p 
    on p.person_id = s.pitcher
    and p.primaryposition_code in ('1','Y')
    where exists (select 1 from teams t where t.team_id = g.away_team_id)
    and exists (select 1 from teams t where t.team_id = g.home_team_id)
    group by s.pitcher, s.game_date, s.pitcher_team, s.game_pk
)

select
    o.outing_id,
    o.pitcher,
    o.pitcher_team,
    o.game_date,
    o.game_pk,
    extract(year from o.game_date) as season,
    o.pitch_count,
    o.avg_horz_rel,
    o.stddev_horz_rel,
    o.avg_vert_rel,
    o.stddev_vert_rel,
    o.avg_rel_ext,
    o.stddev_rel_ext,
    (o.game_date::date - o.last_outing::date) as days_since_last_outing
from outings o