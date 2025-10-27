{{config(materialized='view',
        schema='int')}}

with statcast as (
    select * from {{ ref('statcast') }}
),

players as (
    select * from {{ ref('players') }}
),


outings as (
select pitcher, game_date, pitch_type, count(distinct pitch_id) as pitch_count,
ROUND(avg(release_speed)::numeric,2) as avg_velo,
ROUND(stddev_pop(release_speed)::numeric,2) as stddev_velo,

ROUND(avg(release_spin_rate)::numeric,2) as avg_spin_rate,
ROUND(stddev_pop(release_spin_rate)::numeric,2) as stddev_spin_rate,

ROUND(avg(pfx_x)::numeric,2) as avg_horz_mov,
ROUND(stddev_pop(pfx_x)::numeric,2) as stddev_horz_mov,

ROUND(avg(pfx_z)::numeric,2) as avg_vert_mov,
ROUND(stddev_pop(pfx_z)::numeric,2) as stddev_vert_mov,

ROUND(avg(release_pos_x)::numeric,2) as avg_horz_rel,
ROUND(stddev_pop(release_pos_x)::numeric,2) as stddev_horz_rel,

ROUND(avg(release_pos_z)::numeric,2) as avg_vert_rel, 
ROUND(stddev_pop(release_pos_z)::numeric,2) as stddev_vert_rel,

ROUND(avg(release_extension)::numeric,2) as avg_rel_ext,
ROUND(stddev_pop(release_extension)::numeric,2) as stddev_rel_ext

from statcast
GROUP BY pitcher, game_date, pitch_type
)

select pitcher, game_date, pitch_type, pitch_count, 
avg_velo, stddev_velo,
avg_spin_rate, stddev_spin_rate,
avg_horz_mov, stddev_horz_mov,
avg_vert_mov, stddev_vert_mov,
avg_horz_rel, stddev_horz_rel,
avg_vert_rel, stddev_vert_rel, 
avg_rel_ext, stddev_rel_ext,
ROUND(stddev_horz_rel + stddev_vert_rel + stddev_rel_ext, 2) as stddev_rel_total
from outings