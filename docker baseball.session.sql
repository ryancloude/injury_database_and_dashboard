with outings as (
select pitcher, pitcher_team, game_date, count(distinct pitch_id) as pitch_count,
lag(game_date) over (PARTITION BY pitcher order by game_date asc) as last_outing,
ROUND(avg(release_pos_x)::numeric,2) as avg_horz_rel,
ROUND(stddev_pop(release_pos_x)::numeric,2) as stddev_horz_rel,
ROUND(avg(release_pos_z)::numeric,2) as avg_vert_rel, 
ROUND(stddev_pop(release_pos_z)::numeric,2) as stddev_vert_rel,
ROUND(avg(release_extension)::numeric,2) as avg_rel_ext,
ROUND(stddev_pop(release_extension)::numeric,2) as stddev_rel_ext
from silver.statcast
GROUP BY pitcher, game_date, pitcher_team
)

select pitcher, pitcher_team, game_date, pitch_count, avg_horz_rel, stddev_horz_rel,
avg_vert_rel, stddev_vert_rel, avg_rel_ext, stddev_rel_ext,
game_date::date - last_outing::date as days_since_last_outing
from outings
where pitcher in (select distinct person_id from silver.players where primaryposition_code = '1' or primaryposition_code = 'Y')
limit 10