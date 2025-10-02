with pitcher_ranks as (select pitcher, pitcher_team, game_date, game_pk,
row_number() over(PARTITION BY game_pk, pitcher_team order by at_bat_number, pitch_number) as rn_pitcher
from silver.statcast),

base as (
select po.*, rn_pitcher
from int.pitcher_outings_w_team_games po
left join (select * from pitcher_ranks where rn_pitcher = 1) as pr
on po.game_pk = pr.game_pk 
and po.pitcher = pr.pitcher),

select pitcher, pitcher_team, game_date, game_pk, season, days_since_last_outing, team_game_num,
case
when rn_pitcher = 1 then 'starter'
else 'reliever'
end as role
from base

