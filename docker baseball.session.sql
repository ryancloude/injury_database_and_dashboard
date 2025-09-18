SELECT distinct CAST(gamedate as date) as game_date
from bronze.games g
WHERE gametype in ('R','F', 'D', 'L', 'W') and status_statuscode = 'F'
and not exists (select 1 from bronze.statcast s where s.game_pk = g.gamepk)
order by game_date asc;
