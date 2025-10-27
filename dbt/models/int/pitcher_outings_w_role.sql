{{config(materialized='ephemeral',
        schema='int',
        unique_key = ['outing_id'])}}

with pitcher_outings as (
  select * from {{ ref('pitcher_outings') }}
),

statcast as (
    select * from {{ ref('statcast')}}
),


pitcher_ranks as (select pitcher, pitcher_team, game_date, game_pk,
row_number() over(PARTITION BY game_pk, pitcher_team order by at_bat_number, pitch_number) as rn_pitcher
from statcast
),

base as (
select po.*, rn_pitcher
from pitcher_outings po
left join (select * from pitcher_ranks where rn_pitcher = 1) as pr
on po.game_pk = pr.game_pk 
and po.pitcher = pr.pitcher
),

pitcher_roles as (
select pitcher, pitcher_team, game_date, game_pk, outing_id,
case
when rn_pitcher = 1 then 'starter'
else 'reliever'
end as role
from base
)

select *,
case
when lag(role) over (partition by pitcher order by game_date) in (role, Null) then False
else True
end as role_switch
from pitcher_roles