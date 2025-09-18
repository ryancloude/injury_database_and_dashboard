


with src as (
  -- Reuse staging instead of re-writing transforms
  select * from "baseball"."bronze"."stg_statcast"
)

select
    pitch_id,
    game_pk,
   at_bat_number,
   pitch_number,
   game_date,
   game_year,
   pitcher,
   batter,
   description,
   events,
   case
      when pitch_type in ('CS','EP','KC','CU') then 'CU'
      when pitch_type in ('AB','FA','IN','PO','KN') or pitch_type is null then 'OT'
      when pitch_type in ('CH','SC') then 'CH'
      when pitch_type in ('SV','ST') then 'ST'
      when pitch_type in ('FS','FO') then 'FS'
      else pitch_type
    end as pitch_type,
   release_speed,
   release_spin_rate,
   release_pos_x,
   release_pos_z,
   release_extension,
  -- feet -> inches like your SQL (×12)
   pfx_x   * 12 as pfx_x,
   pfx_z   * 12 as pfx_z,
   plate_x * 12 as plate_x,
   plate_z * 12 as plate_z,
   case when inning_topbot in ('Top','T') then away_team
         when inning_topbot in ('Bot','B') then home_team
         else null end as batter_team,
   case when inning_topbot in ('Top','T') then home_team
         when inning_topbot in ('Bot','B') then away_team
         else null end as pitcher_team,
        now()::timestamptz as silver_updated_at
from src

