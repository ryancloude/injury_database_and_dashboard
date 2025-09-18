
  
    

  create  table "baseball"."silver"."players__dbt_tmp"
  
  
    as
  
  (
    


with src as (
  -- Reuse staging instead of re-writing transforms
  select * from "baseball"."bronze"."stg_players"
)

select *
from src
  );
  