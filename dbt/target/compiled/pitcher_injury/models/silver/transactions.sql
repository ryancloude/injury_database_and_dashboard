


with src as (
  -- Reuse staging instead of re-writing transforms
  select * from "baseball"."bronze"."stg_transactions"
)

select *
from src


  -- Sliding window to pick up late corrections; change 14 -> 7/21/etc. as you like
  where date >= current_date - interval '7 day'
