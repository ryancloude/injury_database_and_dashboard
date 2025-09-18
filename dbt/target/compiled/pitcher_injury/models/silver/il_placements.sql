


with src as (
  -- Reuse staging instead of re-writing transforms
  select * from "baseball"."silver"."transactions"
),


temp as (
SELECT DISTINCT ON (person_id, effectivedate)
  *,
  (
    COALESCE(description, '') ~ 'injured list|disabled list'
    AND NOT (COALESCE(description, '') ~ 'activated|reinstated|returned|transferred|recalled')
  ) AS is_il_placement
FROM src)

select * from temp where is_il_placement = true