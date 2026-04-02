{{ config(
    materialized='table',
    schema='silver',
    alias='players'
) }}

with pl as (
  select * from {{ ref('stg_players') }}
),

pr as (
  select distinct person_id, nameascii
  from {{ ref('stg_projections') }}
),

joined as (
  select
    pl.*,
    pr.nameascii,
    row_number() over (
      partition by pl.person_id
      order by
        pl.season desc nulls last,
        pl.last_played_date desc nulls last,
        pl.mlb_debut_date desc nulls last
    ) as rn
  from pl
  left join pr
    on pl.person_id = pr.person_id
)

select *
from joined
where rn = 1