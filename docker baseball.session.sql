select int.pitcher_outings.*, 
game_date::date - birthdate::date as age,
  (split_part(height, '''', 1)::int * 12) +
  split_part(split_part(height, '''', 2), '"', 1)::int as height,
  weight
from int.pitcher_outings
join silver.players
on int.pitcher_outings.pitcher = silver.players.person_id
limit 10;