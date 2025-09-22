{{config(materialized='view',
        schema='bronze')}}

with src as (
    select * from {{source('bronze','teams')}}
)

select
    id as team_id,
    name,
    teamcode,
    filecode,
    abbreviation,
    locationname as location,
    springleague_id,
    springleague_name,
    venue_id,
    venue_name,
    springvenue_id,
    league_id,
    division_id,
    sport_id

from src