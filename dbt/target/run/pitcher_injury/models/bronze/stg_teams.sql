
  create view "baseball"."bronze"."stg_teams__dbt_tmp"
    
    
  as (
    

with src as (
    select * from "baseball"."bronze"."teams"
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
  );