
  create view "baseball"."bronze"."stg_transactions__dbt_tmp"
    
    
  as (
    

with src as (
    select * from "baseball"."bronze"."transactions"
)

select
    id as trans_id,
    date,
    effectivedate, 
    resolutiondate,
    typecode,
    typedesc,
    description, 
    person_id,  
    toteam_id,
    fromteam_id   
from src
  );