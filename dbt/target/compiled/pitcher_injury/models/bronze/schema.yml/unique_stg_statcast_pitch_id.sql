
    
    

select
    pitch_id as unique_field,
    count(*) as n_records

from "baseball"."bronze"."stg_statcast"
where pitch_id is not null
group by pitch_id
having count(*) > 1


