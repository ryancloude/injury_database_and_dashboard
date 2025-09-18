
    
    

select
    trans_id as unique_field,
    count(*) as n_records

from "baseball"."bronze"."stg_transactions"
where trans_id is not null
group by trans_id
having count(*) > 1


