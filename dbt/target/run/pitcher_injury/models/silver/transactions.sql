
      -- back compat for old kwarg name
  
  
        
            
                
                
            
        
    

    

    merge into "baseball"."silver"."transactions" as DBT_INTERNAL_DEST
        using "transactions__dbt_tmp152555086412" as DBT_INTERNAL_SOURCE
        on (
                    DBT_INTERNAL_SOURCE.trans_id = DBT_INTERNAL_DEST.trans_id
                )

    
    when matched then update set
        "trans_id" = DBT_INTERNAL_SOURCE."trans_id","date" = DBT_INTERNAL_SOURCE."date","effectivedate" = DBT_INTERNAL_SOURCE."effectivedate","resolutiondate" = DBT_INTERNAL_SOURCE."resolutiondate","typecode" = DBT_INTERNAL_SOURCE."typecode","typedesc" = DBT_INTERNAL_SOURCE."typedesc","description" = DBT_INTERNAL_SOURCE."description","person_id" = DBT_INTERNAL_SOURCE."person_id","toteam_id" = DBT_INTERNAL_SOURCE."toteam_id","fromteam_id" = DBT_INTERNAL_SOURCE."fromteam_id"
    

    when not matched then insert
        ("trans_id", "date", "effectivedate", "resolutiondate", "typecode", "typedesc", "description", "person_id", "toteam_id", "fromteam_id")
    values
        ("trans_id", "date", "effectivedate", "resolutiondate", "typecode", "typedesc", "description", "person_id", "toteam_id", "fromteam_id")


  