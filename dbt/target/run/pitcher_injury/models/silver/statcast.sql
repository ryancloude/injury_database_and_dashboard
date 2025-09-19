
      -- back compat for old kwarg name
  
  
        
            
                
                
            
        
    

    

    merge into "baseball"."silver"."statcast" as DBT_INTERNAL_DEST
        using "statcast__dbt_tmp152555602968" as DBT_INTERNAL_SOURCE
        on (
                    DBT_INTERNAL_SOURCE.pitch_id = DBT_INTERNAL_DEST.pitch_id
                )

    
    when matched then update set
        "pitch_id" = DBT_INTERNAL_SOURCE."pitch_id","game_pk" = DBT_INTERNAL_SOURCE."game_pk","at_bat_number" = DBT_INTERNAL_SOURCE."at_bat_number","pitch_number" = DBT_INTERNAL_SOURCE."pitch_number","game_date" = DBT_INTERNAL_SOURCE."game_date","game_year" = DBT_INTERNAL_SOURCE."game_year","pitcher" = DBT_INTERNAL_SOURCE."pitcher","batter" = DBT_INTERNAL_SOURCE."batter","description" = DBT_INTERNAL_SOURCE."description","events" = DBT_INTERNAL_SOURCE."events","pitch_type" = DBT_INTERNAL_SOURCE."pitch_type","release_speed" = DBT_INTERNAL_SOURCE."release_speed","release_spin_rate" = DBT_INTERNAL_SOURCE."release_spin_rate","release_pos_x" = DBT_INTERNAL_SOURCE."release_pos_x","release_pos_z" = DBT_INTERNAL_SOURCE."release_pos_z","release_extension" = DBT_INTERNAL_SOURCE."release_extension","pfx_x" = DBT_INTERNAL_SOURCE."pfx_x","pfx_z" = DBT_INTERNAL_SOURCE."pfx_z","plate_x" = DBT_INTERNAL_SOURCE."plate_x","plate_z" = DBT_INTERNAL_SOURCE."plate_z","batter_team" = DBT_INTERNAL_SOURCE."batter_team","pitcher_team" = DBT_INTERNAL_SOURCE."pitcher_team","silver_updated_at" = DBT_INTERNAL_SOURCE."silver_updated_at"
    

    when not matched then insert
        ("pitch_id", "game_pk", "at_bat_number", "pitch_number", "game_date", "game_year", "pitcher", "batter", "description", "events", "pitch_type", "release_speed", "release_spin_rate", "release_pos_x", "release_pos_z", "release_extension", "pfx_x", "pfx_z", "plate_x", "plate_z", "batter_team", "pitcher_team", "silver_updated_at")
    values
        ("pitch_id", "game_pk", "at_bat_number", "pitch_number", "game_date", "game_year", "pitcher", "batter", "description", "events", "pitch_type", "release_speed", "release_spin_rate", "release_pos_x", "release_pos_z", "release_extension", "pfx_x", "pfx_z", "plate_x", "plate_z", "batter_team", "pitcher_team", "silver_updated_at")


  