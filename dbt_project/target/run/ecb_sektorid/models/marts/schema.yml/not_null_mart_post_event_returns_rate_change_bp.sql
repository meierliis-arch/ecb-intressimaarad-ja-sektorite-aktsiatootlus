
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select rate_change_bp
from "praktikum"."marts"."mart_post_event_returns"
where rate_change_bp is null



  
  
      
    ) dbt_internal_test