
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select event_date
from "praktikum"."marts"."mart_post_event_returns"
where event_date is null



  
  
      
    ) dbt_internal_test