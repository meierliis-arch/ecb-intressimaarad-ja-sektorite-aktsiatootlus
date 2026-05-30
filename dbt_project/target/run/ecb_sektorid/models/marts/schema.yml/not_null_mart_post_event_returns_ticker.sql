
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select ticker
from "praktikum"."marts"."mart_post_event_returns"
where ticker is null



  
  
      
    ) dbt_internal_test