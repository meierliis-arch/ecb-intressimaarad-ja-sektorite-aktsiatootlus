
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select cumulative_return_30d_pct
from "praktikum"."marts"."mart_post_event_returns"
where cumulative_return_30d_pct is null



  
  
      
    ) dbt_internal_test