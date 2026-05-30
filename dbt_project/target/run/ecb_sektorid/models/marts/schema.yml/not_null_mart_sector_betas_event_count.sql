
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select event_count
from "praktikum"."marts"."mart_sector_betas"
where event_count is null



  
  
      
    ) dbt_internal_test