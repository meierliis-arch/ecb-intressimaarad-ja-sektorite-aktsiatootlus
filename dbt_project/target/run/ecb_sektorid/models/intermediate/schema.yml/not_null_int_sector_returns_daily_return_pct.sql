
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select daily_return_pct
from "praktikum"."intermediate"."int_sector_returns"
where daily_return_pct is null



  
  
      
    ) dbt_internal_test