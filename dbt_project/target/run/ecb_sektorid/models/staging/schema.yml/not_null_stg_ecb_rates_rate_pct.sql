
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select rate_pct
from "praktikum"."staging"."stg_ecb_rates"
where rate_pct is null



  
  
      
    ) dbt_internal_test