
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select ecb_rate_pct
from "praktikum"."intermediate"."int_aligned_ecb_rates"
where ecb_rate_pct is null



  
  
      
    ) dbt_internal_test