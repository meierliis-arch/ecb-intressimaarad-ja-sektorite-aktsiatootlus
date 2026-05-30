
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select date_ref
from "praktikum"."staging"."stg_ecb_rates"
where date_ref is null



  
  
      
    ) dbt_internal_test