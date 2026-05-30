
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select close_price
from "praktikum"."staging"."stg_index_prices"
where close_price is null



  
  
      
    ) dbt_internal_test