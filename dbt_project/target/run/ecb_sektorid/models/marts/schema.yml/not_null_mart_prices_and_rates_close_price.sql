
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select close_price
from "praktikum"."marts"."mart_prices_and_rates"
where close_price is null



  
  
      
    ) dbt_internal_test