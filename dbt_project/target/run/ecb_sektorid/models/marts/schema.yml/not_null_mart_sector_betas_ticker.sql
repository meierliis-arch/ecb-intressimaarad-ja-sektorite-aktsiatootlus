
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select ticker
from "praktikum"."marts"."mart_sector_betas"
where ticker is null



  
  
      
    ) dbt_internal_test