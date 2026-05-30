
    
    

select
    ticker as unique_field,
    count(*) as n_records

from "praktikum"."marts"."mart_sector_betas"
where ticker is not null
group by ticker
having count(*) > 1


