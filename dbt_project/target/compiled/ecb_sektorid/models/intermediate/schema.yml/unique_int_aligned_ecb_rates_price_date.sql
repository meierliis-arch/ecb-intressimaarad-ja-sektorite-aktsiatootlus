
    
    

select
    price_date as unique_field,
    count(*) as n_records

from "praktikum"."intermediate"."int_aligned_ecb_rates"
where price_date is not null
group by price_date
having count(*) > 1


