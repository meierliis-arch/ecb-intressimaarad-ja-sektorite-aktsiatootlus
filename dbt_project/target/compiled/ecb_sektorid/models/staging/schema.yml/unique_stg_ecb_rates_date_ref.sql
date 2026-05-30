
    
    

select
    date_ref as unique_field,
    count(*) as n_records

from "praktikum"."staging"."stg_ecb_rates"
where date_ref is not null
group by date_ref
having count(*) > 1


