
  create view "praktikum"."intermediate"."int_aligned_ecb_rates__dbt_tmp"
    
    
  as (
    -- int_aligned_ecb_rates
--
-- Forward-fills the ECB deposit facility rate to every trading day.
-- ECB decisions happen a few times per year; markets trade every weekday.
-- For each trading day, we take the most recent ECB rate that was in effect
-- on or before that date (last-observation-carried-forward / LOCF).
--
-- Example:
--   ECB sets rate on 2023-09-14 → that rate applies to all trading days
--   from 2023-09-14 until the next ECB decision date.

with rates as (
    select * from "praktikum"."staging"."stg_ecb_rates"
),

trading_days as (
    select distinct price_date
    from "praktikum"."staging"."stg_index_prices"
),

-- For each trading day, find the most recent ECB decision date on or before it
latest_rate_date_per_day as (
    select
        t.price_date,
        max(r.date_ref) as effective_rate_date
    from trading_days t
    inner join rates r on r.date_ref <= t.price_date
    group by t.price_date
)

select
    l.price_date,
    l.effective_rate_date,
    r.rate_pct as ecb_rate_pct
from latest_rate_date_per_day l
inner join rates r on r.date_ref = l.effective_rate_date
  );