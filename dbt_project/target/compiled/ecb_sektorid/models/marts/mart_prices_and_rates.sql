-- mart_prices_and_rates
--
-- Metric 1 (architecture doc): Descriptive chart showing sector index prices
-- and the ECB rate level over the observed period.
--
-- Joins daily closing prices with the forward-filled ECB rate so that
-- every row has both the sector price and the rate that was in effect that day.
-- This is the primary input for the time-series visualisation in Superset.

with prices as (
    select * from "praktikum"."staging"."stg_index_prices"
),

rates as (
    select * from "praktikum"."intermediate"."int_aligned_ecb_rates"
),

sectors as (
    select ticker, sector_name
    from "praktikum"."marts"."dim_sectors"
    where valid_to = '9999-01-01'
)

select
    p.price_date,
    p.ticker,
    s.sector_name,
    p.close_price,
    r.ecb_rate_pct,
    r.effective_rate_date
from prices p
left join rates   r on r.price_date = p.price_date
left join sectors s on s.ticker     = p.ticker
order by p.price_date, p.ticker