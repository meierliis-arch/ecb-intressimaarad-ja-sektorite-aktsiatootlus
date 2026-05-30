-- int_sector_returns
--
-- Calculates the daily percentage return for each sector ETF.
-- Return = (close_price - prev_close_price) / prev_close_price * 100
--
-- Uses LAG() partitioned by ticker so each sector's return series is independent.
-- The first trading day per ticker has no return (prev_close is null) and is excluded.

with prices as (
    select * from "praktikum"."staging"."stg_index_prices"
),

with_prev_close as (
    select
        ticker,
        price_date,
        close_price,
        lag(close_price) over (
            partition by ticker
            order by price_date
        ) as prev_close_price
    from prices
)

select
    ticker,
    price_date,
    close_price,
    prev_close_price,
    round(
        (close_price - prev_close_price) / prev_close_price * 100,
        6
    ) as daily_return_pct
from with_prev_close
where prev_close_price is not null
  and prev_close_price > 0