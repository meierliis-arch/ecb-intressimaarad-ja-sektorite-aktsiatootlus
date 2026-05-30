-- stg_index_prices
--
-- Cleans and deduplicates the raw index price table.
-- Incremental fetches include a 5-day overlap, so the same (ticker, price_date)
-- can appear under multiple run_ids. We keep only the most recently fetched version.

with source as (
    select * from {{ source('staging', 'index_prices_raw') }}
),

deduplicated as (
    select
        ticker,
        price_date,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        fetched_at,
        row_number() over (
            partition by ticker, price_date
            order by fetched_at desc
        ) as row_num
    from source
)

select
    ticker,
    price_date,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    fetched_at
from deduplicated
where row_num = 1
  and close_price is not null
