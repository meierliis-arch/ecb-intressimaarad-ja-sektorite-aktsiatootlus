-- stg_ecb_rates
--
-- Cleans and deduplicates the raw ECB rate table.
-- Since the full history is re-inserted on every Airflow run (append-only),
-- the same date_ref can appear multiple times with different run_ids.
-- We keep only the most recently fetched version of each date.

with source as (
    select * from {{ source('staging', 'ecb_rates_raw') }}
),

deduplicated as (
    select
        date_ref,
        rate_pct,
        fetched_at,
        row_number() over (
            partition by date_ref
            order by fetched_at desc
        ) as row_num
    from source
)

select
    date_ref,
    rate_pct,
    fetched_at
from deduplicated
where row_num = 1
