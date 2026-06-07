
  
    

  create  table "praktikum"."marts"."mart_post_event_returns__dbt_tmp"
  
  
    as
  
  (
    -- mart_post_event_returns
--
-- Metric 2 (architecture doc): Average sector returns in the 30 trading days
-- following each ECB rate change decision.
--
-- For every ECB rate change event, we find all trading days in the next 30
-- calendar days and sum up each sector's daily returns over that window.
-- The result is one row per (event_date, ticker).
--
-- rate_change_bp: change in basis points (100 bp = 1 percentage point)
--   positive = rate hike, negative = rate cut

with rate_changes as (
    select
        date_ref                                                          as event_date,
        rate_pct,
        lag(rate_pct) over (order by date_ref)                            as prev_rate_pct,
        round(
            (rate_pct - lag(rate_pct) over (order by date_ref)) * 100,
            2
        )                                                                 as rate_change_bp
    from "praktikum"."staging"."stg_ecb_rates"
),

-- Keep only actual decision dates where the rate changed
events as (
    select *
    from rate_changes
    where rate_change_bp is not null
      and rate_change_bp <> 0
),

returns as (
    select * from "praktikum"."intermediate"."int_sector_returns"
),

sectors as (
    select ticker, sector_name
    from "praktikum"."marts"."dim_sectors"
    where valid_to = '9999-01-01'
),

-- Collect every (event, ticker, trading_day) combination within the 30-day window
post_event_daily as (
    select
        e.event_date,
        e.rate_pct,
        e.prev_rate_pct,
        e.rate_change_bp,
        r.ticker,
        r.price_date,
        r.daily_return_pct
    from events e
    inner join returns r
        on  r.price_date >  e.event_date
        and r.price_date <= e.event_date + 30   -- 30 calendar days after event
),

-- Aggregate to one row per (event, ticker)
aggregated as (
    select
        event_date,
        rate_pct,
        prev_rate_pct,
        rate_change_bp,
        ticker,
        count(*)                        as trading_days_in_window,
        round(sum(daily_return_pct), 4) as cumulative_return_30d_pct
    from post_event_daily
    group by event_date, rate_pct, prev_rate_pct, rate_change_bp, ticker
)

select
    a.event_date,
    a.rate_pct,
    a.prev_rate_pct,
    a.rate_change_bp,
    case
        when a.rate_change_bp > 0 then 'Rate hike'
        when a.rate_change_bp < 0 then 'Rate cut'
    end as rate_change_direction, 
    a.ticker,
    s.sector_name,
    a.trading_days_in_window,
    a.cumulative_return_30d_pct
from aggregated a
left join sectors s on s.ticker = a.ticker
order by a.event_date, a.ticker
  );
  