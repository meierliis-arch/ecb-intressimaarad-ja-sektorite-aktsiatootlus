
  
    

  create  table "praktikum"."marts"."mart_sector_betas__dbt_tmp"
  
  
    as
  
  (
    -- mart_sector_betas
--
-- Metric 3 (architecture doc): Interest rate sensitivity beta per sector.
-- Answers: "How much does a sector's 30-day return change for every 1 bp rate move?"
--
-- Method: OLS linear regression computed directly in SQL.
--   X = rate_change_bp  (the ECB decision size, in basis points)
--   Y = cumulative_return_30d_pct  (sector return in the 30 days after the decision)
--   β = Cov(X, Y) / Var(X)  = Σ[(xi - x̄)(yi - ȳ)] / Σ[(xi - x̄)²]
--
-- A positive beta means the sector tends to rise when rates are hiked.
-- A negative beta means the sector tends to fall when rates are hiked (rate-sensitive).
-- beta = null when there is too little data to compute (fewer than 2 events).

with source as (
    select
        ticker,
        sector_name,
        rate_change_bp::numeric          as x,
        cumulative_return_30d_pct::numeric as y
    from "praktikum"."marts"."mart_post_event_returns"
    where rate_change_bp         is not null
      and cumulative_return_30d_pct is not null
),

-- Per-sector means (needed to compute deviations)
means as (
    select
        ticker,
        sector_name,
        count(*)  as n,
        avg(x)    as mean_x,
        avg(y)    as mean_y
    from source
    group by ticker, sector_name
),

-- Deviations from the mean per observation
deviations as (
    select
        s.ticker,
        s.sector_name,
        (s.x - m.mean_x) * (s.y - m.mean_y) as cross_dev,
        power(s.x - m.mean_x, 2)             as sq_dev_x
    from source s
    inner join means m on m.ticker = s.ticker
)

select
    d.ticker,
    d.sector_name,
    m.n                                          as event_count,
    round(m.mean_y, 4)                           as avg_30d_return_pct,
    round(
        case
            when m.n < 2              then null   -- not enough data
            when sum(d.sq_dev_x) = 0  then null   -- no variance in X (all events same size)
            else sum(d.cross_dev) / sum(d.sq_dev_x)
        end,
        6
    )                                            as beta
from deviations d
inner join means m on m.ticker = d.ticker
group by d.ticker, d.sector_name, m.n, m.mean_y
order by beta desc nulls last
  );
  