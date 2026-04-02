with returns as (
    select * from {{ ref('daily_returns') }}
),

rolling as (
    select
        ticker,
        trade_date,
        company_name,
        sector,
        daily_return_pct,
        round(
            stddev(daily_return_pct) over (
                partition by ticker order by trade_date
                rows between 19 preceding and current row
            )::numeric,
            4
        )                                  as rolling_vol_20d,
        count(*) over (
            partition by ticker order by trade_date
            rows between 19 preceding and current row
        )                                  as window_obs
    from returns
)

select
    ticker,
    trade_date,
    company_name,
    sector,
    daily_return_pct,
    rolling_vol_20d,
    round((rolling_vol_20d * sqrt(252))::numeric, 4)  as annualised_vol
from rolling
where window_obs = 20
