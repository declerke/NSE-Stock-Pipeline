with prices as (
    select * from {{ ref('stg_nse_prices') }}
),

with_prev as (
    select
        ticker,
        trade_date,
        close_price,
        lag(close_price) over (partition by ticker order by trade_date) as prev_close
    from prices
)

select
    w.ticker,
    w.trade_date,
    w.close_price,
    w.prev_close,
    round(
        (w.close_price - w.prev_close) / nullif(w.prev_close, 0) * 100,
        4
    )                                      as daily_return_pct,
    round(
        ln(w.close_price / nullif(w.prev_close, 0)) * 100,
        4
    )                                      as log_return_pct,
    t.company_name,
    t.sector
from with_prev w
left join {{ ref('nse_tickers') }} t using (ticker)
where w.prev_close is not null
