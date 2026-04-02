with prices as (
    select * from {{ ref('stg_nse_prices') }}
),

mas as (
    select
        ticker,
        trade_date,
        close_price,
        round(avg(close_price) over (
            partition by ticker order by trade_date
            rows between 6 preceding and current row
        ), 4)                              as ma_7d,
        round(avg(close_price) over (
            partition by ticker order by trade_date
            rows between 19 preceding and current row
        ), 4)                              as ma_20d,
        round(avg(close_price) over (
            partition by ticker order by trade_date
            rows between 49 preceding and current row
        ), 4)                              as ma_50d
    from prices
)

select
    m.ticker,
    m.trade_date,
    m.close_price,
    m.ma_7d,
    m.ma_20d,
    m.ma_50d,
    case
        when m.close_price > m.ma_20d and m.ma_7d > m.ma_20d then 'Bullish'
        when m.close_price < m.ma_20d and m.ma_7d < m.ma_20d then 'Bearish'
        else 'Neutral'
    end                                    as trend_signal,
    t.company_name,
    t.sector
from mas m
left join {{ ref('nse_tickers') }} t using (ticker)
