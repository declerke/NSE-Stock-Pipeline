with source as (
    select
        ticker,
        trade_date,
        open,
        high,
        low,
        close,
        volume,
        loaded_at
    from {{ source('raw', 'nse_prices') }}
),

cleaned as (
    select
        ticker,
        trade_date::date                 as trade_date,
        round(open::numeric, 4)          as open_price,
        round(high::numeric, 4)          as high_price,
        round(low::numeric, 4)           as low_price,
        round(close::numeric, 4)         as close_price,
        coalesce(volume, 0)              as volume,
        loaded_at
    from source
    where close is not null
      and close > 0
      and trade_date is not null
)

select * from cleaned
