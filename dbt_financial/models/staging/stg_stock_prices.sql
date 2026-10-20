with source as (
    select * from {{ source('staging_raw', 'raw_stock_prices') }}
),

cleaned as (
    select
        trim(upper(symbol)) as symbol,
        trade_date,
        cast(open_price as numeric(14, 4)) as open_price,
        cast(high_price as numeric(14, 4)) as high_price,
        cast(low_price as numeric(14, 4)) as low_price,
        cast(close_price as numeric(14, 4)) as close_price,
        cast(adjusted_close as numeric(14, 4)) as adjusted_close,
        cast(volume as bigint) as volume,
        cast(dividend_amount as numeric(14, 4)) as dividend_amount,
        cast(split_coefficient as numeric(14, 4)) as split_coefficient,
        cast(previous_close as numeric(14, 4)) as previous_close,
        cast(daily_return_pct as numeric(10, 4)) as daily_return_pct,
        cast(daily_price_spread as numeric(14, 4)) as daily_price_spread,
        coalesce(is_positive_day, close_price >= open_price) as is_positive_day,
        cast(moving_avg_7d as numeric(14, 4)) as moving_avg_7d,
        _processed_at as silver_processed_at
    from source
    where symbol is not null and trade_date is not null
)

select * from cleaned
