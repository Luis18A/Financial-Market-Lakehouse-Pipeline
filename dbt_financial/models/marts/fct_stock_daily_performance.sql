{{ config(materialized='table', schema='gold') }}

with prices as (
    select * from {{ ref('stg_stock_prices') }}
),

companies as (
    select * from {{ ref('dim_companies') }}
),

dates as (
    select * from {{ ref('dim_date') }}
)

select
    md5(concat(p.symbol, '_', cast(p.trade_date as text))) as performance_key,
    coalesce(c.company_key, md5(p.symbol)) as company_key,
    coalesce(d.date_key, cast(to_char(p.trade_date, 'YYYYMMDD') as integer)) as date_key,
    p.symbol,
    p.trade_date,
    p.open_price,
    p.high_price,
    p.low_price,
    p.close_price,
    p.adjusted_close,
    p.volume,
    p.daily_return_pct,
    p.daily_price_spread,
    p.is_positive_day,
    p.moving_avg_7d,
    case
        when p.daily_return_pct >= 2.0 then 'Bullish High'
        when p.daily_return_pct > 0.0 then 'Bullish Mild'
        when p.daily_return_pct = 0.0 then 'Neutral'
        when p.daily_return_pct > -2.0 then 'Bearish Mild'
        else 'Bearish High'
    end as performance_tier,
    current_timestamp as fct_created_at
from prices p
left join companies c on p.symbol = c.symbol
left join dates d on p.trade_date = d.date_day
