{{ config(materialized='table', schema='gold') }}

with daily as (
    select * from {{ ref('fct_stock_daily_performance') }}
),

companies as (
    select * from {{ ref('dim_companies') }}
)

select
    d.symbol,
    coalesce(c.company_name, d.symbol) as company_name,
    to_char(d.trade_date, 'YYYY-MM') as year_month,
    count(distinct d.trade_date) as trading_days_count,
    round(avg(d.daily_return_pct), 4) as avg_daily_return_pct,
    round(avg(d.daily_price_spread), 4) as avg_volatility_spread,
    max(d.high_price) as monthly_high_price,
    min(d.low_price) as monthly_low_price,
    sum(d.volume) as total_volume_traded,
    sum(case when d.is_positive_day then 1 else 0 end) as positive_days_count,
    round(
        cast(sum(case when d.is_positive_day then 1 else 0 end) as numeric) / count(distinct d.trade_date) * 100,
        2
    ) as win_rate_pct,
    current_timestamp as mart_updated_at
from daily d
left join companies c on d.symbol = c.symbol
group by d.symbol, coalesce(c.company_name, d.symbol), to_char(d.trade_date, 'YYYY-MM')
