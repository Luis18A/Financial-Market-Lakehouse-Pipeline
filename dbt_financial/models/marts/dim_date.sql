{{ config(materialized='table', schema='gold') }}

with distinct_dates as (
    select distinct trade_date as date_day
    from {{ ref('stg_stock_prices') }}
    where trade_date is not null
)

select
    cast(to_char(date_day, 'YYYYMMDD') as integer) as date_key,
    date_day,
    cast(extract(year from date_day) as integer) as year,
    cast(extract(quarter from date_day) as integer) as quarter,
    cast(extract(month from date_day) as integer) as month,
    to_char(date_day, 'FMMonth') as month_name,
    cast(extract(day from date_day) as integer) as day_of_month,
    to_char(date_day, 'FMDay') as day_of_week_name,
    case
        when extract(isodow from date_day) in (6, 7) then true
        else false
    end as is_weekend
from distinct_dates
