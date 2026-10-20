{{ config(materialized='table', schema='gold') }}

with staging as (
    select * from {{ ref('stg_company_listings') }}
)

select
    md5(symbol) as company_key,
    symbol,
    company_name,
    exchange,
    asset_type,
    ipo_date,
    delisting_date,
    status,
    is_active,
    current_timestamp as dim_updated_at
from staging
