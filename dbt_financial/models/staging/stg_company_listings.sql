with source as (
    select * from {{ source('staging_raw', 'raw_company_listings') }}
),

cleaned as (
    select
        trim(upper(symbol)) as symbol,
        coalesce(nullif(trim(company_name), ''), 'Unknown Entity (' || trim(upper(symbol)) || ')') as company_name,
        coalesce(nullif(trim(upper(exchange)), ''), 'UNKNOWN') as exchange,
        coalesce(nullif(trim(asset_type), ''), 'Stock') as asset_type,
        ipo_date,
        delisting_date,
        coalesce(nullif(trim(status), ''), 'Active') as status,
        coalesce(is_active, status = 'Active', true) as is_active,
        _processed_at as ingested_at
    from source
    where symbol is not null and symbol != ''
)

select * from cleaned
