-- Test de Calidad de Datos:
-- Ningún precio de apertura, cierre, máximo o mínimo debe ser menor o igual a cero.
-- Si esta consulta retorna filas, dbt test fallará automáticamente.

select
    performance_key,
    symbol,
    trade_date,
    open_price,
    close_price,
    high_price,
    low_price
from {{ ref('fct_stock_daily_performance') }}
where open_price <= 0
   or close_price <= 0
   or high_price <= 0
   or low_price <= 0
