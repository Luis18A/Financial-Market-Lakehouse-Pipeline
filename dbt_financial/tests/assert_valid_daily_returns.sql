-- Test de Calidad de Datos:
-- Las variaciones porcentuales diarias no pueden ser inferiores al -100% (pérdida total).
-- Si retorna registros, dbt test fallará indicando anomalías graves de cálculo.

select
    performance_key,
    symbol,
    trade_date,
    daily_return_pct
from {{ ref('fct_stock_daily_performance') }}
where daily_return_pct < -100.0
