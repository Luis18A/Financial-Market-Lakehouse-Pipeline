-- ==============================================================================
-- Inicialización de Bases de Datos PostgreSQL
-- 1. Base de datos para Metadatos de Apache Airflow
-- 2. Base de datos para el Data Warehouse Analítico (Capa Gold / dbt)
-- ==============================================================================

CREATE DATABASE airflow;
CREATE DATABASE financial_dw;

-- Permisos y extensiones útiles
\c financial_dw;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

\c airflow;
GRANT ALL PRIVILEGES ON DATABASE airflow TO postgres;
GRANT ALL PRIVILEGES ON DATABASE financial_dw TO postgres;
