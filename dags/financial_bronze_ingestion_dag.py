"""
### DAG: Financial Market Lakehouse Pipeline (End-to-End)
Orquesta el flujo completo de la arquitectura Medallón para datos bursátiles:
1. Capa Bronze: Ingesta cruda de Alpha Vantage (Listing Status y Time Series Daily) en MinIO (S3).
2. Capa Silver: Procesamiento y transformaciones distribuidas en Apache Spark (PySpark), generando Parquet con Snappy.
3. Capa Gold:
   a. Carga masiva (bulk load) de Parquet a PostgreSQL Staging.
   b. Modelado dimensional en estrella con dbt Core (Star Schema: dim_companies, dim_date, fct_stock_daily_performance, mart_stock_monthly_summary).
   c. Data Quality Gates: 25 pruebas de integridad referencial, unicidad, valores nulos y aserciones de negocio cuantitativas.
"""

from datetime import datetime, timedelta
import logging
import os
import time
from typing import List

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.bash import BashOperator

# Utilidades del proyecto
from utils.minio_storage import MinIOStorage
from utils.alpha_vantage_client import AlphaVantageClient
from utils.dw_loader import DataWarehouseLoader

logger = logging.getLogger(__name__)

# Configuración por defecto del DAG
DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

# Tickers por defecto a monitorear
DEFAULT_SYMBOLS = ["IBM", "AAPL", "MSFT", "GOOGL", "AMZN"]
BUCKET_BRONZE = os.getenv("BUCKET_BRONZE", "bronze")
BUCKET_SILVER = os.getenv("BUCKET_SILVER", "silver")


@dag(
    dag_id="financial_market_lakehouse_pipeline",
    default_args=DEFAULT_ARGS,
    description="Pipeline End-to-End Medallion Lakehouse: Bronze (MinIO) -> Silver (Spark) -> Gold (dbt & PostgreSQL) -> Quality Gates",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "medallion", "bronze", "silver", "gold", "spark", "dbt", "minio"],
)
def financial_market_lakehouse_pipeline():
    """
    Pipeline maestro que orquesta las capas Bronze, Silver y Gold del Lakehouse Financiero.
    """

    # -------------------------------------------------------------------------
    # FASE 1: BRONZE LAYER (Ingesta Cruda)
    # -------------------------------------------------------------------------
    @task(task_id="check_infrastructure_connectivity")
    def check_minio() -> bool:
        """Verifica la conectividad con MinIO y asegura los buckets del Lakehouse."""
        storage = MinIOStorage()
        storage.ping()
        for bucket in [BUCKET_BRONZE, BUCKET_SILVER, "gold"]:
            storage.ensure_bucket(bucket)
        logger.info("MinIO disponible y buckets (bronze, silver, gold) listos.")
        return True

    @task(task_id="ingest_listing_status")
    def ingest_listing_status(ds: str = None) -> str:
        """Extrae el catálogo general de activos (acciones y ETFs) y lo almacena crudo en MinIO."""
        client = AlphaVantageClient()
        storage = MinIOStorage()

        csv_content = client.get_listing_status()
        object_name = f"alpha_vantage/listing_status/ingestion_date={ds}/listing_status.csv"

        s3_path = storage.upload_csv(
            bucket_name=BUCKET_BRONZE,
            object_name=object_name,
            csv_content=csv_content,
            metadata={"ingestion_date": ds, "source": "alpha_vantage_listing_status"},
        )
        return s3_path

    @task(task_id="ingest_stock_time_series")
    def ingest_stock_time_series(ds: str = None) -> List[str]:
        """Extrae la serie temporal diaria para los tickers configurados y guarda cada JSON crudo."""
        client = AlphaVantageClient()
        storage = MinIOStorage()

        tickers_var = Variable.get("FINANCIAL_TICKERS", default_var=None)
        if tickers_var:
            symbols = [s.strip().upper() for s in tickers_var.split(",") if s.strip()]
        else:
            symbols = DEFAULT_SYMBOLS

        uploaded_paths = []
        logger.info("Iniciando ingesta de series de precios para tickers: %s", symbols)

        for idx, symbol in enumerate(symbols):
            try:
                data = client.get_daily_time_series(symbol=symbol, outputsize="compact")

                object_name = (
                    f"alpha_vantage/time_series_daily/symbol={symbol}/"
                    f"ingestion_date={ds}/daily_adjusted.json"
                )

                s3_uri = storage.upload_json(
                    bucket_name=BUCKET_BRONZE,
                    object_name=object_name,
                    data=data,
                    metadata={"ingestion_date": ds, "symbol": symbol},
                )
                uploaded_paths.append(s3_uri)

                if idx < len(symbols) - 1:
                    logger.info("Rate limit Alpha Vantage: esperando 12 segundos...")
                    time.sleep(12)

            except Exception as e:
                logger.error("Error al procesar ticker '%s': %s", symbol, e)
                raise

        return uploaded_paths

    @task(task_id="verify_bronze_ingestion")
    def verify_bronze_layer(listing_path: str, series_paths: List[str], ds: str = None):
        """Valida que todos los artefactos de la fecha de ejecución se hayan persistido en MinIO."""
        storage = MinIOStorage()
        logger.info("Validando integridad de la Capa Bronze para fecha: %s", ds)

        listing_obj = f"alpha_vantage/listing_status/ingestion_date={ds}/listing_status.csv"
        if not storage.object_exists(BUCKET_BRONZE, listing_obj):
            raise FileNotFoundError(f"Falta catálogo de activos: {listing_obj}")

        missing = []
        for path in series_paths:
            obj_name = path.replace(f"s3://{BUCKET_BRONZE}/", "")
            if not storage.object_exists(BUCKET_BRONZE, obj_name):
                missing.append(obj_name)

        if missing:
            raise FileNotFoundError(f"Faltan series en Capa Bronze: {missing}")

        total_ingested = 1 + len(series_paths)
        logger.info("✅ Capa Bronze validada exitosamente. Total archivos: %d", total_ingested)
        return True

    # -------------------------------------------------------------------------
    # FASE 2: SILVER LAYER (Transformación Distribuida con PySpark)
    # -------------------------------------------------------------------------
    @task(task_id="trigger_spark_silver_processing")
    def run_spark_silver() -> dict:
        """Dispara el job distribuido de Apache Spark para limpiar, enriquecer y persistir en Parquet."""
        import requests

        spark_runner_url = "http://spark-master:5050/run-silver"
        logger.info("Disparando ejecución en cluster Spark: %s...", spark_runner_url)

        try:
            response = requests.post(spark_runner_url, timeout=300)
            response.raise_for_status()
            data = response.json()
            if data.get("status") != "SUCCESS":
                raise RuntimeError(f"Fallo en PySpark Job: {data}")
            logger.info("PySpark Job finalizado exitosamente: %s", data.get("message"))
            return data
        except Exception as e:
            logger.error("Error al ejecutar job en cluster Spark: %s", e)
            raise

    @task(task_id="verify_silver_layer")
    def verify_silver_layer() -> bool:
        """Audita que los datasets Parquet existan físicamente en s3://silver/."""
        storage = MinIOStorage()
        objects = storage.list_objects(BUCKET_SILVER)
        logger.info("Total objetos en s3://%s/: %d", BUCKET_SILVER, len(objects))

        has_prices = any("stock_prices" in obj for obj in objects)
        has_listings = any("company_listings" in obj for obj in objects)

        if not (has_prices and has_listings):
            raise FileNotFoundError("Datasets Parquet incompletos en s3://silver/.")

        logger.info("✅ Capa Silver auditada con éxito (Parquet datasets validados).")
        return True

    # -------------------------------------------------------------------------
    # FASE 3: GOLD LAYER (Data Warehouse + dbt Dimensional Modeling)
    # -------------------------------------------------------------------------
    @task(task_id="load_silver_to_staging")
    def load_silver_to_staging() -> dict:
        """Lee los Parquet desde MinIO Silver y realiza bulk load en PostgreSQL staging."""
        loader = DataWarehouseLoader()
        result = loader.load_silver_to_staging()
        logger.info("✅ Carga a staging completada: %s", result)
        return result

    # Transformación dbt: Construye Vistas Staging, Tablas de Dimensiones, Hechos y Marts
    dbt_run_models = BashOperator(
        task_id="dbt_run_gold_models",
        bash_command=(
            "dbt run --profiles-dir /opt/airflow/dbt_financial "
            "--project-dir /opt/airflow/dbt_financial"
        ),
    )

    # Control de Calidad dbt: 25 Pruebas Automatizadas de Integridad y Lógica de Negocio
    dbt_test_gates = BashOperator(
        task_id="dbt_test_quality_gates",
        bash_command=(
            "dbt test --profiles-dir /opt/airflow/dbt_financial "
            "--project-dir /opt/airflow/dbt_financial"
        ),
    )

    @task(task_id="verify_gold_analytics_layer")
    def verify_gold_layer() -> dict:
        """Verifica las métricas finales generadas en la Capa Gold del Data Warehouse."""
        loader = DataWarehouseLoader()
        conn = loader.get_connection()
        cur = conn.cursor()

        try:
            cur.execute("SELECT COUNT(*) FROM gold.dim_companies;")
            companies_count = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM gold.dim_date;")
            dates_count = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM gold.fct_stock_daily_performance;")
            facts_count = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*), ROUND(AVG(win_rate_pct), 2) FROM gold.mart_stock_monthly_summary;")
            marts_row = cur.fetchone()
            marts_count, avg_win_rate = marts_row[0], marts_row[1]

            summary = {
                "dim_companies": companies_count,
                "dim_date": dates_count,
                "fct_stock_daily_performance": facts_count,
                "mart_stock_monthly_summary": marts_count,
                "avg_monthly_win_rate": float(avg_win_rate) if avg_win_rate else 0.0,
            }

            logger.info("==================================================================")
            logger.info("🏆 RESUMEN DE EJECUCIÓN END-TO-END - CAPA GOLD DEL DATA WAREHOUSE:")
            logger.info(" - dim_companies: %d registros", companies_count)
            logger.info(" - dim_date: %d fechas bursátiles", dates_count)
            logger.info(" - fct_stock_daily_performance: %d transacciones analizadas", facts_count)
            logger.info(" - mart_stock_monthly_summary: %d resúmenes mensuales", marts_count)
            logger.info(" - Win Rate Promedio: %s%%", avg_win_rate)
            logger.info("==================================================================")
            return summary

        finally:
            cur.close()
            conn.close()

    # -------------------------------------------------------------------------
    # DEFINICIÓN DEL GRAFO DE DEPENDENCIAS (DAG FLOW)
    # -------------------------------------------------------------------------
    minio_ok = check_minio()
    listing_task = ingest_listing_status()
    series_task = ingest_stock_time_series()
    bronze_verification = verify_bronze_layer(listing_task, series_task)

    spark_silver = run_spark_silver()
    silver_verification = verify_silver_layer()

    staging_loader = load_silver_to_staging()
    gold_audit = verify_gold_layer()

    # Flujo lineal robusto y desacoplado:
    # MinIO Check -> Ingesta Bronze -> Verificación Bronze ->
    # PySpark Silver -> Verificación Silver -> Carga Staging ->
    # dbt Run (Gold Star Schema) -> dbt Test (25 Quality Gates) -> Verificación Gold
    (
        minio_ok
        >> [listing_task, series_task]
        >> bronze_verification
        >> spark_silver
        >> silver_verification
        >> staging_loader
        >> dbt_run_models
        >> dbt_test_gates
        >> gold_audit
    )


# Instanciación
financial_market_lakehouse_pipeline()
