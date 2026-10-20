"""
### DAG: Financial Market Bronze Ingestion
Orquesta la ingesta cruda de datos de mercado desde Alpha Vantage hacia MinIO (Capa Bronze).

Características:
- Ingesta de catálogo de activos (LISTING_STATUS en CSV).
- Ingesta de series diarias ajustadas (TIME_SERIES_DAILY_ADJUSTED en JSON).
- Particionamiento Hive: `ingestion_date={{ ds }}` y `symbol={{ ticker }}`.
- Idempotencia: ejecuciones para la misma fecha sobrescriben la partición exacta sin alterar datos históricos.
- Respeto a cuotas y rate limits de la API.
"""

from datetime import datetime, timedelta
import logging
import os
import time
from typing import List

from airflow.decorators import dag, task
from airflow.models import Variable

# Importar utilidades del proyecto
from utils.minio_storage import MinIOStorage
from utils.alpha_vantage_client import AlphaVantageClient

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


@dag(
    dag_id="financial_bronze_ingestion",
    default_args=DEFAULT_ARGS,
    description="Pipeline de ingesta cruda (Bronze Layer) para datos bursátiles de Alpha Vantage en MinIO",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "bronze", "alpha_vantage", "minio"],
)
def financial_bronze_ingestion_pipeline():
    """
    Flujo de trabajo para poblar la Capa Bronze del Lakehouse.
    """

    @task(task_id="check_minio_connectivity")
    def check_minio() -> bool:
        """Verifica la conectividad con MinIO y asegura que los buckets del Lakehouse existan."""
        storage = MinIOStorage()
        storage.ping()
        for bucket in [BUCKET_BRONZE, "silver", "gold"]:
            storage.ensure_bucket(bucket)
        logger.info("MinIO disponible y buckets (bronze, silver, gold) listos.")
        return True

    @task(task_id="ingest_listing_status")
    def ingest_listing_status(ds: str = None) -> str:
        """
        Extrae el catálogo general de activos (acciones y ETFs) y lo almacena crudo en MinIO.
        """
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
        """
        Extrae la serie temporal diaria para los tickers configurados y guarda cada JSON crudo.
        """
        client = AlphaVantageClient()
        storage = MinIOStorage()

        # Permitir sobrescribir tickers mediante Airflow Variables
        tickers_var = Variable.get("FINANCIAL_TICKERS", default_var=None)
        if tickers_var:
            symbols = [s.strip().upper() for s in tickers_var.split(",") if s.strip()]
        else:
            symbols = DEFAULT_SYMBOLS

        uploaded_paths = []
        logger.info("Iniciando ingesta de series de precios para tickers: %s", symbols)

        for idx, symbol in enumerate(symbols):
            try:
                # Descarga de datos
                data = client.get_daily_time_series(symbol=symbol, outputsize="compact")

                # Ruta particionada estilo Hive
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

                # Pausa preventiva para no saturar el límite gratuito de Alpha Vantage (5 llamadas/min)
                if idx < len(symbols) - 1:
                    logger.info("Esperando 12 segundos para respetar el rate limit de la API...")
                    time.sleep(12)

            except Exception as e:
                logger.error("Error al procesar ticker '%s': %s", symbol, e)
                raise

        return uploaded_paths

    @task(task_id="verify_bronze_ingestion")
    def verify_bronze_layer(listing_path: str, series_paths: List[str], ds: str = None):
        """
        Valida que todos los artefactos de la fecha de ejecución se hayan persistido en MinIO.
        """
        storage = MinIOStorage()
        logger.info("Validando integridad de la Capa Bronze para fecha: %s", ds)

        # 1. Validar listing status
        listing_obj = f"alpha_vantage/listing_status/ingestion_date={ds}/listing_status.csv"
        if not storage.object_exists(BUCKET_BRONZE, listing_obj):
            raise FileNotFoundError(
                f"No se encontró el archivo de catálogo esperado: {listing_obj}"
            )

        # 2. Validar series temporales
        missing = []
        for path in series_paths:
            obj_name = path.replace(f"s3://{BUCKET_BRONZE}/", "")
            if not storage.object_exists(BUCKET_BRONZE, obj_name):
                missing.append(obj_name)

        if missing:
            raise FileNotFoundError(
                f"Faltan los siguientes objetos en la Capa Bronze: {missing}"
            )

        total_ingested = 1 + len(series_paths)
        logger.info(
            "✅ Verificación exitosa de la Capa Bronze. Total de archivos ingestados: %d",
            total_ingested,
        )
        return True

    @task(task_id="trigger_spark_silver_processing")
    def run_spark_silver() -> dict:
        """
        Dispara el procesamiento masivo distribuido en Apache Spark (PySpark)
        para limpiar, tipar y calcular indicadores financieros hacia la Capa Silver.
        """
        import requests

        spark_runner_url = "http://spark-master:5050/run-silver"
        logger.info("Enviando orden de ejecución a Apache Spark: %s...", spark_runner_url)

        try:
            response = requests.post(spark_runner_url, timeout=300)
            response.raise_for_status()
            data = response.json()
            if data.get("status") != "SUCCESS":
                raise RuntimeError(f"Fallo en PySpark Job: {data}")
            logger.info("PySpark Job finalizado exitosamente.")
            return data
        except Exception as e:
            logger.error("Error al ejecutar job en cluster Spark: %s", e)
            raise

    @task(task_id="verify_silver_layer")
    def verify_silver_layer() -> bool:
        """
        Audita que los conjuntos de datos en formato Parquet existan físicamente en s3://silver/.
        """
        storage = MinIOStorage()
        objects = storage.list_objects("silver")
        logger.info("Total de objetos encontrados en s3://silver/: %d", len(objects))

        has_prices = any("stock_prices" in obj for obj in objects)
        has_listings = any("company_listings" in obj for obj in objects)

        if not (has_prices and has_listings):
            raise FileNotFoundError(
                "No se encontraron los artefactos Parquet esperados en s3://silver/."
            )

        logger.info(
            "✅ Capa Silver auditada con éxito: datasets de precios y catálogo de empresas listos en Parquet."
        )
        return True

    # Definición de dependencias en el Grafo (DAG)
    minio_ok = check_minio()
    listing_task = ingest_listing_status()
    series_task = ingest_stock_time_series()
    bronze_verification = verify_bronze_layer(listing_task, series_task)
    spark_silver = run_spark_silver()
    silver_verification = verify_silver_layer()

    minio_ok >> [listing_task, series_task] >> bronze_verification >> spark_silver >> silver_verification


# Instanciación del DAG
financial_bronze_ingestion_pipeline()
