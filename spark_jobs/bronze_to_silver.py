"""
PySpark Job: Bronze to Silver (Financial Market Lakehouse)

Pipeline Medallón - Capa Silver (Cleaned / Curated):
1. Ingesta distribuida y parseo resiliente de JSONs multi-línea (Time Series Daily).
2. Limpieza de datos (filtrado de nulos, eliminación de duplicados por symbol + trade_date).
3. Casteo estricto de tipos de datos (DateType, DoubleType, LongType).
4. Enriquecimiento financiero mediante Funciones de Ventana (Window Functions):
   - previous_close: precio de cierre anterior
   - daily_return_pct: variación porcentual diaria ((close - prev) / prev * 100)
   - daily_price_spread: volatilidad intra-día (high - low)
   - is_positive_day: indicador booleano de jornada alcista
   - moving_avg_7d: promedio móvil de 7 sesiones
5. Procesamiento y estandarización del catálogo maestro de activos (LISTING_STATUS).
6. Persistencia optimizada en formato Parquet columnar (Snappy) en s3a://silver/.
"""

import json
import logging
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    current_timestamp,
    lag,
    lit,
    round,
    to_date,
    trim,
    upper,
    when,
)
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

# Configuración de Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("BronzeToSilverSparkJob")


def get_spark_session() -> SparkSession:
    """Crea e inicializa la sesión de Spark con el conector S3A para MinIO."""
    minio_endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
    if not minio_endpoint.startswith("http"):
        minio_endpoint = f"http://{minio_endpoint}"

    access_key = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

    logger.info("Inicializando SparkSession conectando a MinIO en: %s", minio_endpoint)

    builder = (
        SparkSession.builder.appName("Financial-Market-Lakehouse-Bronze-To-Silver")
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _parse_time_series_record(record):
    """Función de mapeo distribuido para extraer filas tabulares de cada JSON crudo."""
    path, content = record
    try:
        data = json.loads(content)
        symbol = (
            data.get("Meta Data", {}).get("2. Symbol")
            or data.get("_metadata", {}).get("symbol")
            or "UNKNOWN"
        ).strip().upper()

        time_series = data.get("Time Series (Daily)", {})
        rows = []
        for date_str, metrics in time_series.items():
            try:
                open_p = float(metrics.get("1. open", 0.0))
                high_p = float(metrics.get("2. high", 0.0))
                low_p = float(metrics.get("3. low", 0.0))
                close_p = float(metrics.get("4. close", 0.0))
                adj_close = float(metrics.get("5. adjusted close", close_p))
                vol = int(metrics.get("6. volume", 0))
                div_amt = float(metrics.get("7. dividend amount", 0.0))
                split_c = float(metrics.get("8. split coefficient", 1.0))

                rows.append((
                    symbol,
                    date_str,
                    open_p,
                    high_p,
                    low_p,
                    close_p,
                    adj_close,
                    vol,
                    div_amt,
                    split_c,
                ))
            except (ValueError, TypeError):
                continue
        return rows
    except Exception:
        return []


def process_stock_prices(spark: SparkSession) -> int:
    """
    Procesa las series temporales de la capa Bronze, calcula indicadores
    financieros y almacena en Parquet particionado en la capa Silver.
    """
    bronze_path = "s3a://bronze/alpha_vantage/time_series_daily/*/*/*.json"
    silver_output = "s3a://silver/financial_market/stock_prices"

    logger.info("Leyendo datos crudos de precios desde: %s", bronze_path)

    # 1. Lectura distribuida de archivos multi-línea mediante RDD wholeTextFiles
    raw_files_rdd = spark.sparkContext.wholeTextFiles(bronze_path)
    parsed_rows_rdd = raw_files_rdd.flatMap(_parse_time_series_record)

    schema = StructType([
        StructField("symbol", StringType(), False),
        StructField("trade_date_str", StringType(), False),
        StructField("open_price", DoubleType(), True),
        StructField("high_price", DoubleType(), True),
        StructField("low_price", DoubleType(), True),
        StructField("close_price", DoubleType(), True),
        StructField("adjusted_close", DoubleType(), True),
        StructField("volume", LongType(), True),
        StructField("dividend_amount", DoubleType(), True),
        StructField("split_coefficient", DoubleType(), True),
    ])

    df_base = spark.createDataFrame(parsed_rows_rdd, schema)
    df_typed = df_base.withColumn("trade_date", to_date(col("trade_date_str"), "yyyy-MM-dd")).drop("trade_date_str")

    # 2. Limpieza de datos y deduplicación
    df_clean = (
        df_typed.filter(
            col("symbol").isNotNull()
            & (col("symbol") != "UNKNOWN")
            & col("trade_date").isNotNull()
            & (col("close_price") > 0.0)
        )
        .dropDuplicates(["symbol", "trade_date"])
    )

    # 3. Cálculo de Indicadores Financieros con Funciones de Ventana
    window_spec = Window.partitionBy("symbol").orderBy("trade_date")
    window_7d = Window.partitionBy("symbol").orderBy("trade_date").rowsBetween(-6, 0)

    df_enriched = (
        df_clean.withColumn("previous_close", lag("adjusted_close", 1).over(window_spec))
        .withColumn(
            "daily_return_pct",
            when(
                col("previous_close").isNotNull() & (col("previous_close") > 0),
                round(
                    ((col("adjusted_close") - col("previous_close")) / col("previous_close")) * 100,
                    4,
                ),
            ).otherwise(lit(0.0)),
        )
        .withColumn("daily_price_spread", round(col("high_price") - col("low_price"), 4))
        .withColumn("is_positive_day", when(col("close_price") >= col("open_price"), True).otherwise(False))
        .withColumn("moving_avg_7d", round(avg("adjusted_close").over(window_7d), 4))
        .withColumn("_processed_at", current_timestamp())
    )

    record_count = df_enriched.count()
    logger.info("Total de registros limpios y calculados: %d", record_count)

    # 4. Persistencia en Capa Silver en formato Parquet particionado por símbolo
    logger.info("Escribiendo datos limpios en formato Parquet a: %s", silver_output)
    (
        df_enriched.write.mode("overwrite")
        .partitionBy("symbol")
        .parquet(silver_output)
    )

    logger.info("✅ Series de precios procesadas y persistidas exitosamente en Capa Silver.")
    return record_count


def process_company_listings(spark: SparkSession) -> int:
    """
    Procesa el catálogo de empresas de la capa Bronze, tipa columnas
    y almacena la dimensión limpia en Parquet en la capa Silver.
    """
    bronze_csv_path = "s3a://bronze/alpha_vantage/listing_status/*/*.csv"
    silver_output = "s3a://silver/financial_market/company_listings"

    logger.info("Leyendo catálogo maestro de empresas desde: %s", bronze_csv_path)

    df_raw = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .csv(bronze_csv_path)
    )

    df_clean = (
        df_raw.select(
            upper(trim(col("symbol"))).alias("symbol"),
            trim(col("name")).alias("company_name"),
            upper(trim(col("exchange"))).alias("exchange"),
            trim(col("assetType")).alias("asset_type"),
            to_date(col("ipoDate"), "yyyy-MM-dd").alias("ipo_date"),
            when(col("delistingDate") == "null", lit(None))
            .otherwise(to_date(col("delistingDate"), "yyyy-MM-dd"))
            .alias("delisting_date"),
            trim(col("status")).alias("status"),
            when(col("status") == "Active", True).otherwise(False).alias("is_active"),
            current_timestamp().alias("_processed_at"),
        )
        .filter(col("symbol").isNotNull())
        .dropDuplicates(["symbol"])
    )

    record_count = df_clean.count()
    logger.info("Total de empresas y activos catalogados: %d", record_count)

    logger.info("Escribiendo catálogo en formato Parquet a: %s", silver_output)
    df_clean.write.mode("overwrite").parquet(silver_output)

    logger.info("✅ Company listings procesado y persistido exitosamente en Capa Silver.")
    return record_count


def main():
    logger.info("=== Iniciando Pipeline PySpark Capa Silver ===")
    spark = get_spark_session()

    try:
        prices_count = process_stock_prices(spark)
        listings_count = process_company_listings(spark)

        logger.info(
            "=== Resumen de Ejecución Silver ===\n"
            "- Precios y métricas procesadas: %d registros\n"
            "- Activos y empresas catalogadas: %d registros\n"
            "Estado: ÉXITO TOTAL",
            prices_count,
            listings_count,
        )
    except Exception as e:
        logger.error("Fallo durante el procesamiento en Spark: %s", e, exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()
        logger.info("SparkSession cerrada limpiamente.")


if __name__ == "__main__":
    main()
