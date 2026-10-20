"""
Módulo de carga de datos desde MinIO (Capa Silver) hacia PostgreSQL (Data Warehouse).
Prepara las tablas staging para que dbt Core ejecute las transformaciones a la Capa Gold.
"""

import io
import logging
import os
import pyarrow.parquet as pq
import psycopg2
from psycopg2.extras import execute_values
from typing import Dict, Any

from utils.minio_storage import MinIOStorage

logger = logging.getLogger(__name__)


class DataWarehouseLoader:
    def __init__(self):
        self.db_url = (
            f"postgresql://{os.getenv('POSTGRES_USER', 'postgres')}:"
            f"{os.getenv('POSTGRES_PASSWORD', 'postgrespassword')}@"
            f"{os.getenv('POSTGRES_HOST', 'postgres')}:5432/"
            f"{os.getenv('WAREHOUSE_DB', 'financial_dw')}"
        )
        self.storage = MinIOStorage()

    def get_connection(self):
        return psycopg2.connect(self.db_url)

    def load_silver_to_staging(self) -> Dict[str, int]:
        """
        Lee los archivos Parquet de la capa Silver en MinIO y realiza una carga
        masiva (bulk load) en el esquema staging de PostgreSQL.
        """
        conn = self.get_connection()
        cur = conn.cursor()

        try:
            # 1. Asegurar que exista el esquema staging
            cur.execute("CREATE SCHEMA IF NOT EXISTS staging;")
            conn.commit()

            # 2. Cargar company_listings
            listings_count = self._load_company_listings(cur, conn)

            # 3. Cargar stock_prices
            prices_count = self._load_stock_prices(cur, conn)

            logger.info(
                "Carga a staging finalizada: %d empresas, %d registros de precios.",
                listings_count,
                prices_count,
            )
            return {"listings": listings_count, "prices": prices_count}

        finally:
            cur.close()
            conn.close()

    def _load_company_listings(self, cur, conn) -> int:
        prefix = "financial_market/company_listings/"
        objects = [
            obj for obj in self.storage.list_objects("silver", prefix=prefix)
            if obj.endswith(".parquet")
        ]

        if not objects:
            logger.warning("No se encontraron archivos Parquet para company_listings en Silver.")
            return 0

        # Crear tabla staging si no existe y truncar para carga limpia e idempotente
        cur.execute("""
            CREATE TABLE IF NOT EXISTS staging.raw_company_listings (
                symbol VARCHAR(20),
                company_name VARCHAR(255),
                exchange VARCHAR(50),
                asset_type VARCHAR(50),
                ipo_date DATE,
                delisting_date DATE,
                status VARCHAR(50),
                is_active BOOLEAN,
                _processed_at TIMESTAMP
            );
            TRUNCATE TABLE staging.raw_company_listings;
        """)

        total_rows = 0
        for obj_name in objects:
            response = self.storage.client.get_object("silver", obj_name)
            parquet_data = response.read()
            table = pq.read_table(io.BytesIO(parquet_data))
            df = table.to_pandas()

            records = [
                (
                    row.get("symbol"),
                    row.get("company_name"),
                    row.get("exchange"),
                    row.get("asset_type"),
                    row.get("ipo_date"),
                    row.get("delisting_date"),
                    row.get("status"),
                    bool(row.get("is_active")),
                    row.get("_processed_at"),
                )
                for _, row in df.iterrows()
            ]

            if records:
                insert_query = """
                    INSERT INTO staging.raw_company_listings (
                        symbol, company_name, exchange, asset_type,
                        ipo_date, delisting_date, status, is_active, _processed_at
                    ) VALUES %s
                """
                execute_values(cur, insert_query, records, page_size=2000)
                total_rows += len(records)

        conn.commit()
        logger.info("staging.raw_company_listings cargada exitosamente (%d filas).", total_rows)
        return total_rows

    def _load_stock_prices(self, cur, conn) -> int:
        prefix = "financial_market/stock_prices/"
        objects = [
            obj for obj in self.storage.list_objects("silver", prefix=prefix)
            if obj.endswith(".parquet")
        ]

        if not objects:
            logger.warning("No se encontraron archivos Parquet para stock_prices en Silver.")
            return 0

        # Crear tabla staging si no existe y truncar para carga limpia e idempotente
        cur.execute("""
            CREATE TABLE IF NOT EXISTS staging.raw_stock_prices (
                symbol VARCHAR(20),
                trade_date DATE,
                open_price NUMERIC(14, 4),
                high_price NUMERIC(14, 4),
                low_price NUMERIC(14, 4),
                close_price NUMERIC(14, 4),
                adjusted_close NUMERIC(14, 4),
                volume BIGINT,
                dividend_amount NUMERIC(14, 4),
                split_coefficient NUMERIC(14, 4),
                previous_close NUMERIC(14, 4),
                daily_return_pct NUMERIC(10, 4),
                daily_price_spread NUMERIC(14, 4),
                is_positive_day BOOLEAN,
                moving_avg_7d NUMERIC(14, 4),
                _processed_at TIMESTAMP
            );
            TRUNCATE TABLE staging.raw_stock_prices;
        """)

        total_rows = 0
        for obj_name in objects:
            # Extraer símbolo si viene en la ruta particionada
            symbol_from_path = None
            if "symbol=" in obj_name:
                symbol_from_path = obj_name.split("symbol=")[1].split("/")[0]

            response = self.storage.client.get_object("silver", obj_name)
            parquet_data = response.read()
            table = pq.read_table(io.BytesIO(parquet_data))
            df = table.to_pandas()

            records = []
            for _, row in df.iterrows():
                symbol = row.get("symbol") or symbol_from_path
                records.append((
                    symbol,
                    row.get("trade_date"),
                    row.get("open_price"),
                    row.get("high_price"),
                    row.get("low_price"),
                    row.get("close_price"),
                    row.get("adjusted_close"),
                    int(row.get("volume", 0)),
                    row.get("dividend_amount"),
                    row.get("split_coefficient"),
                    row.get("previous_close"),
                    row.get("daily_return_pct"),
                    row.get("daily_price_spread"),
                    bool(row.get("is_positive_day")),
                    row.get("moving_avg_7d"),
                    row.get("_processed_at"),
                ))

            if records:
                insert_query = """
                    INSERT INTO staging.raw_stock_prices (
                        symbol, trade_date, open_price, high_price, low_price,
                        close_price, adjusted_close, volume, dividend_amount,
                        split_coefficient, previous_close, daily_return_pct,
                        daily_price_spread, is_positive_day, moving_avg_7d, _processed_at
                    ) VALUES %s
                """
                execute_values(cur, insert_query, records, page_size=1000)
                total_rows += len(records)

        conn.commit()
        logger.info("staging.raw_stock_prices cargada exitosamente (%d filas).", total_rows)
        return total_rows
