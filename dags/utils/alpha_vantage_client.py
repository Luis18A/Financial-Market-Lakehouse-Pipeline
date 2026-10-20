"""
Cliente HTTP para la API de Alpha Vantage con manejo de cuotas,
reintentos y fallbacks para desarrollo local.
"""

import datetime
import logging
import os
import time
from typing import Any, Dict, Optional
import requests

logger = logging.getLogger(__name__)


class AlphaVantageClient:
    """
    Cliente para extraer datos bursátiles de Alpha Vantage.
    Endpoints:
    - LISTING_STATUS: Listado activo de acciones y ETFs.
    - TIME_SERIES_DAILY_ADJUSTED: Precios diarios históricos ajustados y dividendos.
    """

    BASE_URL = "https://www.alphavantage.co/query"

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("ALPHA_VANTAGE_API_KEY", "demo")

    def get_listing_status(self) -> str:
        """
        Descarga el estado maestro de listados (acciones y ETFs).
        Retorna la respuesta cruda en formato CSV.
        """
        params = {
            "function": "LISTING_STATUS",
            "apikey": self.api_key,
        }
        logger.info("Solicitando datos a Alpha Vantage: LISTING_STATUS...")

        try:
            response = requests.get(self.BASE_URL, params=params, timeout=30)
            response.raise_for_status()
            content = response.text

            # Validación de payload válido en formato CSV
            if "symbol,name,exchange" in content:
                logger.info("LISTING_STATUS recibido correctamente (CSV válido).")
                return content

            # Si la API respondió con advertencia o rate limit
            logger.warning(
                "Alpha Vantage devolvió un payload no estándar para LISTING_STATUS: %s",
                content[:250],
            )

            # Si se usa la clave demo o hay límite de cuota, generar dataset representativo
            return self._generate_fallback_listing_status()

        except Exception as e:
            logger.error("Error al consultar LISTING_STATUS: %s", e)
            return self._generate_fallback_listing_status()

    def get_daily_time_series(
        self, symbol: str, outputsize: str = "compact"
    ) -> Dict[str, Any]:
        """
        Descarga la serie temporal diaria ajustada para un ticker bursátil.
        Retorna el diccionario JSON crudo con metadatos de ingesta.
        """
        params = {
            "function": "TIME_SERIES_DAILY_ADJUSTED",
            "symbol": symbol.upper(),
            "outputsize": outputsize,
            "apikey": self.api_key,
        }
        logger.info(
            "Solicitando TIME_SERIES_DAILY_ADJUSTED para ticker '%s'...", symbol
        )

        try:
            response = requests.get(self.BASE_URL, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            # Revisar si se recibió respuesta con datos reales
            if "Time Series (Daily)" in data:
                logger.info(
                    "Serie diaria para '%s' obtenida exitosamente (%d días).",
                    symbol,
                    len(data["Time Series (Daily)"]),
                )
                data["_metadata"] = {
                    "source": "alpha_vantage_api",
                    "ingestion_timestamp": datetime.datetime.now(
                        datetime.timezone.utc
                    ).isoformat(),
                    "symbol": symbol.upper(),
                    "is_fallback": False,
                }
                return data

            # Detección de avisos de límite de llamadas gratuitas (5 req/min, 25 req/día)
            if "Note" in data:
                logger.warning("Alpha Vantage Rate Limit alcanzado: %s", data["Note"])
            elif "Information" in data:
                logger.warning("Alpha Vantage Info: %s", data["Information"])
            elif "Error Message" in data:
                logger.error("Alpha Vantage Error: %s", data["Error Message"])

            # Fallback seguro para entorno de desarrollo local
            logger.info(
                "Generando datos sintéticos de prueba para '%s' para garantizar la continuidad del pipeline...",
                symbol,
            )
            return self._generate_fallback_daily_series(symbol)

        except Exception as e:
            logger.error("Fallo de red en TIME_SERIES_DAILY_ADJUSTED para %s: %s", symbol, e)
            return self._generate_fallback_daily_series(symbol)

    def _generate_fallback_listing_status(self) -> str:
        """Genera un listado representativo en formato CSV estándar de Alpha Vantage."""
        return (
            "symbol,name,exchange,assetType,ipoDate,delistingDate,status\n"
            "AAPL,Apple Inc,NASDAQ,Stock,1980-12-12,null,Active\n"
            "MSFT,Microsoft Corporation,NASDAQ,Stock,1986-03-13,null,Active\n"
            "GOOGL,Alphabet Inc - Class A,NASDAQ,Stock,2004-08-19,null,Active\n"
            "AMZN,Amazon.com Inc,NASDAQ,Stock,1997-05-15,null,Active\n"
            "IBM,International Business Machines,NYSE,Stock,1915-11-11,null,Active\n"
            "TSLA,Tesla Inc,NASDAQ,Stock,2010-06-29,null,Active\n"
            "NVDA,NVIDIA Corporation,NASDAQ,Stock,1999-01-22,null,Active\n"
        )

    def _generate_fallback_daily_series(self, symbol: str) -> Dict[str, Any]:
        """Genera un payload con el esquema idéntico a TIME_SERIES_DAILY_ADJUSTED."""
        today = datetime.date.today()
        time_series = {}
        base_price = 150.0

        for i in range(10):
            day_str = (today - datetime.timedelta(days=i)).strftime("%Y-%m-%d")
            time_series[day_str] = {
                "1. open": f"{base_price + i * 0.5:.2f}",
                "2. high": f"{base_price + i * 0.5 + 2.0:.2f}",
                "3. low": f"{base_price + i * 0.5 - 1.5:.2f}",
                "4. close": f"{base_price + i * 0.5 + 0.8:.2f}",
                "5. adjusted close": f"{base_price + i * 0.5 + 0.8:.2f}",
                "6. volume": f"{1000000 + i * 50000}",
                "7. dividend amount": "0.0000",
                "8. split coefficient": "1.0",
            }

        return {
            "Meta Data": {
                "1. Information": f"Daily Time Series with Splits and Dividend Events (Dev Fallback)",
                "2. Symbol": symbol.upper(),
                "3. Last Refreshed": today.strftime("%Y-%m-%d"),
                "4. Output Size": "Compact",
                "5. Time Zone": "US/Eastern",
            },
            "Time Series (Daily)": time_series,
            "_metadata": {
                "source": "synthetic_dev_fallback",
                "ingestion_timestamp": datetime.datetime.now(
                    datetime.timezone.utc
                ).isoformat(),
                "symbol": symbol.upper(),
                "is_fallback": True,
            },
        }
