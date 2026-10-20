"""
Utilidad de almacenamiento para MinIO (S3 Object Storage).
Permite la gestión de buckets y subida de archivos crudos en la Capa Bronze.
"""

import io
import json
import logging
import os
from typing import Any, Dict, List, Optional
from minio import Minio
from minio.error import S3Error

logger = logging.getLogger(__name__)


class MinIOStorage:
    """
    Cliente de alto nivel para interactuar con MinIO compatible con Amazon S3.
    """

    def __init__(
        self,
        endpoint: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        secure: bool = False,
    ):
        raw_endpoint = endpoint or os.getenv("S3_ENDPOINT", "minio:9000")
        
        # Limpiar protocolos si vienen en la variable de entorno
        if raw_endpoint.startswith("http://"):
            self.endpoint = raw_endpoint[7:]
            self.secure = False
        elif raw_endpoint.startswith("https://"):
            self.endpoint = raw_endpoint[8:]
            self.secure = True
        else:
            self.endpoint = raw_endpoint
            self.secure = secure

        self.access_key = access_key or os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
        self.secret_key = secret_key or os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

        self.client = Minio(
            endpoint=self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=self.secure,
        )

    def ping(self) -> bool:
        """Verifica la conectividad con el servicio MinIO listando buckets."""
        try:
            self.client.list_buckets()
            return True
        except Exception as e:
            logger.error("Error al conectar con MinIO en %s: %s", self.endpoint, e)
            raise

    def ensure_bucket(self, bucket_name: str) -> None:
        """Crea el bucket si no existe (idempotencia)."""
        if not self.client.bucket_exists(bucket_name):
            logger.info("Creando bucket '%s' en MinIO...", bucket_name)
            self.client.make_bucket(bucket_name)
        else:
            logger.debug("Bucket '%s' verificado correctamente.", bucket_name)

    def upload_json(
        self,
        bucket_name: str,
        object_name: str,
        data: Dict[str, Any],
        metadata: Optional[Dict[str, str]] = None,
    ) -> str:
        """
        Sube un diccionario Python serializado como JSON a MinIO.
        """
        self.ensure_bucket(bucket_name)
        payload = json.dumps(data, indent=2, ensure_ascii=False).encode("utf-8")
        data_stream = io.BytesIO(payload)

        self.client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=data_stream,
            length=len(payload),
            content_type="application/json",
            metadata=metadata,
        )
        s3_uri = f"s3://{bucket_name}/{object_name}"
        logger.info("Archivo JSON subido a %s (%d bytes)", s3_uri, len(payload))
        return s3_uri

    def upload_csv(
        self,
        bucket_name: str,
        object_name: str,
        csv_content: str,
        metadata: Optional[Dict[str, str]] = None,
    ) -> str:
        """
        Sube una cadena de texto en formato CSV a MinIO.
        """
        self.ensure_bucket(bucket_name)
        payload = csv_content.encode("utf-8")
        data_stream = io.BytesIO(payload)

        self.client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=data_stream,
            length=len(payload),
            content_type="text/csv",
            metadata=metadata,
        )
        s3_uri = f"s3://{bucket_name}/{object_name}"
        logger.info("Archivo CSV subido a %s (%d bytes)", s3_uri, len(payload))
        return s3_uri

    def object_exists(self, bucket_name: str, object_name: str) -> bool:
        """Verifica si un objeto existe en un bucket determinado."""
        try:
            self.client.stat_object(bucket_name, object_name)
            return True
        except S3Error as e:
            if e.code == "NoSuchKey":
                return False
            raise

    def list_objects(self, bucket_name: str, prefix: str = "") -> List[str]:
        """Lista nombres de objetos bajo un prefijo."""
        objects = self.client.list_objects(bucket_name, prefix=prefix, recursive=True)
        return [obj.object_name for obj in objects]
