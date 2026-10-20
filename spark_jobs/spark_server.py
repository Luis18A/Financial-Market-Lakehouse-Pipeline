"""
Servidor HTTP ligero para orquestación de PySpark Jobs desde Apache Airflow.
Permite ejecutar 'spark-submit' mediante peticiones HTTP REST de forma segura.
"""

from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import logging
import subprocess
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] SparkJobServer: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("SparkJobServer")


class SparkJobHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status": "UP", "service": "SparkJobServer"}')
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        if self.path == "/run-silver":
            logger.info("Recibida orden de ejecución: Pipeline Capa Silver (Bronze to Silver)")
            
            try:
                # Ejecutar spark-submit en el cluster local
                result = subprocess.run(
                    [
                        "spark-submit",
                        "--master",
                        "spark://spark-master:7077",
                        "/opt/bitnami/spark/jobs/bronze_to_silver.py",
                    ],
                    capture_output=True,
                    text=True,
                    check=False,
                )

                if result.returncode == 0:
                    logger.info("✅ PySpark job ejecutado exitosamente.")
                    response_payload = {
                        "status": "SUCCESS",
                        "exit_code": result.returncode,
                        "stdout_tail": result.stdout[-500:],
                    }
                    self.send_response(200)
                else:
                    logger.error("❌ PySpark job falló con código: %d", result.returncode)
                    response_payload = {
                        "status": "FAILED",
                        "exit_code": result.returncode,
                        "stderr_tail": result.stderr[-500:],
                    }
                    self.send_response(500)

            except Exception as e:
                logger.error("Error al despachar spark-submit: %s", e)
                response_payload = {"status": "ERROR", "message": str(e)}
                self.send_response(500)

            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(response_payload).encode("utf-8"))
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        # Redirigir logs estándar a logger formateado
        logger.info("%s - %s", self.address_string(), format % args)


def run_server(port: int = 5050):
    server_address = ("0.0.0.0", port)
    httpd = HTTPServer(server_address, SparkJobHandler)
    logger.info("Servidor de despacho Spark escuchando en http://0.0.0.0:%d", port)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
        logger.info("Servidor cerrado.")


if __name__ == "__main__":
    run_server()
