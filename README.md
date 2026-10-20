# 🚀 Financial-Market-Lakehouse-Pipeline
### Arquitectura Medallón (Bronze - Silver - Gold) para Análisis Financiero y Bursátil

Bienvenido a la documentación técnica de **Financial-Market-Lakehouse-Pipeline**. Este proyecto implementa una arquitectura moderna de **Data Lakehouse** de nivel empresarial, diseñada para la ingesta masiva, procesamiento distribuido, modelado analítico y control de calidad de datos del mercado financiero.

---

## 1. Visión de Arquitectura: Patrón Medallón

El diseño desacopla el **almacenamiento** de la **computación**, permitiendo escalabilidad horizontal, reprocesamiento histórico y gobernanza de datos:

```
                  ┌──────────────────────────────────────────────┐
                  │          ALPHA VANTAGE (API Externa)         │
                  └──────────────────────┬───────────────────────┘
                                         │
                                         ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                                   DATA LAKEHOUSE                                       │
│                                                                                        │
│  ┌────────────────────────┐  ┌────────────────────────┐  ┌──────────────────────────┐  │
│  │      CAPA BRONZE       │  │      CAPA SILVER       │  │        CAPA GOLD         │  │
│  │     (Raw / Ingestión)  │  │   (Cleaned / Curated)  │  │   (Curated Analytics/DW) │  │
│  │                        │  │                        │  │                          │  │
│  │ • MinIO (S3)           │─▶│ • MinIO (S3)           │─▶│ • PostgreSQL (DW)        │  │
│  │ • JSON / Parquet crudo │  │ • Parquet Optimizado   │  │ • dbt Core               │  │
│  │ • Partición por fecha  │  │ • PySpark Jobs         │  │ • Tablas Hechos / Dims   │  │
│  │ • Inmutable            │  │ • Limpieza y métricas  │  │ • Data Quality Tests     │  │
│  └────────────────────────┘  └────────────────────────┘  └──────────────────────────┘  │
└────────────────────────────────────────────────────────────────────────────────────────┘
                                         ▲
                                         │
                  ┌──────────────────────┴───────────────────────┐
                  │         APACHE AIRFLOW (Orquestación)         │
                  │   DAG: Ingesta -> Spark -> dbt -> Tests      │
                  └──────────────────────────────────────────────┘
```

### Componentes Tecnológicos
1. **Object Storage (Data Lake - MinIO)**: Compatible con la API de Amazon S3. Almacena las capas Bronze (datos crudos e inmutables) y Silver (tablas consolidadas en formato Parquet particionado).
2. **Motor de Computación Distribuida (Apache Spark 3.5)**: Ejecuta transformaciones masivas en memoria (PySpark) para limpiar, tipar y enriquecer los datos de la capa Silver.
3. **Data Warehouse & Modelado Analítico (PostgreSQL + dbt Core)**: dbt estructura la capa Gold mediante modelos dimensionales (Kimball / Star Schema), ejecutando tests automatizados de unicidad, no-nulidad y reglas de negocio.
4. **Orquestación End-to-End (Apache Airflow 2.8)**: Coordina el pipeline completo mediante grafos acíclicos dirigidos (DAGs) con tolerancia a fallos y reintentos.

---

## 2. Estructura del Repositorio

El proyecto sigue las mejores prácticas de ingeniería de software y datos:

```text
Financial-Market-Lakehouse-Pipeline/
│
├── .env.example              # Plantilla documentada de variables de entorno y credenciales
├── .env                      # Variables locales activas (ignorado por Git por seguridad)
├── .gitignore                # Reglas de exclusión (secretos, temporales, logs, venv, caches)
├── docker-compose.yml        # Orquestación de infraestructura multi-contenedor
├── README.md                 # Documentación técnica completa
│
├── config/                   # Configuraciones compartidas entre servicios
│   └── spark-defaults.conf   # Conexión S3A entre Apache Spark y MinIO
│
├── dags/                     # Grafos de orquestación de Apache Airflow
│   ├── financial_bronze_ingestion_dag.py # DAG de ingesta cruda a MinIO
│   └── utils/                # Módulos reutilizables de ingesta y almacenamiento
│       ├── __init__.py
│       ├── alpha_vantage_client.py # Cliente HTTP con rate-limiting y fallbacks
│       └── minio_storage.py        # Wrapper S3 para uploads e idempotencia
│
├── docker/                   # Definiciones personalizadas de contenedores
│   └── minio/
│       └── Dockerfile        # Compilación multi-stage de MinIO desde código fuente oficial
│
├── spark_jobs/               # Scripts PySpark de procesamiento masivo (Capa Silver)
│   └── .gitkeep
│
├── dbt_financial/            # Proyecto dbt Core (Capa Gold y Data Quality)
│   ├── dbt_project.yml       # Configuración global del proyecto dbt
│   ├── profiles.yml.example  # Perfil de conexión a PostgreSQL
│   ├── models/
│   │   ├── staging/          # Modelos de preparación y tipado inicial
│   │   └── marts/            # Modelos de hechos (facts) y dimensiones (dims)
│   └── tests/                # Pruebas de calidad de datos personalizadas
│
├── sql/                      # Scripts DDL y de inicialización
│   └── init-db.sql           # Inicialización automática de bases de datos PostgreSQL
│
└── logs/                     # Directorio de trazas de ejecución de Airflow y Spark
    └── .gitkeep
```

---

## 3. Infraestructura Docker Compose

El archivo [`docker-compose.yml`](file:///c:/Users/luisC/Music/DataEngineer/Financial-Market-Lakehouse-Pipeline/docker-compose.yml) orquesta todos los servicios dentro de una red privada virtual (`lakehouse_net`).

### Servicios Desplegados:
| Servicio | Contenedor | Puerto Local | Descripción |
| :--- | :--- | :--- | :--- |
| **MinIO** | `lakehouse_minio` | `9000` (API), `9001` (Web) | Almacenamiento S3 local (compilado o precargado como `docker-minio:latest`) |
| **PostgreSQL** | `postgres_lakehouse` | `5432` | Base para Airflow (`airflow`) y Data Warehouse analítico (`financial_dw`) |
| **Spark Master** | `lakehouse_spark_master` | `7077` (Cluster), `8081` (Web) | Nodo maestro de procesamiento distribuido (`bitnamilegacy/spark:3.5.0`) |
| **Spark Worker** | `lakehouse_spark_worker` | `8082` (Web) | Nodo trabajador asignado con núcleos y memoria para PySpark |
| **Airflow Standalone**| `lakehouse_airflow` | `8085` (Web) | Scheduler, Webserver y LocalExecutor con inicialización de buckets en MinIO |

---

## 4. Guía de Puesta en Marcha (Paso a Paso)

### Prerrequisitos
- **Docker Desktop** (o Docker Engine en Linux) con soporte para WSL2 en Windows habilitado. Asegúrate de que el demonio de Docker esté corriendo.
- **Git** instalado.

### Paso 1: Clonar y Ubicarse en el Repositorio
```bash
git clone <URL_DE_TU_REPOSITORIO>
cd Financial-Market-Lakehouse-Pipeline
```

### Paso 2: Configurar Variables de Entorno
Crea tu archivo `.env` a partir de la plantilla:
```bash
# En Windows PowerShell:
Copy-Item .env.example .env

# En Linux / macOS / Git Bash:
cp .env.example .env
```
> [!NOTE]
> Puedes abrir `.env` y colocar tu clave gratuita de la API de Alpha Vantage en `ALPHA_VANTAGE_API_KEY`. Por defecto viene con credenciales estándar seguras para desarrollo local.

### Paso 3: Levantar la Infraestructura con Docker Compose
Ejecuta el siguiente comando en la raíz del proyecto:
```bash
docker compose up -d
```
> [!TIP]
> En la primera ejecución, Docker descargará las imágenes de MinIO, PostgreSQL, Bitnami Spark y Apache Airflow. Dependiendo de tu conexión, esto tomará unos minutos.

### Paso 4: Verificar el Estado de los Contenedores
```bash
docker compose ps
```
Todos los servicios principales (`lakehouse_minio`, `postgres_lakehouse`, `lakehouse_spark_master`, `lakehouse_spark_worker`, `lakehouse_airflow`) deben mostrar estado `Up` o `Up (healthy)`.

---

## 5. Accesos a las Interfaces Web (UIs)

Una vez levantado el entorno, puedes acceder a las consolas web desde tu navegador:

| Servicio | URL Local | Usuario por Defecto | Contraseña por Defecto |
| :--- | :--- | :--- | :--- |
| **Apache Airflow** | [http://localhost:8085](http://localhost:8085) | `admin` | `admin` |
| **MinIO Console** | [http://localhost:9001](http://localhost:9001) | `minioadmin` | `minioadmin` |
| **Spark Master UI**| [http://localhost:8081](http://localhost:8081) | *No requiere* | *No requiere* |
| **Spark Worker UI**| [http://localhost:8082](http://localhost:8082) | *No requiere* | *No requiere* |
| **PostgreSQL (DW)**| `localhost:5432` | `postgres` | `postgrespassword` |

---

## 6. Verificaciones de Infraestructura

### 1. Comprobar Buckets en MinIO
1. Ingresa a [http://localhost:9001](http://localhost:9001) con `minioadmin` / `minioadmin`.
2. En la sección **Buckets**, verifica que existan:
   - `bronze`: Destino de datos crudos de Alpha Vantage.
   - `silver`: Destino de datos limpios transformados en Parquet.
   - `gold`: Destino de exportaciones o modelos consolidados.

### 2. Comprobar Bases de Datos en PostgreSQL
Puedes conectarte desde cualquier cliente (DBeaver, DataGrip, VS Code Database Client o CLI):
- Host: `localhost`
- Puerto: `5432`
- Usuario: `postgres`
- Contraseña: `postgrespassword`
- Bases de datos creadas:
  - `airflow`: Base de datos de metadatos del scheduler y ejecuciones.
  - `financial_dw`: Data Warehouse limpio donde dbt creará los esquemas `staging` y `gold`.

### 3. Comprobar Cluster Spark
Ingresa a [http://localhost:8081](http://localhost:8081). Deberías ver:
- Spark Master URL: `spark://spark-master:7077`.
- Al menos 1 Worker activo registrado en la tabla "Alive Workers".

---

## 7. Capa Bronze: Ingesta de Mercado Financiero con Airflow

La **Capa Bronze** implementa el patrón de ingesta cruda e inmutable. El DAG [`financial_bronze_ingestion`](file:///c:/Users/luisC/Music/DataEngineer/Financial-Market-Lakehouse-Pipeline/dags/financial_bronze_ingestion_dag.py) orquesta la extracción desde Alpha Vantage y la persistencia en el bucket `s3://bronze/`.

```
                    ┌───────────────────────────┐
                    │  check_minio_connectivity │
                    └─────────────┬─────────────┘
                                  │
                  ┌───────────────┴───────────────┐
                  ▼                               ▼
      ┌───────────────────────┐       ┌────────────────────────┐
      │ ingest_listing_status │       │ ingest_stock_time_series│
      └───────────┬───────────┘       └───────────┬────────────┘
                  │                               │
                  └───────────────┬───────────────┘
                                  ▼
                    ┌───────────────────────────┐
                    │   verify_bronze_ingestion │
                    └───────────────────────────┘
```

### ¿Qué es un DAG en Apache Airflow?
Un **DAG** (*Directed Acyclic Graph* o Grafo Acíclico Dirigido) es la definición en código Python de un flujo de trabajo orquestado:
* **Grafo**: Representa un conjunto de tareas (nodos) interconectadas mediante dependencias (aristas).
* **Dirigido**: La ejecución tiene una única dirección lógica: la tarea `verify_bronze_ingestion` solo se ejecuta si las tareas predecesoras (`ingest_listing_status` e `ingest_stock_time_series`) concluyen exitosamente.
* **Acíclico**: No admite bucles o ciclos infinitos (la tarea C no puede enviar su salida a la tarea A). Esto garantiza que el pipeline tenga un inicio determinista y un fin predecible.

### Contrato de Almacenamiento (Particionamiento Hive)
Para garantizar lecturas de alta eficiencia en la capa Silver sin escanear datos redundantes, los datos se organizan siguiendo la convención de particionamiento Hive:

- **Catálogo de Acciones y ETFs (`LISTING_STATUS`)**:
  ```text
  s3://bronze/alpha_vantage/listing_status/ingestion_date=YYYY-MM-DD/listing_status.csv
  ```
- **Series Diarias Ajustadas (`TIME_SERIES_DAILY_ADJUSTED`)**:
  ```text
  s3://bronze/alpha_vantage/time_series_daily/symbol=AAPL/ingestion_date=YYYY-MM-DD/daily_adjusted.json
  s3://bronze/alpha_vantage/time_series_daily/symbol=MSFT/ingestion_date=YYYY-MM-DD/daily_adjusted.json
  ```

### ¿Por qué esta estructura ahorra hasta un 95% de I/O y memoria en Apache Spark?
* **Sin Particionamiento (Full Scan tradicional)**: Si todas las transacciones históricas de miles de empresas estuvieran en un único archivo o carpeta plana, cuando Spark necesite analizar únicamente `AAPL`, tendría que descargar por red gigabytes de datos innecesarios, deserializar todos los registros y filtrarlos en memoria RAM, desperdiciando recursos.
* **Con Hive Partitioning (`Partition Pruning`)**: Al almacenar bajo subdirectorios `symbol=AAPL/ingestion_date=.../`, el optimizador de Spark (**Catalyst Optimizer**) evalúa los filtros `WHERE symbol = 'AAPL'` antes de leer el almacenamiento. Spark "poda" (descarta) todas las demás carpetas de otros símbolos y realiza peticiones HTTP/S3 **únicamente** sobre la carpeta de Apple. Esto reduce drásticamente la latencia de red, evita errores de *Out-Of-Memory* (OOM) y acelera las consultas analíticas.

### Principios de Ingeniería Evaluados por Reclutadores Técnicos:
1. **Idempotencia Absoluta**: Si el DAG se reejecuta para una fecha específica (`ds`), sobrescribe exactamente la partición correspondiente sin duplicar registros ni alterar el historial.
2. **Respeto de Cuotas y Rate-Limiting**: El cliente HTTP implementa pausas calculadas de 12 segundos entre llamadas y detección de mensajes de advertencia de la API gratuita (5 req/min), asegurando que el pipeline no sea bloqueado.
3. **Data Quality Gate Integrado**: La tarea `verify_bronze_ingestion` no asume que todo funcionó; realiza una auditoría física en el Object Storage MinIO verificando que los archivos existan con contenido antes de marcar la corrida en verde.
4. **Desacople Almacenamiento-Cómputo**: Los datos crudos viven en MinIO (S3), mientras que el cómputo reside en Spark y la base de datos relacional (PostgreSQL) se reserva exclusivamente para analítica estructurada (Gold).

### Evidencia de Ejecución Verificada:
* **Buckets creados en MinIO**: `bronze`, `silver`, `gold`.
* **Archivos ingestados en la primera corrida**:
  - `alpha_vantage/listing_status/ingestion_date=2026-10-19/listing_status.csv`
  - `alpha_vantage/time_series_daily/symbol=AAPL/ingestion_date=2026-10-19/daily_adjusted.json`
  - `alpha_vantage/time_series_daily/symbol=AMZN/ingestion_date=2026-10-19/daily_adjusted.json`
  - `alpha_vantage/time_series_daily/symbol=GOOGL/ingestion_date=2026-10-19/daily_adjusted.json`
  - `alpha_vantage/time_series_daily/symbol=IBM/ingestion_date=2026-10-19/daily_adjusted.json`
  - `alpha_vantage/time_series_daily/symbol=MSFT/ingestion_date=2026-10-19/daily_adjusted.json`

---

## 8. Comandos Útiles de Mantenimiento

```bash
# Ver los logs en tiempo real de todos los servicios
docker compose logs -f

# Ver los logs únicamente de Airflow
docker compose logs -f airflow-webserver

# Detener todos los servicios sin borrar datos almacenados
docker compose stop

# Reanudar servicios detenidos
docker compose start

# Destruir contenedores preservando volúmenes de datos
docker compose down

# Destruir contenedores Y volúmenes (reseteo completo desde cero)
docker compose down -v
```

---

## 9. Hoja de Ruta para las Siguientes Fases

Con la Capa Bronze implementada, el avance del Lakehouse continúa con:

1. **Fase 3 - Capa Silver (Data Processing con PySpark)**:
   - Scripts de PySpark ejecutados en el cluster Spark.
   - Limpieza, cálculo de variaciones porcentuales, casting de tipos y persistencia en `s3://silver/` en formato Parquet optimizado.
2. **Fase 4 - Capa Gold (Data Modeling con dbt Core)**:
   - Ingesta a `financial_dw` y modelado dimensional (tablas de hechos e indicadores financieros).
   - Pruebas de calidad y linaje con `dbt test`.
3. **Fase 5 - Orquestación Final**:
   - Ensamblado del DAG integral en Airflow con alertas y monitoreo.
