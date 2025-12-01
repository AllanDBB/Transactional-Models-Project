FROM python:3.11-slim

WORKDIR /app

# Instalar dependencias de sistema para FreeTDS (pymssql las necesita)
RUN apt-get update && apt-get install -y --no-install-recommends \
    freetds-bin \
    freetds-dev \
    && rm -rf /var/lib/apt/lists/*

# Copiar requirements
COPY DWH/init_scripts/requirements.txt .

# Instalar dependencias Python
RUN pip install --no-cache-dir -r requirements.txt

# Copiar carpeta shared con datos JSON
COPY shared/ /app/shared/

# Copiar scripts de inicializacion
COPY DWH/init_scripts/bccr_exchange_rate.py .
COPY DWH/init_scripts/cargar_mapeo_productos_mysql.py .
COPY DWH/init_scripts/db_utils.py .
COPY DWH/init_scripts/etl_mongo.py .
COPY DWH/init_scripts/etl_mssql_src.py .
COPY DWH/init_scripts/etl_mysql.py .
COPY DWH/init_scripts/etl_neo4j.py .
COPY DWH/init_scripts/etl_supabase.py .
COPY DWH/init_scripts/scheduler.py .

# Copiar .env
COPY DWH/.env .

# Crear directorio para logs
RUN mkdir -p /app/logs

# Configurar timezone
ENV TZ=America/Costa_Rica
ENV PYTHONUNBUFFERED=1

# Ejecutar inicializacion y luego el scheduler
CMD ["bash", "-c", "\
  set -e; \
  echo '==============================================================================='; \
  echo 'Iniciando DWH Initialization & Scheduler'; \
  echo '==============================================================================='; \
  echo 'Esperando a que SQL Server este disponible...'; \
  sleep 10; \
  echo; \
  echo 'Ejecutando poblacion historica de tipos de cambio...'; \
  python3 bccr_exchange_rate.py populate && echo '[OK] Poblacion historica completada'; \
  echo 'Promoviendo staging a DWH...'; \
  python3 -c \"from db_utils import execute_sp;\\ntry:\\n    execute_sp('sp_etl_run_all')\\n    print('sp_etl_run_all ejecutado')\\nexcept Exception as e:\\n    print('WARN: sp_etl_run_all fallo', e)\"; \
  echo; \
  echo 'Iniciando scheduler de actualizaciones diarias...'; \
  echo '==============================================================================='; \
  exec python3 scheduler.py \
"]
