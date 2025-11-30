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
COPY DWH/init_scripts/scheduler.py .
COPY DWH/init_scripts/cargar_mapeo_productos_mysql.py .
COPY DWH/init_scripts/entrypoint.sh .

# Copiar .env
COPY DWH/.env .

# Crear directorio para logs
RUN mkdir -p /app/logs

# Hacer ejecutable el entrypoint
RUN chmod +x /app/entrypoint.sh

# Configurar timezone
ENV TZ=America/Costa_Rica
ENV PYTHONUNBUFFERED=1

# Ejecutar entrypoint que hace todo el setup
CMD ["/app/entrypoint.sh"]
