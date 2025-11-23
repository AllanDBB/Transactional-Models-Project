FROM python:3.11-slim

WORKDIR /app

# Instalar dependencias de sistema para FreeTDS (pymssql las necesita)
RUN apt-get update && apt-get install -y --no-install-recommends \
    freetds-bin \
    freetds-dev \
    && rm -rf /var/lib/apt/lists/*

# Copiar requirements
COPY DWH/etl/requirements.txt .

# Instalar dependencias Python
RUN pip install --no-cache-dir -r requirements.txt

# Copiar scripts BCCR
COPY DWH/bccr/bccr_exchange_rate.py .
COPY DWH/bccr/scheduler.py .

# Copiar .env
COPY .env .

# Crear directorio para logs
RUN mkdir -p /app/logs

# Configurar timezone
ENV TZ=America/Costa_Rica
ENV PYTHONUNBUFFERED=1

# Ejecutar scheduler
CMD ["python3", "scheduler.py"]
