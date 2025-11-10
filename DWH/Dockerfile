FROM clickhouse/clickhouse-server:latest

# Crear directorios
RUN mkdir -p /docker-entrypoint-initdb.d
RUN mkdir -p /etc/clickhouse-server/config.d

# Copiar scripts de inicialización
COPY init/*.sql /docker-entrypoint-initdb.d/

# Exponer puertos
EXPOSE 8123 9000

# El comando por defecto ya está definido en la imagen base
