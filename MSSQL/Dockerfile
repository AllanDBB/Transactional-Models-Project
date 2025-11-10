FROM mcr.microsoft.com/mssql/server:2022-latest

# Crear usuario no-root
USER root

# Instalar herramientas
RUN apt-get update && apt-get install -y \
    curl \
    vim \
    && rm -rf /var/lib/apt/lists/*

# Crear directorio para scripts
RUN mkdir -p /docker-entrypoint-initdb.d

# Copiar scripts de inicialización
COPY init/*.sql /docker-entrypoint-initdb.d/

# Configurar timezone
ENV TZ=America/Costa_Rica

# Exponer puerto
EXPOSE 1433

# Volver a usuario mssql
USER mssql
