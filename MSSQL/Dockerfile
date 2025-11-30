FROM mcr.microsoft.com/mssql/server:2022-latest

USER root

# Instalar herramientas base y sqlcmd (mssql-tools18)
RUN apt-get update \
    && apt-get install -y curl apt-transport-https gnupg vim \
    && mkdir -p /etc/apt/keyrings \
    && curl https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /etc/apt/keyrings/microsoft.gpg \
    && echo "deb [arch=amd64 signed-by=/etc/apt/keyrings/microsoft.gpg] https://packages.microsoft.com/ubuntu/22.04/prod jammy main" > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y mssql-tools18 unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

ENV PATH="/opt/mssql-tools18/bin:${PATH}"

# Crear directorio para scripts
RUN mkdir -p /docker-entrypoint-initdb.d

# Copiar scripts de inicializacion
COPY MSSQL/init/*.sql /docker-entrypoint-initdb.d/

# Copiar script de inicio que ejecutara los .sql
COPY MSSQL/docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Configurar timezone
ENV TZ=America/Costa_Rica

# Exponer puerto
EXPOSE 1433

# Usar el script de entrada personalizado
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
