FROM mcr.microsoft.com/mssql/server:2022-latest

USER root

# Instalar mssql-tools para sqlcmd
RUN apt-get update \
    && apt-get install -y curl apt-transport-https gnupg \
    && mkdir -p /etc/apt/keyrings \
    && curl https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /etc/apt/keyrings/microsoft.gpg \
    && echo "deb [arch=amd64 signed-by=/etc/apt/keyrings/microsoft.gpg] https://packages.microsoft.com/ubuntu/22.04/prod jammy main" > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y mssql-tools18 unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

ENV PATH="/opt/mssql-tools18/bin:${PATH}" \
    TZ=America/Costa_Rica

# Copiar scripts SQL
COPY MSSQL/init/*.sql /docker-entrypoint-initdb.d/

EXPOSE 1433

# Iniciar SQL Server directamente (sin bash scripts)
CMD ["/opt/mssql/bin/sqlservr"]
