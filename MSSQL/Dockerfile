FROM mcr.microsoft.com/mssql/server:2022-latest

USER root

# Instalar herramientas base y sqlcmd (mssql-tools18)
RUN apt-get update \
    && apt-get install -y curl apt-transport-https gnupg \
    && mkdir -p /etc/apt/keyrings \
    && curl https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /etc/apt/keyrings/microsoft.gpg \
    && echo "deb [arch=amd64 signed-by=/etc/apt/keyrings/microsoft.gpg] https://packages.microsoft.com/ubuntu/22.04/prod jammy main" > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y mssql-tools18 unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

ENV PATH="/opt/mssql-tools18/bin:${PATH}"
ENV TZ=America/Costa_Rica

# Crear directorio para scripts
RUN mkdir -p /docker-entrypoint-initdb.d

# Copiar scripts de inicializacion
COPY MSSQL/init/*.sql /docker-entrypoint-initdb.d/

# Script de inicialización inline usando CMD
COPY --chmod=755 <<'EOF' /usr/local/bin/init-db.sh
#!/bin/bash
set -e
echo "Iniciando SQL Server..."
/opt/mssql/bin/sqlservr &
SQLSERVER_PID=$!

echo "Esperando SQL Server..."
for i in {1..60}; do
    if sqlcmd -S localhost -U sa -P "$SA_PASSWORD" -C -Q "SELECT 1" >/dev/null 2>&1; then
        echo "✓ SQL Server listo"
        break
    fi
    sleep 1
done

echo "Ejecutando scripts de inicialización..."
for script in /docker-entrypoint-initdb.d/*.sql; do
    if [ -f "$script" ]; then
        echo "→ $(basename $script)"
        sqlcmd -S localhost -U sa -P "$SA_PASSWORD" -C -i "$script"
    fi
done
echo "✓ Inicialización completa"

wait $SQLSERVER_PID
EOF

EXPOSE 1433

ENTRYPOINT ["/usr/local/bin/init-db.sh"]
