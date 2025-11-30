#!/bin/bash
# Entrypoint para SQL Server con inicializacion de base de datos

# Iniciar SQL Server en background
/opt/mssql/bin/sqlservr &
SQLSERVER_PID=$!

# Esperar a que SQL Server este listo (max 60 segundos)
echo "Esperando que SQL Server inicie..."
for i in {1..60}; do
    if sqlcmd -S localhost -U sa -P "$SA_PASSWORD" -C -Q "SELECT 1" >/dev/null 2>&1; then
        echo "SQL Server esta listo"
        break
    fi
    echo "Intento $i/60..."
    sleep 1
done

# Ejecutar scripts de inicializacion
echo "Ejecutando scripts de inicializacion..."
for script in /docker-entrypoint-initdb.d/*.sql; do
    if [ -f "$script" ]; then
        echo "Ejecutando: $script"
        sqlcmd -S localhost -U sa -P "$SA_PASSWORD" -C -i "$script"
        echo "Completado: $script"
    fi
done

# Mantener SQL Server en foreground
wait $SQLSERVER_PID
