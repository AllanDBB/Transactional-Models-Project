#!/bin/bash
# Entrypoint para SQL Server con inicialización de base de datos

# Iniciar SQL Server en background
/opt/mssql/bin/sqlservr &
SQLSERVER_PID=$!

# Esperar a que SQL Server esté listo (máx 60 segundos)
echo "Esperando que SQL Server inicie..."
for i in {1..60}; do
    if /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "$SA_PASSWORD" -Q "SELECT 1" &> /dev/null; then
        echo "SQL Server está listo"
        break
    fi
    echo "Intento $i/60..."
    sleep 1
done

# Ejecutar scripts de inicialización
echo "Ejecutando scripts de inicialización..."
for script in /docker-entrypoint-initdb.d/*.sql; do
    if [ -f "$script" ]; then
        echo "Ejecutando: $script"
        /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "$SA_PASSWORD" -i "$script"
        echo "✓ Completado: $script"
    fi
done

# Mantener SQL Server en foreground
wait $SQLSERVER_PID

