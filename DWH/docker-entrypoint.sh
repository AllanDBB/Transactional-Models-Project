#!/bin/bash
# Entrypoint para SQL Server DWH con inicializacion

# Iniciar SQL Server en background
/opt/mssql/bin/sqlservr &
SQLSERVER_PID=$!

# Esperar a que SQL Server esté listo
echo "Esperando que SQL Server DWH inicie..."
for i in {1..60}; do
    if /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "$SA_PASSWORD" -Q "SELECT 1" &> /dev/null; then
        echo "OK: SQL Server DWH está listo"
        break
    fi
    echo "Intento $i/60..."
    sleep 1
done

# Ejecutar scripts de inicializacion en orden
echo "Ejecutando scripts de inicializacion DWH..."
for script in /docker-entrypoint-initdb.d/*.sql; do
    if [ -f "$script" ]; then
        echo "Ejecutando: $(basename "$script")"
        /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "$SA_PASSWORD" -i "$script"
        if [ $? -eq 0 ]; then
            echo "OK: Completado $(basename "$script")"
        else
            echo "ERROR: Fallo $(basename "$script")"
        fi
    fi
done

echo "Inicializacion DWH completada"

# Mantener SQL Server en foreground
wait $SQLSERVER_PID
