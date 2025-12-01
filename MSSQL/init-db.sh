#!/bin/bash

# Esperar a que SQL Server esté listo
echo "Esperando a que SQL Server inicie..."
sleep 30s

# Ejecutar scripts SQL en orden
echo "Ejecutando scripts de inicialización..."

for script in /usr/src/app/*.sql; do
    if [ -f "$script" ]; then
        echo "Ejecutando: $(basename $script)"
        /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$SA_PASSWORD" -d master -C -i "$script"
        
        if [ $? -eq 0 ]; then
            echo "✓ $(basename $script) completado"
        else
            echo "✗ Error ejecutando $(basename $script)"
        fi
    fi
done

echo "Inicialización completada"
