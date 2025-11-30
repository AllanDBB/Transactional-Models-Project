#!/bin/bash
set -e

echo "================================================================================"
echo "Iniciando DWH Initialization & Scheduler"
echo "================================================================================"

# Esperar a que SQL Server esté listo
echo "Esperando a que SQL Server esté disponible..."
sleep 10

# Ejecutar poblacion historica de BCCR
echo ""
echo "Ejecutando poblacion historica de tipos de cambio..."
python3 bccr_exchange_rate.py populate
if [ $? -eq 0 ]; then
    echo "[OK] Poblacion historica completada"
else
    echo "[ERROR] Fallo en poblacion historica"
fi

# Ejecutar mapeo de productos MySQL
echo ""
echo "Ejecutando mapeo de productos MySQL..."
python3 cargar_mapeo_productos_mysql.py load
if [ $? -eq 0 ]; then
    echo "[OK] Mapeo de productos completado"
else
    echo "[ERROR] Fallo en mapeo de productos"
fi

# Iniciar el scheduler
echo ""
echo "Iniciando scheduler de actualizaciones diarias..."
echo "================================================================================"
exec python3 scheduler.py
