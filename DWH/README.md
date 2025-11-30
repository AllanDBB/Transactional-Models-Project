# Data Warehouse - Transactional Models Project

## Comando

```bash
cd DWH
docker compose up -d
```

## Qué hace

1. Levanta SQL Server 2022 en contenedor sqlserver-dw
   - Crea base de datos MSSQL_DW
   - Crea todas las tablas del DWH (dimensiones, hechos, staging)

2. Levanta contenedor dwh-scheduler que ejecuta:
   - bccr_exchange_rate.py populate: Carga histórico de tipos de cambio CRC->USD desde hace 3 años hasta hoy
   - cargar_mapeo_productos_mysql.py load: Mapea codigo_alt -> sku en tabla staging_map_producto desde productos JSON
   - scheduler.py: Corre 24/7, actualiza tipos de cambio automáticamente cada día a las 5:00 AM
