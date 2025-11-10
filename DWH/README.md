# Data Warehouse (ClickHouse) - Transactional Models Project

## 🚀 Inicio Rápido

```bash
cd DWH
docker-compose up -d
```

## 📦 Servicios

- **ClickHouse HTTP**: http://localhost:8123
- **ClickHouse Native**: localhost:9000
- **Credenciales**: default / clickhouse123

## 🔄 ETL Process

El ETL del DWH extrae datos de todas las fuentes (MongoDB, MySQL, MSSQL, Neo4j, PostgreSQL) y los carga en el Data Warehouse.

```bash
pip install -r etl/requirements.txt
python etl/run_etl.py
```

## 📁 Estructura

```
DWH/
├── docker-compose.yml
├── Dockerfile
├── init/              # Scripts de inicialización
├── config/            # Configuraciones ClickHouse
├── data/              # Datos de importación
└── etl/               # Proceso ETL
    ├── extract/       # Extrae de todas las fuentes
    ├── transform/     # Transforma para el DWH
    └── load/          # Carga al DWH
```

## 📊 Queries Útiles

```sql
-- Ver ventas totales por día
SELECT date_key, sum(total_amount) as total
FROM fact_sales
GROUP BY date_key
ORDER BY date_key;

-- Ver productos más vendidos
SELECT p.product_name, sum(f.quantity) as units_sold
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY p.product_name
ORDER BY units_sold DESC
LIMIT 10;

-- Ventas mensuales agregadas
SELECT * FROM mv_sales_monthly
ORDER BY year_month DESC;
```
