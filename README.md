# 1 Levantar contenedor el DWH
    - docker compose down -v
    - docker compose build --no-cache
    - docker compose up -d

# 2 Leventar bases de datos.
    3.1 - Levantar MYSQL:
        cd MYSQL; docker compose up -d --build
    3.2 - Levantar MSSQL: 
        cd MSSQL; docker compose up -d -- build;
    3.3 - Levantar web SQL:
        cd MSSQL/server; npm i ; npm run dev (iniciar schemas desde ahí)
    3.3 - Levantar SupaBase: 
        cd SUPABASE/website; npm i; npm run dev
    3.4 - Levantar MongoDB: 
        cd MONGODB; npm i, npm run dev
    3.5 - Levantar Neo4J:
        cd NEO4J; npm  i, npm run dev (Revisar que la web no este caido el contenedor)

# 3 Carga de datos.
    3.1 - Para cargar datos de SQL ir al website y clic en cargar datos
    3.2 - Para cargar datos de Mongo, Supa, Neo4; shared/load_ ejecutarlos

# 4 Extract Layer. Carga de datos hacia el staging 
    4.1 - docker exec dwh-scheduler python etl_mssql_src.py; 
    4.2 - docker exec dwh-scheduler python etl_mysql.py; 
    4.3 - docker exec dwh-scheduler python etl_mongo.py; 
    4.4 - docker exec dwh-scheduler python etl_neo4j.py; 
    4.5 - docker exec dwh-scheduler python etl_supabase.py

## Para probar utilizar:
``` sql
select * from staging.mongo_orders
select * from staging.mongo_customers
select * from staging.mongo_order_items

select * from staging.mssql_customers
select * from staging.mssql_products
select * from staging.mssql_sales

select * from staging.mysql_sales
select * from staging.mysql_customers
select * from staging.mysql_products

select * from staging.neo4j_edges
select * from staging.neo4j_nodes
select * from staging.neo4j_order_items

select * from staging.supabase_order_items
select * from staging.supabase_orders
select * from staging.supabase_users
select * from staging.supabase_products
```

# 5 Transform Layer - Consolidación y Limpieza de Datos

## Ejecutar transformación completa (staging → dwh):
```bash
docker exec dwh-scheduler python transform_staging_to_dwh.py
```
## Para probar
```sql
select * from dwh.DimCategory -- No sale nada
select * from dwh.DimChannel
select * from dwh.DimCustomer
select * from dwh.DimExchangeRate
select * from dwh.DimOrder -- No sale nada
select * from dwh.DimProduct -- Al parecer hay una cosa raro con los de mongo
select * from dwh.DimTime
select * from dwh.FactSales
select * from dwh.FactTargetSales -- No sale nada
select * from dwh.MetasVentas -- No tiene nada / no debería tener nada
```

# 6 Carga de datos a DWH. ¿debería haber un MDM? (Load layer)

# 7 Pruebas unitarias a websites.

# 8 Apriori - Reglas de Asociación de Productos

# 9 Powerbgay