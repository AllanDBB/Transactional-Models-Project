# 1 Levantar contenedor el DWH
    - docker compose down -v
    - docker compose build --no-cache
    - docker compose up -d

**Credenciales del DWH:**
- Host: `localhost:1434`
- Usuario: `sa`
- Contraseña: `BasesDatos2!`
- Base de datos: `MSSQL_DW`

# 2 Leventar bases de datos.
    3.1 - Levantar MYSQL:
        cd MYSQL; docker compose up -d --build
    3.2 - Levantar MSSQL: 
        cd MSSQL; docker-compose up -d --build;
    3.3 - Levantar web SQL:
        cd MSSQL/server; npm i ; npm run dev (iniciar schemas desde ahí)
    3.3 - Levantar SupaBase: 
        cd SUPABASE/website; npm i; npm run dev
        cd SUPABASE/server; npm i; npm run dev
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
    4.5 - docker exec dwh-scheduler python etl_supabase.py;

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
select * from dwh.DimCategory
select * from dwh.DimChannel
select * from dwh.DimCustomer
select * from dwh.DimExchangeRate
select * from dwh.DimOrder 
select * from dwh.DimProduct
select * from dwh.DimTime
select * from dwh.FactSales
select * from dwh.FactTargetSales 
select * from dwh.MetasVentas 
```

## Bases de datos revisadas en el DWH
    - MongoDB: Todo funciona bien (creo)
    - Neo4J: Parece que todo funciona (creo x2)
    - SupaBase: esa vara parece funcionar, no tengo ni la menor idea de porqué, parece que sí !
    - MySQL: funcionó bien
    - MSSQL: solo Dios sabe porqué funcionó

# 6 Pruebas unitarias a websites.
    - Deben revisar cada una individualmente y que funcione. (MSSQL y MySQL parece que funcionan)
    - Neo4J: Neo revisado por Allan, funcionando.
    - MongoDB: Mongo revisado por Allan, funcionando.
    - SupaBase: ni idea, creo que funca no sé

# 7 Apriori - Reglas de Asociación de Productos

## 7.1 ¿Qué es Apriori?
Sistema de **recomendaciones de productos** basado en patrones de compra históricos. Encuentra productos que frecuentemente se compran juntos y calcula la probabilidad de que un cliente compre el producto B si ya compró el producto A.

**Métricas clave:**
- **Support (Soporte):** Frecuencia con la que aparece el conjunto de productos
- **Confidence (Confianza):** Probabilidad de que B se compre cuando se compra A
- **Lift (Elevación):** Qué tan fuerte es la asociación (>1 = correlación positiva)

## 7.2 Ejecutar análisis Apriori

### Manual:
```bash
docker exec dwh-scheduler python apriori_analysis.py run
```

### Automático:
```sql
- Se ejecuta **cada domingo a las 2:00 AM**
- Configurado en `DWH/init_scripts/scheduler.py`
- Analiza todas las transacciones del DWH

### Estadísticas generales:
-- Estadísticas generales
EXEC dbo.sp_get_apriori_stats;

-- Top 20 reglas por Lift
EXEC dbo.sp_get_top_association_rules @TopN=20, @OrderBy='Lift';

-- Recomendaciones para un producto específico
EXEC dbo.sp_get_product_recommendations @ProductId=5689, @TopN=5;

-- Recomendaciones para un carrito de compras
EXEC sp_get_cart_recommendations @ProductIds='5689,5737', @TopN=10;

-- Ver todas las reglas activas
SELECT * FROM dwh.ProductAssociationRules WHERE Activo = 1 ORDER BY Lift DESC;
```

## 7.3 Integración con websites

### MongoDB:
#### 5.1 Mapear productos MongoDB con DWH
```bash
cd MONGODB/server; node update_product_skus.js
```

### Supabase:
#### 5.2 Mapear productos Supabase con DWH
```bash
cd SUPABASE/server; node update_product_skus.js
```

# 8 Power BI
crear metas:
```sql
INSERT INTO dwh.MetasVentas (customerId, productId, Anio, Mes, MetaUSD)
SELECT
    B.customerId,
    B.productId,
    B.Anio,
    B.Mes,
    CAST(
        B.totalUSD * (0.9 + (ABS(CHECKSUM(NEWID())) % 21) / 100.0)
        AS DECIMAL(18,2)
    ) AS MetaUSD
FROM (
    SELECT
        fs.customerId,
        fs.productId,
        dt.[year]  AS Anio,      -- Año desde dimTime
        dt.[month] AS Mes,       -- Mes desde dimTime
        SUM(fs.lineTotalUSD) AS totalUSD
    FROM dwh.FactSales fs
    JOIN dwh.dimTime dt
        ON fs.timeID = dt.id
    GROUP BY
        fs.customerId,
        fs.productId,
        dt.[year],
        dt.[month]
) AS B
WHERE NOT EXISTS (
    SELECT 1
    FROM dwh.MetasVentas mv
    WHERE mv.customerId = B.customerId
      AND mv.productId  = B.productId
      AND mv.Anio       = B.Anio
      AND mv.Mes        = B.Mes
);
```
