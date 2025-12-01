# 1 Levantar contenedor el DWH
    1.1 - Crear schema

# 2 Levantar el contenedor BCCR
    2.2 - docker exec bccr-scheduler python3 bccr_exchange_rate.py populate

# 3 Leventar bases de datos.
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

# 4 Carga de datos. 

# 5 ETLS}





Cómo probar end-to-end:

Genera datos/landing: python shared/seed_all.py (opcional --push). #Check

Rebuild y levanta DWH: cd DWH && docker-compose build --no-cache && docker-compose up -d. # Check
Ejecuta ETLs en el scheduler (opcional manual): 
docker exec dwh-scheduler python etl_mongo.py #Check
docker exec dwh-scheduler python etl_mssql_src.py #Check
docker exec dwh-scheduler python etl_mysql.py #Check
docker exec dwh-scheduler python etl_supabase.py #Check
docker exec dwh-scheduler python etl_neo4j.py # Tiene error


Promoción: docker exec dwh-scheduler python bccr_exchange_rate.py populate y luego en SQL EXEC sp_etl_run_all;.
Verifica en DWH:
SELECT COUNT(*) FROM staging.mongo_orders; (9026)
SELECT COUNT(*) FROM staging.mssql_products; (550)
SELECT COUNT(*) FROM staging.mysql_products; (550)
SELECT COUNT(*) FROM staging.supabase_users; (1120)
SELECT COUNT(*) FROM dwh.DimExchangeRate; (una por día CRC→USD).
