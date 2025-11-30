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

# 5 ETLS
