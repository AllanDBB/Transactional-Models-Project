# 1 Levantar contenedor el DWH
    1.1 - Crear schema

# 2 Levantar el contenedor BCCR
    2.2 - docker exec bccr-scheduler python3 bccr_exchange_rate.py populate

# 3 Leventar bases de datos.
    3.1 - Levantar MYSQL:
        cd MYSQL; docker compose up -d --build
    3.2 - Levantar MSSQL: 
        cd 
    3.3 - Levantar SupaBase: No está
    3.4 - Levantar MongoDB: 
        cd MONGODB; npm i, npm run dev
        * Presentar error crear ordenes.
    3.5 - 


# 4 Carga de datos. 