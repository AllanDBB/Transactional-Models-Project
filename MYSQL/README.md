# MySQL - Transactional Database ETL

## Quick Start

### Start Docker Containers

```bash
cd MYSQL
docker-compose up -d
```

This will start:
- **MySQL transactional database** on port 3306
- **phpMyAdmin** GUI on port 8082 (http://localhost:8082)

### Install Python Dependencies

```bash
cd MYSQL/etl
pip install -r requirements.txt
```

### Run ETL

```bash
python run_etl.py
```

---

## Services

- **MySQL (Transactional)**: Port 3306 → sales_mysql
- **Data Warehouse (DWH)**: Port 1434 → MSSQL_DW (shared)
- **phpMyAdmin**: Port 8082 → Database Management UI

**Credentials:**
- MySQL: `user` / `user123` (or `root` / `root123`)
- DWH: `admin` / `admin123`

---

## Project Structure

```
MYSQL/
├── README.md                      (this file)
├── docker-compose.yml             (MySQL + phpMyAdmin services)
├── Dockerfile                     (MySQL 8.0 with custom entrypoint)
├── docker-entrypoint.sh           (initialization script executor)
├── .env.example                   (environment template)
├── init/                          (SQL initialization scripts)
│   ├── 00-create-database.sql
│   ├── 00-sp_init_schema.sql      (creates tables)
│   ├── 03-sp_limpiar_bd.sql       (cleanup procedure)
│   ├── 04-sp_generar_datos.sql    (test data generation)
│   └── 05-sp_drop_schema.sql      (drop schema)
├── etl/
│   ├── config.py                  (database connections + .env support)
│   ├── .env.example               (ETL environment template)
│   ├── requirements.txt
│   ├── run_etl.py                 (main ETL orchestration)
│   ├── extract/
│   │   └── __init__.py            (DataExtractor class)
│   ├── transform/
│   │   └── __init__.py            (DataTransformer with 5 integration rules)
│   └── load/
│       └── __init__.py            (DataLoader class)
└── config/                        (MySQL configuration)
```

---

## 5 Integration Rules Implemented

1. **REGLA 1:** Product Homologization
   - Maps `codigo_alt` (MySQL alternative code) across sources
   - Bridges with MSSQL SKU and MongoDB codes

2. **REGLA 2:** Currency Normalization
   - Converts CRC → USD using exchange rates
   - Handles mixed currency data (USD/CRC split)

3. **REGLA 3:** Gender Standardization
   - Converts ENUM('M','F','X') → Masculino/Femenino/No especificado

4. **REGLA 4:** Date Conversion
   - VARCHAR 'YYYY-MM-DD' → DATE
   - VARCHAR 'YYYY-MM-DD HH:MM:SS' → DATETIME

5. **REGLA 5:** Amount Transformation
   - VARCHAR with mixed formats → DECIMAL
   - Handles: '1,200.50' and '1.200,50' formats

---

## MySQL-Specific Heterogeneities

- **codigo_alt**: Alternative product code (NOT official SKU)
- **Dates as VARCHAR**: 'YYYY-MM-DD' for created_at, 'YYYY-MM-DD HH:MM:SS' for fecha
- **Amounts as VARCHAR**: Can have commas or periods ('1,200.50' or '1200.50')
- **Gender ENUM**: Different from MSSQL format (M/F/X vs Masculino/Femenino)
- **Mixed Currency**: Orders in USD or CRC
- **No Discount Field**: OrdenDetalle table has no descuento field (MySQL-specific)

---

## Docker Setup Details

### Initialization Process

When `docker-compose up` runs:

1. **MySQL container starts** with the base image
2. **docker-entrypoint.sh executes** all SQL scripts in `/docker-entrypoint-initdb.d/`
3. **Schema is created** via `sp_init_schema` stored procedure
4. **ETL scripts can then be run** via Python

### Scripts Execution Order

The init scripts are executed alphabetically:

1. `00-create-database.sql` - Creates `sales_mysql` database
2. `00-sp_init_schema.sql` - Creates tables and indexes
3. `03-sp_limpiar_bd.sql` - Cleanup procedure (available for reuse)
4. `04-sp_generar_datos.sql` - Test data generation procedure
5. `05-sp_drop_schema.sql` - Schema drop procedure

---

## Environment Configuration

### Local Setup (.env file)

Copy the example:
```bash
cp .env.example .env
```

For **local Docker development**, use:
```env
MYSQL_ROOT_PASSWORD=root123
MYSQL_DATABASE=sales_mysql
MYSQL_USER=user
MYSQL_PASSWORD=user123
```

### ETL Configuration (etl/.env file)

```bash
cp etl/.env.example etl/.env
```

For **local development**:
```env
MYSQL_HOST=mysql-transactional
MYSQL_PORT=3306
MYSQL_DATABASE=sales_mysql
MYSQL_USER=user
MYSQL_PASSWORD=user123

MSSQL_DW_SERVER=localhost
MSSQL_DW_PORT=1434
MSSQL_DW_USER=admin
MSSQL_DW_PASSWORD=admin123
```

For **remote DWH** (multi-team setup):
```env
MSSQL_DW_SERVER=192.168.100.50
MSSQL_DW_PORT=1434
```

---

## Common Tasks

### Generate Test Data

```bash
cd MYSQL
docker-compose exec mysql mysql -u root -proot123 sales_mysql -e "CALL sp_generar_dados();"
```

### Clean Database

```bash
cd MYSQL
docker-compose exec mysql mysql -u root -proot123 sales_mysql -e "CALL sp_limpiar_bd();"
```

### Run ETL Pipeline

```bash
cd MYSQL/etl
python run_etl.py
```

### Stop Containers

```bash
cd MYSQL
docker-compose down
```

### View Logs

```bash
cd MYSQL
docker-compose logs -f mysql
```

---

## Troubleshooting

### MySQL Connection Fails

Verify MySQL is ready:
```bash
docker-compose exec mysql mysql -u root -proot123 -e "SELECT 1;"
```

### phpMyAdmin Access Issues

Ensure container is running:
```bash
docker-compose ps
```

Access at: http://localhost:8082

### ETL Fails to Connect to DWH

Check DWH connectivity:
```bash
cd MYSQL/etl
python -c "from config import DatabaseConfig; print(DatabaseConfig.get_dw_connection_string())"
```

Verify MSSQL DW is running on port 1434.

---

## Multi-Team Setup

For team members on different machines sharing a DWH:

1. Get the DWH server IP (e.g., 192.168.100.50)
2. Copy `etl/.env.example` to `etl/.env`
3. Update `MSSQL_DW_SERVER`:
   ```env
   MSSQL_DW_SERVER=192.168.100.50
   ```
4. Run: `python etl/run_etl.py`

---

## DWH Integration

All ETL scripts load into the shared DWH: `MSSQL_DW`

Verify multi-source data:
```sql
SELECT source_system, COUNT(*) as records
FROM staging_source_tracking
GROUP BY source_system
```

Expected sources: MYSQL, MSSQL, MONGODB, NEO4J, POSTGRESQL
