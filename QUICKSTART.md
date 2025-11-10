# Quick Start Guide 🚀

## Inicio Rápido para cada Base de Datos

### 1. MongoDB

```bash
cd MONGODB
cp .env.example .env
docker-compose up -d
```

**Acceso:**
- MongoDB: `mongodb://admin:admin123@localhost:27017`
- GUI: http://localhost:8081 (admin/pass)

**ETL:**
```bash
cd etl
pip install -r requirements.txt
python run_etl.py
```

---

### 2. MySQL

```bash
cd MYSQL
cp .env.example .env
docker-compose up -d
```

**Acceso:**
- MySQL: `localhost:3306` (user/user123)
- GUI: http://localhost:8082

**ETL:**
```bash
cd etl
pip install -r requirements.txt
python run_etl.py
```

---

### 3. MS SQL Server

```bash
cd MSSQL
cp .env.example .env
docker-compose up -d
```

**Acceso:**
- MSSQL: `localhost:1433` (sa/YourStrong@Password123)

**ETL:**
```bash
cd etl
pip install -r requirements.txt
python run_etl.py
```

---

### 4. Neo4j

```bash
cd NEO4J
cp .env.example .env
docker-compose up -d
```

**Acceso:**
- Browser: http://localhost:7474 (neo4j/password123)
- Bolt: `bolt://localhost:7687`

**ETL:**
```bash
cd etl
pip install -r requirements.txt
python run_etl.py
```

---

### 5. PostgreSQL / Supabase

```bash
cd SUPABASE
cp .env.example .env
docker-compose up -d
```

**Acceso:**
- PostgreSQL: `localhost:5432` (postgres/postgres123)
- GUI: http://localhost:5050 (admin@admin.com/admin123)

**ETL:**
```bash
cd etl
pip install -r requirements.txt
python run_etl.py
```

---

### 6. Data Warehouse (ClickHouse)

```bash
cd DWH
cp .env.example .env
docker-compose up -d
```

**Acceso:**
- HTTP: http://localhost:8123
- Native: `localhost:9000` (default/clickhouse123)

**ETL (Extrae de todas las fuentes):**
```bash
cd etl
pip install -r requirements.txt
python run_etl.py
```

---

## Comandos Útiles Globales

### Ver todos los contenedores

```bash
docker ps -a
```

### Detener todo

```bash
# Opción 1: Desde cada carpeta
cd MONGODB && docker-compose down
cd MYSQL && docker-compose down
cd MSSQL && docker-compose down
cd NEO4J && docker-compose down
cd SUPABASE && docker-compose down
cd DWH && docker-compose down

# Opción 2: Desde la raíz (si usaste docker-compose.yml principal)
docker-compose down
```

### Ver logs de un servicio

```bash
docker logs -f nombre-contenedor
```

### Verificar conectividad

```bash
# MongoDB
docker exec -it mongodb-container mongosh -u admin -p admin123

# MySQL
docker exec -it mysql-container mysql -u user -p

# MSSQL
docker exec -it mssql-container /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'YourStrong@Password123'

# Neo4j
docker exec -it neo4j-container cypher-shell -u neo4j -p password123

# PostgreSQL
docker exec -it postgres-container psql -U postgres

# ClickHouse
docker exec -it clickhouse-container clickhouse-client --password clickhouse123
```

---

## Solución de Problemas Comunes

### Puerto en uso

```powershell
# Ver qué proceso usa el puerto
netstat -ano | findstr :27017

# Matar proceso (reemplaza PID)
taskkill /PID <PID> /F
```

### Contenedor no inicia

```bash
# Ver logs
docker logs nombre-contenedor

# Reiniciar
docker restart nombre-contenedor

# Reconstruir
cd CARPETA
docker-compose down
docker-compose build --no-cache
docker-compose up -d
```

### Limpiar todo Docker (CUIDADO: Elimina datos)

```bash
docker system prune -a --volumes
```

---

## Próximos Pasos

1. ✅ Iniciar las bases de datos que necesites
2. ✅ Verificar acceso usando las GUIs
3. ✅ Ejecutar los ETLs para poblar datos
4. ✅ Explorar los datos en cada sistema
5. ✅ Personalizar según tus necesidades

---

## Recursos

- [Documentación completa](./README.md)
- Documentación individual en cada carpeta: `MONGODB/README.md`, etc.
