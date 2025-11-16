# Transactional Models Project 🚀

Proyecto completo de modelos transaccionales con múltiples bases de datos y procesos ETL independientes.

## 📋 Contenido

Este proyecto incluye configuraciones Docker y ETLs para:

- **MongoDB** - Base de datos NoSQL orientada a documentos
- **MySQL** - Base de datos relacional
- **MS SQL Server** - Base de datos relacional de Microsoft
- **Neo4j** - Base de datos de grafos
- **PostgreSQL/Supabase** - Base de datos relacional avanzada
- **ClickHouse (DWH)** - Data Warehouse para análisis OLAP
- **BCCR** - Módulo compartido de integración con Banco Central de Costa Rica (tipos de cambio)

## 🏗️ Arquitectura

Cada base de datos es **completamente independiente** con su propio:
- `docker-compose.yml` - Para iniciar solo ese servicio
- `Dockerfile` - Imagen personalizada
- `init/` - Scripts de inicialización automática
- `etl/` - Proceso ETL (Extract, Transform, Load)
- `data/` - Datos de ejemplo
- `.env.example` - Configuración de variables

## 🚀 Inicio Rápido

### Prerrequisitos
- Docker Desktop instalado
- Docker Compose v2.0 o superior
- Python 3.10+ instalado
- Mínimo 8GB RAM disponible

### Configuración Inicial

#### 1. Configurar Entorno Virtual Python

**Windows (PowerShell):**
```powershell
# Crear entorno virtual
python -m venv .venv

# Activar entorno virtual
.\.venv\Scripts\Activate.ps1

# Si hay error de políticas de ejecución:
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

**Linux/Mac:**
```bash
# Crear entorno virtual
python3 -m venv .venv

# Activar entorno virtual
source .venv/bin/activate
```

#### 2. Instalar Dependencias Python

```powershell
# Dependencias generales
pip install pyodbc python-dotenv pandas requests

# Para MSSQL específicamente
cd MSSQL/etl
pip install -r requirements.txt
```

#### 3. Configurar Variables de Entorno

```bash
cp .env.example .env
```

Edita el archivo `.env` con tus credenciales personalizadas.

#### 4. Iniciar Bases de Datos con Docker

**Iniciar todos los servicios:**
```bash
docker-compose up -d
```

**Iniciar servicios específicos:**
```bash
# Solo MSSQL (Transaccional + DWH)
cd MSSQL
docker-compose up -d

# Solo MongoDB
cd MONGODB
docker-compose up -d

# Solo MySQL
cd MYSQL
docker-compose up -d

# Solo Neo4j
cd NEO4J
docker-compose up -d

# Solo PostgreSQL/Supabase
cd SUPABASE
docker-compose up -d

# Solo ClickHouse (DWH)
cd DWH
docker-compose up -d
```

#### 5. Inicializar Datos y Ejecutar ETL

**Para MSSQL:**
```powershell
# 1. Ejecuta 01-init.sql en SSMS para crear tablas
# 2. Ejecuta 02-insert_test_data.sql para insertar 600 clientes, 5000 productos, 1000 órdenes

# 3. Ejecutar ETL
cd MSSQL\etl
python run_etl.py

# 4. (Opcional) Limpiar todo y empezar de nuevo
python limpiar_todo.py
```

## 📦 Servicios Disponibles

### MS SQL Server (Transaccional)
- **Puerto**: 1433
- **Base de datos**: SalesDB_MSSQL
- **Credenciales**: sa/YourStrong@Passw0rd
- **Conexión**: `localhost:1433`
- **Schema**: `sales_ms` (Cliente, Producto, Orden, OrdenDetalle)

### MS SQL Server (DWH - Data Warehouse)
- **Puerto**: 1434
- **Base de datos**: MSSQL_DW
- **Credenciales**: sa/YourStrong@Passw0rd
- **Conexión**: `192.168.100.50:1434` (red Docker)
- **Tablas**: DimCustomer, DimProduct, DimCategory, DimChannel, DimTime, FactSales
- **Staging**: staging_map_producto, staging_tipo_cambio, staging_source_tracking

### MongoDB
- **Puerto**: 27017
- **GUI**: http://localhost:8081 (Mongo Express)
- **Credenciales**: admin/admin123

### MySQL
- **Puerto**: 3306
- **GUI**: http://localhost:8082 (phpMyAdmin)
- **Credenciales**: user/user123

### Neo4j
- **Puerto HTTP**: 7474
- **Puerto Bolt**: 7687
- **Browser**: http://localhost:7474
- **Credenciales**: neo4j/password123

### PostgreSQL (Supabase)
- **Puerto**: 5432
- **GUI**: http://localhost:5050 (pgAdmin)
- **Credenciales**: postgres/postgres123

### ClickHouse (DWH)
- **Puerto HTTP**: 8123
- **Puerto Native**: 9000
- **Credenciales**: default/clickhouse123

### Redis
- **Puerto**: 6379

## 🛠️ Comandos Útiles

### Entorno Virtual Python

```powershell
# Activar entorno (Windows)
.\.venv\Scripts\Activate.ps1

# Activar entorno (Linux/Mac)
source .venv/bin/activate

# Desactivar
deactivate

# Verificar Python activo
python --version
which python  # Linux/Mac
Get-Command python  # Windows PowerShell
```

### Docker

**Ver logs:**
```bash
# Todos los servicios
docker-compose logs -f

# Servicio específico
docker-compose logs -f mssql
docker-compose logs -f mssql-dwh
```

**Detener servicios:**
```bash
# Todos
docker-compose down

# Con limpieza de volúmenes (CUIDADO: elimina datos)
docker-compose down -v
```

**Reiniciar servicio:**
```bash
docker-compose restart mssql
```

**Acceder a contenedores:**
```bash
# MSSQL Transaccional
docker exec -it mssql-transactional /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P YourStrong@Passw0rd

# MSSQL DWH
docker exec -it mssql-dwh /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P YourStrong@Passw0rd

# MongoDB
docker exec -it transactional-mongodb mongosh

# MySQL
docker exec -it transactional-mysql mysql -u root -p

# Neo4j
docker exec -it transactional-neo4j cypher-shell -u neo4j -p password123

# PostgreSQL
docker exec -it transactional-postgres psql -U postgres
```

**Ver estado:**
```bash
docker-compose ps
docker ps -a
```

### ETL (MSSQL)

```powershell
# Activar entorno
cd MSSQL\etl
.\.venv\Scripts\Activate.ps1

# Ejecutar ETL
python run_etl.py

# Limpiar DWH y BD transaccional
python limpiar_todo.py

# Ver estructura final
cat ESTRUCTURA_FINAL.md
```

## 📁 Estructura del Proyecto

```
.
├── docker-compose.yml          # Configuración principal de Docker Compose
├── .env.example                # Plantilla de variables de entorno
├── .env                        # Variables de entorno (crear manualmente)
├── MONGODB/
│   ├── Dockerfile
│   ├── init/                   # Scripts de inicialización
│   └── config/                 # Configuraciones personalizadas
├── MYSQL/
│   ├── Dockerfile
│   ├── init/                   # Scripts SQL de inicialización
│   └── config/                 # Configuraciones personalizadas
├── MSSQL/
│   ├── Dockerfile
│   ├── init/                   # Scripts SQL de inicialización
│   └── backup/                 # Directorio de backups
├── NEO4J/
│   ├── Dockerfile
│   └── init/                   # Scripts Cypher de inicialización
├── SUPABASE/
│   ├── Dockerfile
│   └── init/                   # Scripts SQL de inicialización
└── DWH/
    ├── Dockerfile
    ├── init/                   # Scripts SQL de inicialización
    └── config/                 # Configuraciones de ClickHouse
```

## 🔧 Configuración Avanzada

### Importar datos iniciales

Coloca tus scripts de inicialización en las carpetas `init/` correspondientes:
- **MongoDB**: `.js` archivos
- **MySQL/PostgreSQL**: `.sql` archivos
- **MSSQL**: `.sql` archivos
- **Neo4j**: `.cypher` archivos

### Backups

```bash
# MongoDB
docker exec transactional-mongodb mongodump --out /data/backup

# MySQL
docker exec transactional-mysql mysqldump -u root -p transactional_db > backup.sql

# PostgreSQL
docker exec transactional-postgres pg_dump -U postgres transactional_db > backup.sql
```

## ⚠️ Notas Importantes

1. **Seguridad**: Las credenciales por defecto son para desarrollo. Cámbialas en producción.
2. **Recursos**: Asegúrate de tener suficientes recursos (CPU, RAM, Disco).
3. **Puertos**: Verifica que los puertos no estén en uso antes de iniciar.
4. **Datos**: Los datos persisten en volúmenes de Docker. Usa `docker-compose down -v` solo si quieres eliminarlos.

## 🆘 Troubleshooting

### Puerto ya en uso
```bash
# Ver procesos usando un puerto
netstat -ano | findstr :27017
```

### Limpiar todo y empezar de nuevo
```bash
docker-compose down -v
docker system prune -a --volumes
docker-compose up -d
```

### Logs detallados
```bash
docker-compose logs --tail=100 -f <servicio>
```

## 📊 Módulo BCCR (Compartido)

El módulo de integración con el Banco Central de Costa Rica está centralizado en `/BCCR` para que **todos los ETLs** lo usen:

- **Ubicación**: `/BCCR/src/bccr_integration.py`
- **Documentación**: `/BCCR/README.md`
- **Guía de integración**: `/BCCR/INTEGRACION.md`
- **Ejemplo de uso**: `/BCCR/ejemplo_uso.py`

**Uso desde cualquier ETL:**
```python
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'BCCR' / 'src'))
from bccr_integration import BCCRIntegration

bccr = BCCRIntegration()
df_tasas = bccr.get_historical_rates(years_back=3)
```

## 📚 Recursos

- [Docker Documentation](https://docs.docker.com/)
- [MongoDB Docker Hub](https://hub.docker.com/_/mongo)
- [MySQL Docker Hub](https://hub.docker.com/_/mysql)
- [Neo4j Docker Hub](https://hub.docker.com/_/neo4j)
- [PostgreSQL Docker Hub](https://hub.docker.com/_/postgres)
- [BCCR API Documentation](https://gee.bccr.fi.cr)
