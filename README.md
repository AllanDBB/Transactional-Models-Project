# Transactional Models Project - Docker Setup

Este proyecto contiene configuraciones Docker para múltiples bases de datos y sistemas de almacenamiento.

## 🚀 Inicio Rápido

### Prerrequisitos
- Docker Desktop instalado
- Docker Compose v2.0 o superior
- Mínimo 8GB RAM disponible

### Configuración Inicial

1. **Clonar variables de entorno**
```bash
cp .env.example .env
```

2. **Editar credenciales** (opcional)
Modifica el archivo `.env` con tus credenciales personalizadas.

3. **Iniciar todos los servicios**
```bash
docker-compose up -d
```

4. **Iniciar servicios específicos**
```bash
# Solo MongoDB
docker-compose up -d mongodb mongo-express

# Solo MySQL
docker-compose up -d mysql phpmyadmin

# Solo MSSQL
docker-compose up -d mssql

# Solo Neo4j
docker-compose up -d neo4j

# Solo PostgreSQL
docker-compose up -d postgres pgadmin

# Solo ClickHouse (DWH)
docker-compose up -d clickhouse
```

## 📦 Servicios Disponibles

### MongoDB
- **Puerto**: 27017
- **GUI**: http://localhost:8081 (Mongo Express)
- **Credenciales**: admin/admin123

### MySQL
- **Puerto**: 3306
- **GUI**: http://localhost:8082 (phpMyAdmin)
- **Credenciales**: user/user123

### MS SQL Server
- **Puerto**: 1433
- **Credenciales**: sa/YourStrong@Password123

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

### Ver logs
```bash
# Todos los servicios
docker-compose logs -f

# Servicio específico
docker-compose logs -f mongodb
```

### Detener servicios
```bash
# Todos
docker-compose down

# Con limpieza de volúmenes (CUIDADO: elimina datos)
docker-compose down -v
```

### Reiniciar servicio
```bash
docker-compose restart mongodb
```

### Acceder a un contenedor
```bash
# MongoDB
docker exec -it transactional-mongodb mongosh

# MySQL
docker exec -it transactional-mysql mysql -u root -p

# MSSQL
docker exec -it transactional-mssql /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P YourStrong@Password123

# Neo4j
docker exec -it transactional-neo4j cypher-shell -u neo4j -p password123

# PostgreSQL
docker exec -it transactional-postgres psql -U postgres
```

### Ver estado de servicios
```bash
docker-compose ps
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

## 📚 Recursos

- [Docker Documentation](https://docs.docker.com/)
- [MongoDB Docker Hub](https://hub.docker.com/_/mongo)
- [MySQL Docker Hub](https://hub.docker.com/_/mysql)
- [Neo4j Docker Hub](https://hub.docker.com/_/neo4j)
- [PostgreSQL Docker Hub](https://hub.docker.com/_/postgres)
