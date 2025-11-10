# MySQL - Transactional Models Project

## 🚀 Inicio Rápido

### Iniciar MySQL
```bash
cd MYSQL
docker-compose up -d
```

### Acceder a MySQL
```bash
# MySQL CLI
docker exec -it mysql-container mysql -u user -p

# phpMyAdmin (GUI)
# http://localhost:8082
```

## 📦 Servicios

- **MySQL**: Puerto 3306
- **phpMyAdmin**: http://localhost:8082

## 🔄 ETL Process

### Ejecutar ETL
```bash
pip install -r etl/requirements.txt
python etl/run_etl.py
```

## 📁 Estructura
```
MYSQL/
├── docker-compose.yml
├── Dockerfile
├── init/
├── config/
├── data/
└── etl/
```
