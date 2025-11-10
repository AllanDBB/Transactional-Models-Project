# PostgreSQL / Supabase - Transactional Models Project

## 🚀 Inicio Rápido

```bash
cd SUPABASE
docker-compose up -d
```

## 📦 Servicios

- **PostgreSQL**: Puerto 5432
- **pgAdmin**: http://localhost:5050
- **Credenciales DB**: postgres / postgres123
- **Credenciales pgAdmin**: admin@admin.com / admin123

## 🔄 ETL Process

```bash
pip install -r etl/requirements.txt
python etl/run_etl.py
```

## 📁 Estructura

```
SUPABASE/
├── docker-compose.yml
├── Dockerfile
├── init/
├── data/
└── etl/
```
