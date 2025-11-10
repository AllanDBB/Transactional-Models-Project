# MongoDB - Transactional Models Project

## 🚀 Inicio Rápido

### Iniciar MongoDB
```bash
cd MONGODB
docker-compose up -d
```

### Acceder a MongoDB
```bash
# Mongo Shell
docker exec -it mongodb-container mongosh -u admin -p admin123

# Mongo Express (GUI)
# http://localhost:8081
```

## 📦 Servicios

- **MongoDB**: Puerto 27017
- **Mongo Express**: http://localhost:8081

## 🔄 ETL Process

El proceso ETL para MongoDB incluye:
1. **Extract**: Lectura de fuentes de datos (CSV, JSON, APIs)
2. **Transform**: Limpieza y transformación de datos
3. **Load**: Carga a colecciones MongoDB

### Ejecutar ETL
```bash
# Instalar dependencias
pip install -r etl/requirements.txt

# Ejecutar ETL
python etl/run_etl.py
```

## 📁 Estructura
```
MONGODB/
├── docker-compose.yml       # Configuración Docker
├── Dockerfile              # Imagen personalizada
├── init/                   # Scripts de inicialización
├── config/                 # Configuraciones
├── data/                   # Datos de ejemplo
└── etl/                    # Proceso ETL
    ├── extract/           # Scripts de extracción
    ├── transform/         # Scripts de transformación
    ├── load/              # Scripts de carga
    └── run_etl.py         # Orquestador principal
```
