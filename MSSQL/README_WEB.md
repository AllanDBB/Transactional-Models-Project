# Panel Web MSSQL

Panel de administración web para gestionar la base de datos transaccional MSSQL.

## Características

- ✅ **Inicializar Schema**: Crea todas las tablas necesarias
- 🧹 **Limpiar Datos**: Elimina todos los registros
- 📊 **Generar Datos**: Genera datos de prueba (600 clientes, 5000 productos, 5000 órdenes, 17500 detalles)
- 📈 **Estadísticas**: Visualiza el estado actual de la base de datos

## Instalación

```bash
cd MSSQL/server
npm install
```

## Configuración

Copia `.env.example` a `.env` y ajusta las credenciales:

```bash
cp .env.example .env
```

## Ejecución

```bash
# Modo producción
npm start

# Modo desarrollo (con nodemon)
npm run dev
```

El servidor estará disponible en: http://localhost:3001

## Requisitos Previos

1. **Docker** con el contenedor `mssql_transaccional` corriendo
2. **Stored Procedures** creados en la base de datos:
   - `dbo.sp_init_schema`
   - `sales_ms.sp_limpiar_bd`
   - `sales_ms.sp_generar_datos`

## Crear Stored Procedures

```bash
# Desde la raíz del proyecto
docker exec -it mssql_transaccional /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "SaPassword123!" -i /docker-entrypoint-initdb.d/00-sp_init_schema.sql
docker exec -it mssql_transaccional /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "SaPassword123!" -i /docker-entrypoint-initdb.d/03-sp_limpiar_bd.sql
docker exec -it mssql_transaccional /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "SaPassword123!" -i /docker-entrypoint-initdb.d/04-sp_generar_datos.sql
```

## API Endpoints

- `POST /api/mssql/init-schema` - Inicializar schema
- `POST /api/mssql/clean` - Limpiar base de datos
- `POST /api/mssql/generate-data` - Generar datos de prueba
- `GET /api/mssql/stats` - Obtener estadísticas
- `GET /health` - Health check

## Estructura

```
MSSQL/
├── client/
│   └── public/
│       ├── index.html    # Interfaz web
│       ├── style.css     # Estilos
│       └── app.js        # Lógica del cliente
└── server/
    ├── package.json
    ├── .env.example
    └── src/
        ├── index.js      # Servidor Express
        └── routes/
            └── mssqlRoutes.js   # API endpoints
```
