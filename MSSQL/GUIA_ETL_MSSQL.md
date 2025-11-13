# ETL MSSQL → DWH - GUÍA DE INSTALACIÓN Y USO

## 📋 Resumen Ejecutivo

Se ha implementado un **ETL completo en Python** que:
- ✅ Extrae datos de `SalesDB_MSSQL` (BD transaccional)
- ✅ Aplica transformaciones según las reglas de integración
- ✅ Carga los datos normalizados en `MSSQL_DW` (Data Warehouse)

## 🏗️ Estructura del Proyecto

```
MSSQL/
├── etl/                    # 👈 NUEVO: Carpeta ETL
│   ├── config.py          # Configuración de conexiones
│   ├── run_etl.py         # Script principal
│   ├── id_mapper.py       # Mapeo de IDs (auxiliar)
│   ├── requirements.txt   # Dependencias
│   ├── README.md          # Documentación detallada
│   ├── extract/
│   │   └── __init__.py    # Clase DataExtractor
│   ├── transform/
│   │   └── __init__.py    # Clase DataTransformer
│   └── load/
│       └── __init__.py    # Clase DataLoader
├── init/
│   ├── 01-init.sql        # Creación de base de datos y esquema
│   └── 02-sample-data.sql # 👈 NUEVO: Datos de prueba
├── docker-compose.yml     # Docker SQL Server
└── README.md
```

## 🚀 Pasos de Instalación y Ejecución

### Paso 1: Levantar los contenedores Docker

```powershell
cd "c:\Users\Santiago Valverde\Downloads\University\BD2\Transactional-Models-Project\MSSQL"
docker-compose up -d
```

**Esperar 30-60 segundos** para que SQL Server inicie completamente.

### Paso 2: Crear las bases de datos

Ejecutar en SSMS (`Server: localhost,1433`):

```sql
-- Ejecutar el script 01-init.sql para crear SalesDB_MSSQL y el esquema
```

O si prefieres desde PowerShell (con `sqlcmd`):

```powershell
sqlcmd -S localhost,1433 -U sa -P "BasesDatos2!" -i "init\01-init.sql"
```

### Paso 3: Insertar datos de prueba

En SSMS, ejecutar:

```sql
-- Ejecutar el script 02-sample-data.sql para insertar 5 clientes, 5 productos, 5 órdenes
```

### Paso 4: Crear el Data Warehouse

En SSMS, conectar con usuario `admin / admin123`:

```sql
-- Ejecutar el script DWH/init/01-init.sql
```

### Paso 5: Instalar dependencias Python

```powershell
cd "MSSQL\etl"
pip install -r requirements.txt
```

Si hay problemas con `pyodbc`, instalar también:

```powershell
pip install pyodbc
```

### Paso 6: Ejecutar el ETL

```powershell
cd "MSSQL\etl"
python run_etl.py
```

**Salida esperada:**

```
2024-11-12 10:45:30 - __main__ - INFO - ================================================================================
2024-11-12 10:45:30 - __main__ - INFO - INICIANDO PROCESO ETL: MSSQL → DWH
2024-11-12 10:45:30 - __main__ - INFO - ================================================================================

[FASE 1] EXTRAYENDO DATOS...
✓ Clientes extraídos: 5
✓ Productos extraídos: 5
✓ Órdenes extraídas: 5
✓ Detalles extraídos: 10

[FASE 2] TRANSFORMANDO DATOS...
✓ Clientes transformados: 5
✓ Productos transformados: 5
✓ Órdenes transformadas: 5
✓ Detalles transformados: 10
✓ Categorías extraídas: 2
✓ Canales extraídos: 3
✓ Fechas en DimTime: 5

[FASE 3] CARGANDO DATOS AL DWH...
✓ Dimensiones cargadas correctamente

================================================================================
✅ PROCESO ETL COMPLETADO EXITOSAMENTE
================================================================================
```

## 📊 Reglas de Transformación Implementadas

| Regla | Fuente | Transformación | Destino |
|-------|--------|-----------------|---------|
| **Género** | `Masculino, Femenino` | → `M, F` | `DimCustomer.gender` |
| **Moneda** | Siempre `USD` | Sin cambio | `FactSales.lineTotalUSD` |
| **SKU** | `codigo_sku` | Normalizar (uppercase) | `DimProduct.code` |
| **Fecha** | `DATETIME2` | Convertir a `DATE` | `DimTime.date` |
| **Precio** | `DECIMAL(18,2)` | Validar ≥ 0 | `DimProduct` |
| **Descuento** | `NULL o 0-100%` | Limitar 0-100% | `FactSales.discountPercentage` |
| **Total Línea** | Calculado | `Precio × Cant × (1-Desc%)` | `FactSales.lineTotalUSD` |

## 🔍 Verificar Datos en el DWH

Después de ejecutar el ETL, conectar en SSMS con `admin / admin123`:

```sql
USE MSSQL_DW;

-- Ver clientes cargados
SELECT * FROM DimCustomer;

-- Ver productos cargados
SELECT * FROM DimProduct;

-- Ver dimensiones
SELECT * FROM DimCategory;
SELECT * FROM DimChannel;
SELECT * FROM DimTime;

-- Ver fact table (vacía por ahora, se llena en próxima versión)
SELECT * FROM FactSales;
```

## ⚙️ Configuración Avanzada

### Cambiar credenciales

Editar `MSSQL/etl/config.py`:

```python
SOURCE_DB = {
    'server': 'localhost',
    'port': 1433,
    'database': 'SalesDB_MSSQL',
    'uid': 'sa',
    'pwd': 'BasesDatos2!'
}

DW_DB = {
    'server': 'localhost',
    'port': 1433,
    'database': 'MSSQL_DW',
    'uid': 'admin',
    'pwd': 'admin123'
}
```

### Usar variables de entorno

Crear archivo `.env` en `MSSQL/etl/`:

```
MSSQL_SERVER=localhost
MSSQL_PORT=1433
MSSQL_USER=sa
MSSQL_PASSWORD=BasesDatos2!

MSSQL_DW_SERVER=localhost
MSSQL_DW_PORT=1433
MSSQL_DW_USER=admin
MSSQL_DW_PASSWORD=admin123
```

El script cargará automáticamente estas variables.

## 🐛 Troubleshooting

### ❌ Error: "Cannot connect to SQL Server"

**Causa**: Docker no está corriendo o SQL Server no ha iniciado completamente

**Solución**:
```powershell
# Verificar contenedor
docker-compose ps

# Ver logs
docker-compose logs sqlserver

# Esperar 60 segundos y reintentar
```

### ❌ Error: "Import pyodbc not found"

**Causa**: Dependencias no instaladas

**Solución**:
```powershell
pip install -r requirements.txt
pip install pyodbc
```

### ❌ Error: "Database already exists"

**Causa**: Base de datos ya creada de ejecuciones anteriores

**Solución**: El script `01-init.sql` ya maneja esto automáticamente. Si persiste, ejecutar en SSMS:

```sql
DROP DATABASE IF EXISTS SalesDB_MSSQL;
DROP DATABASE IF EXISTS MSSQL_DW;
```

### ❌ Error: "Transaction log is full"

**Causa**: Archivo de log alcanzó su límite

**Solución**:
```sql
-- En SSMS
DBCC SHRINKFILE(MSSQL_DW_log, 1);
```

## 📝 Logs

El archivo `etl_process.log` se genera en `MSSQL/etl/` y contiene todos los detalles del proceso.

Ver logs en tiempo real:

```powershell
Get-Content -Path "MSSQL/etl/etl_process.log" -Tail 50 -Wait
```

## 🔄 Próximas Fases

### Fase 2: Integración de MySQL

```
MySQL (Transaccional)
    ├── codigo_alt (alternativo)
    ├── Moneda: USD/CRC
    ├── Género: M/F/X
    ├── Fechas: VARCHAR
    └── Precios: VARCHAR con comas/puntos
        ↓
    Transformaciones adicionales
        ↓
    MSSQL_DW (Mismo DWH)
```

### Fase 3: Integración de MongoDB

```
MongoDB (Documentos)
    ├── codigo_mongo
    ├── Moneda: CRC (enteros)
    ├── Totales en items[]
    └── Estructura anidada
        ↓
    Transformaciones + Conversión CRC→USD
        ↓
    MSSQL_DW
```

### Fase 4: Integración de Supabase/PostgreSQL

```
Supabase (UUIDs)
    ├── cliente_id: UUID
    ├── producto_id: UUID (algunos NULL)
    ├── Género: M/F
    ├── Moneda: USD/CRC
    └── SKU: puede estar vacío
        ↓
    Transformaciones + Mapeo UUID→INT
        ↓
    MSSQL_DW
```

## 📚 Documentación Adicional

- `MSSQL/etl/README.md` - Documentación técnica del ETL
- `instrucciones.txt` - Especificaciones del proyecto
- `DWH/README.md` - Especificaciones del Data Warehouse

## ✅ Checklist

- [ ] Docker está corriendo (`docker-compose up -d`)
- [ ] Base de datos transaccional creada (`01-init.sql` ejecutado)
- [ ] Datos de prueba insertados (`02-sample-data.sql` ejecutado)
- [ ] Data Warehouse creado (`DWH/init/01-init.sql` ejecutado)
- [ ] Dependencias instaladas (`pip install -r requirements.txt`)
- [ ] ETL ejecutado exitosamente (`python run_etl.py`)
- [ ] Datos cargados en DWH (verificados en SSMS)

## 🎯 Próximos Pasos

1. ✅ **ETL MSSQL** (Completado - Este documento)
2. 🔄 **ETL MySQL** (Próxima semana)
3. 🔄 **ETL MongoDB** (Próxima semana)
4. 🔄 **ETL Supabase** (Próxima semana)
5. 🔄 **ETL Neo4j** (Próxima semana)
6. 📊 **Dashboard Power BI** (Semana 15)
7. 🤖 **Análisis Apriori** (Semana 16)

---

**Última actualización**: 12 de Noviembre de 2024
**Versión**: 1.0.0
**Estado**: ✅ Funcional
