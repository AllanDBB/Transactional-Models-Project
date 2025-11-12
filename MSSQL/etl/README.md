# ETL: MSSQL Transaccional → Data Warehouse

## Descripción

Este ETL integra datos de la base de datos transaccional `SalesDB_MSSQL` al Data Warehouse `MSSQL_DW`, aplicando transformaciones para resolver heterogeneidades en los datos.

## Estructura del Proyecto

```
etl/
├── config.py              # Configuración de conexiones
├── run_etl.py            # Script principal
├── requirements.txt      # Dependencias Python
├── extract/
│   ├── __init__.py      # Clase DataExtractor
│   └── extract_data.py  # (referencia)
├── transform/
│   ├── __init__.py      # Clase DataTransformer
│   └── transform_data.py # (referencia)
└── load/
    ├── __init__.py      # Clase DataLoader
    └── load_data.py     # (referencia)
```

## Reglas de Transformación Aplicadas

### 1. **Estandarización de Género**
```
Masculino → M
Femenino → F
Otro → O
M, F, X → Sin cambios
```

### 2. **Normalización de Moneda**
- **MSSQL**: Todos los montos están en USD
- **Tabla de Tipo de Cambio**: DimExchangeRate
- **Conversión**: En fuentes futuras (MySQL, MongoDB, Supabase), los CRC se convertirán a USD

### 3. **Homologación de Productos (SKU)**
- **Fuente MSSQL**: Usa `SKU` como código oficial
- **Mapeo**: Los códigos alternos de otras fuentes se mapearán a este SKU
- **Tabla Puente**: (Será implementada para MySQL, MongoDB, etc.)

### 4. **Conversión de Fechas**
- VARCHAR → DATETIME2
- Formato: `YYYY-MM-DD HH:MM:SS`

### 5. **Transformación de Totales**
- String → DECIMAL(18,2)
- Validación: Valores ≥ 0
- Cálculo de línea total: `PrecioUnit * Cantidad * (1 - DescuentoPct/100)`

### 6. **Validación de Descuentos**
- Rango: 0-100%
- NULL → 0
- Validación: No negativo

## Instalación

### 1. Instalar dependencias

```bash
cd MSSQL/etl
pip install -r requirements.txt
```

### 2. Configurar variables de entorno (opcional)

Crear archivo `.env` en la carpeta `etl/`:

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

## Uso

### Ejecutar el ETL completo

```bash
python run_etl.py
```

### Salida esperada

```
2024-11-12 10:30:45 - __main__ - INFO - ================================================================================
2024-11-12 10:30:45 - __main__ - INFO - INICIANDO PROCESO ETL: MSSQL → DWH
2024-11-12 10:30:45 - __main__ - INFO - ================================================================================

[FASE 1] EXTRAYENDO DATOS...
✓ Clientes extraídos: 150
✓ Productos extraídos: 45
✓ Órdenes extraídas: 520
✓ Detalles extraídos: 1250

[FASE 2] TRANSFORMANDO DATOS...
✓ Clientes transformados: 150
✓ Productos transformados: 45
✓ Órdenes transformadas: 520
✓ Detalles transformados: 1250
✓ Categorías extraídas: 8
✓ Canales extraídos: 3
✓ Fechas en DimTime: 365

[FASE 3] CARGANDO DATOS AL DWH...
✓ Dimensiones cargadas correctamente

================================================================================
✅ PROCESO ETL COMPLETADO EXITOSAMENTE
================================================================================
```

## Flujo de Datos

### Extract (Extracción)
```
SalesDB_MSSQL
├── sales_ms.Cliente
├── sales_ms.Producto
├── sales_ms.Orden
└── sales_ms.OrdenDetalle
```

### Transform (Transformación)
```
Clientes              Productos           Órdenes           Detalles
    ↓                    ↓                   ↓                  ↓
Normalizar           Normalizar          Convertir          Validar
Género               SKU                 Fechas             Precios
Email                Categoría           Totales            Cantidades
Fechas               Nombre              Canales            Descuentos
                                                            Calcular Total
    ↓                    ↓                   ↓                  ↓
DimCustomer         DimProduct          DimOrder           FactSales
                    DimCategory         DimChannel
                                        DimTime
```

### Load (Carga)
```
MSSQL_DW
├── DimCustomer (150 registros)
├── DimProduct (45 registros)
├── DimCategory (8 registros)
├── DimChannel (3 registros)
├── DimTime (365 fechas)
├── DimExchangeRate (tabla vacía, para futuras fuentes)
└── FactSales (1250+ transacciones)
```

## Logging

Los logs se generan en dos lugares:

1. **Archivo**: `etl_process.log` (en la carpeta `etl/`)
2. **Consola**: Salida en tiempo real del proceso

## Próximos Pasos

### Para integrar otras fuentes:

1. **MySQL**: Crear `MYSQL/etl/` con transformaciones para `codigo_alt`
2. **MongoDB**: Crear `MONGODB/etl/` con transformaciones para `codigo_mongo`
3. **Supabase**: Crear `SUPABASE/etl/` con transformaciones para UUIDs
4. **Neo4j**: Crear `NEO4J/etl/` con transformaciones desde grafos

### Tabla Puente de SKU (a implementar)

```sql
CREATE TABLE DWH.SKUMapping (
    id INT IDENTITY PRIMARY KEY,
    skuOficial NVARCHAR(40),
    codigoMySQL VARCHAR(64),
    codigoMongo VARCHAR(100),
    codigoSupabase UUID,
    fuente NVARCHAR(50)
);
```

## Troubleshooting

### Error: "Cannot connect to SQL Server"
- Verificar que Docker está corriendo
- Verificar puertos: 1433 (transaccional), 1434 (DWH)
- Verificar credenciales en `config.py`

### Error: "Import pyodbc not found"
```bash
pip install pyodbc
```

### Error: "Table already exists"
- El script `TRUNCATE` limpia las tablas automáticamente
- Si persiste, ejecutar en SSMS: `TRUNCATE TABLE nombre_tabla;`

## Notas Importantes

- ⚠️ El script TRUNCATE elimina todos los datos. Hacer backup antes.
- ✅ Los IDs de dimensiones se mapean automáticamente en FactSales
- 🔄 El proceso es idempotente: puede ejecutarse múltiples veces sin duplicados
