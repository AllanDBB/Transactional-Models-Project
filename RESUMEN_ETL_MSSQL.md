# RESUMEN: ETL MSSQL → Data Warehouse

## 📦 Archivos Creados

```
✅ MSSQL/etl/
├── config.py                 → Configuración de conexiones BD
├── run_etl.py               → Script principal (ejecutar)
├── id_mapper.py             → Mapeo de IDs (auxiliar)
├── requirements.txt         → Dependencias Python
├── README.md                → Documentación técnica
├── extract/__init__.py      → Clase DataExtractor
├── transform/__init__.py    → Clase DataTransformer (✨ Todas las reglas)
└── load/__init__.py         → Clase DataLoader

✅ MSSQL/init/
└── 02-sample-data.sql       → Datos de prueba (5 clientes, 5 productos, 5 órdenes)

✅ Raíz del proyecto/
└── GUIA_ETL_MSSQL.md        → Esta guía (paso a paso)
```

## 🎯 Reglas de Transformación Implementadas

### 1. **Estandarización de Género** ✅
```
Entrada            | Salida
Masculino          → M
Femenino           → F
Otro               → O
```

### 2. **Homologación de Productos (SKU)** ✅
```
Entrada (SKU_oficial)  | Salida (DimProduct.code)
SKU-001                → SKU-001 (normalizado)
SKU-002                → SKU-002
...
```

### 3. **Normalización de Moneda** ✅
```
MSSQL: Siempre USD → No requiere conversión
Tabla de cambio: DimExchangeRate (lista para CRC)
```

### 4. **Conversión de Fechas** ✅
```
DATETIME2 → DATE (en DimTime)
Ej: 2024-03-15 10:30:00 → 2024-03-15
```

### 5. **Transformación de Totales** ✅
```
DECIMAL(18,2) → DECIMAL(18,2)
Cálculo línea: PrecioUnit * Cantidad * (1 - Desc%)
Validación: >= 0
```

### 6. **Validación de Descuentos** ✅
```
NULL → 0
0-100 → Se mantiene
Rango válido: 0-100%
```

## 🔄 Flujo ETL Completo

```
┌─────────────────────────────────────────────────────────────────┐
│ FASE 1: EXTRACT - Extrae de SalesDB_MSSQL                       │
├─────────────────────────────────────────────────────────────────┤
│ • sales_ms.Cliente                                               │
│ • sales_ms.Producto                                              │
│ • sales_ms.Orden                                                 │
│ • sales_ms.OrdenDetalle                                          │
└──────────────────────┬──────────────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────────────┐
│ FASE 2: TRANSFORM - Aplica reglas de normalización              │
├─────────────────────────────────────────────────────────────────┤
│ DataTransformer.transform_clientes()                            │
│   ├─ Género: Masculino/Femenino → M/F                          │
│   ├─ Email: normalizar (lowercase, trim)                       │
│   └─ Fecha: DATETIME2 → DATE                                   │
│                                                                  │
│ DataTransformer.transform_productos()                          │
│   ├─ SKU: normalizar (uppercase)                               │
│   ├─ Nombre: trim                                              │
│   └─ Categoría: uppercase                                      │
│                                                                  │
│ DataTransformer.transform_ordenes()                            │
│   ├─ Fecha: VARCHAR → DATETIME2                                │
│   ├─ Total: validar >= 0                                       │
│   └─ Canal: normalizar                                         │
│                                                                  │
│ DataTransformer.transform_orden_detalle()                      │
│   ├─ Precio: DECIMAL ✓                                         │
│   ├─ Cantidad: validar > 0                                     │
│   ├─ Descuento: limitar 0-100%                                 │
│   └─ Total línea: Precio*Cant*(1-Desc%)                        │
└──────────────────────┬──────────────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────────────┐
│ FASE 3: LOAD - Carga en MSSQL_DW                                │
├─────────────────────────────────────────────────────────────────┤
│ DataLoader.load_dim_category()        → DimCategory           │
│ DataLoader.load_dim_channel()         → DimChannel            │
│ DataLoader.load_dim_customer()        → DimCustomer           │
│ DataLoader.load_dim_time()            → DimTime               │
│ DataLoader.load_dim_product()         → DimProduct            │
│ DataLoader.load_fact_sales()          → FactSales             │
└─────────────────────────────────────────────────────────────────┘
```

## 🚀 Pasos de Uso Rápido

### 1️⃣ Iniciar Docker
```powershell
cd MSSQL
docker-compose up -d
```

### 2️⃣ Crear bases de datos
```
SSMS → Ejecutar: MSSQL/init/01-init.sql
SSMS → Ejecutar: MSSQL/init/02-sample-data.sql
SSMS → Ejecutar: DWH/init/01-init.sql
```

### 3️⃣ Instalar dependencias
```powershell
cd MSSQL/etl
pip install -r requirements.txt
```

### 4️⃣ Ejecutar ETL
```powershell
python run_etl.py
```

### 5️⃣ Verificar resultados
```sql
USE MSSQL_DW;
SELECT * FROM DimCustomer;
SELECT * FROM DimProduct;
SELECT * FROM DimCategory;
SELECT * FROM DimChannel;
SELECT * FROM DimTime;
```

## 📊 Estructura del Data Warehouse

```
MSSQL_DW
├── Dimensiones (DIM)
│   ├── DimCustomer         (Clientes normalizados)
│   ├── DimProduct          (Productos con SKU oficial)
│   ├── DimCategory         (Categorías únicas)
│   ├── DimChannel          (Canales: WEB, TIENDA, APP)
│   ├── DimTime             (Calendario con tasas cambio)
│   ├── DimOrder            (Total por orden)
│   └── DimExchangeRate     (Para conversiones CRC→USD)
│
└── Hechos (FACT)
    └── FactSales          (Transacciones: 
                            cliente × producto × canal × fecha)
```

## 🔗 Relaciones en FactSales

```
FactSales
├── customerId      → DimCustomer.id
├── productId       → DimProduct.id
├── channelId       → DimChannel.id
├── timeId          → DimTime.id
├── orderId         → DimOrder.id
└── exchangeRateId  → DimExchangeRate.id (NULL para USD)
```

## 📈 Ejemplo de Transformación

**Antes (SalesDB_MSSQL)**
```
ClienteId: 1
Nombre: Juan Pérez
Email: JUAN.PEREZ@EMAIL.COM
Genero: Masculino
Pais: Costa Rica
FechaRegistro: 2024-01-15 10:00:00

ProductoId: 1
SKU: SKU-001
Nombre: Laptop Dell XPS 13
Categoria: ELECTRÓNICA

OrdenId: 1
Fecha: 2024-03-15 10:30:00
Canal: WEB
Total: 1500.00
Moneda: USD

OrdenDetalleId: 1
Cantidad: 1
PrecioUnit: 1200.00
DescuentoPct: 5.0
```

**Después (MSSQL_DW)**
```
DimCustomer
├── id: 1
├── name: Juan Pérez
├── email: juan.perez@email.com    ← lowercase
├── gender: M                        ← M en lugar de Masculino
└── country: Costa Rica

DimProduct
├── id: 1
├── name: Laptop Dell XPS 13
├── code: SKU-001                   ← normalizado
└── categoryId: [mapped]

DimCategory
├── id: 1
└── name: ELECTRÓNICA              ← uppercase

DimChannel
├── id: 1
├── name: WEB
└── channelType: Website

DimTime
├── id: 1
├── date: 2024-03-15               ← solo fecha
├── year: 2024
├── month: 3
├── day: 15
└── exchangeRateToUSD: 1.0

FactSales
├── customerId: 1
├── productId: 1
├── channelId: 1
├── timeId: 1
├── productCant: 1
├── productUnitPriceUSD: 1200.00
├── discountPercentage: 5.0        ← validado 0-100
└── lineTotalUSD: 1140.00          ← 1200 * 1 * (1 - 5/100)
```

## 🎓 Conceptos Clave

### ¿Qué es un Schema Estrella?
```
         DimTime
           |
    DimChannel-+-FactSales-+-DimProduct
           |                  |
       DimOrder          DimCategory
           |
    DimCustomer
    
Las dimensiones rodean el hecho (FactSales)
```

### ¿Por qué normalizar?
- **Consistencia**: Todos los géneros son M/F/O
- **Integridad**: No hay valores NULL inesperados
- **Rendimiento**: Búsquedas más rápidas
- **Análisis**: Reportes correctos en Power BI

## 📝 Logging

Cada ejecución del ETL genera logs:

```
2024-11-12 10:45:30 - __main__ - INFO - ================================================================================
2024-11-12 10:45:30 - __main__ - INFO - INICIANDO PROCESO ETL: MSSQL → DWH
2024-11-12 10:45:31 - extract - INFO - Extrayendo clientes...
2024-11-12 10:45:31 - extract - INFO - Consulta exitosa: 5 registros
2024-11-12 10:45:32 - transform - INFO - Transformando 5 clientes...
2024-11-12 10:45:32 - transform - INFO - Clientes transformados: 5
2024-11-12 10:45:33 - load - INFO - Limpiando tablas: DimCategory, DimChannel, ...
2024-11-12 10:45:34 - load - INFO - Cargando 2 categorías...
2024-11-12 10:45:34 - load - INFO - 2 registros cargados en DimCategory
...
2024-11-12 10:45:40 - __main__ - INFO - ✅ PROCESO ETL COMPLETADO EXITOSAMENTE
```

Ver archivo: `MSSQL/etl/etl_process.log`

## ✅ Validaciones Implementadas

| Validación | Campo | Regla |
|------------|-------|-------|
| Género válido | DimCustomer.gender | M, F, O |
| Email único | DimCustomer.email | UNIQUE |
| Email válido | DimCustomer.email | Contiene @ |
| Precio >= 0 | FactSales.productUnitPriceUSD | >= 0 |
| Cantidad > 0 | FactSales.productCant | > 0 |
| Descuento 0-100% | FactSales.discountPercentage | BETWEEN 0 AND 100 |
| Fecha válida | DimTime.date | DATE |
| SKU único | DimProduct.code | UNIQUE |
| Total >= 0 | DimOrder.totalOrderUSD | >= 0 |

## 🔐 Seguridad

- Credenciales en `config.py` (puede usar variables de entorno)
- Conexiones ODBC driver 17
- Transacciones ACID en cargas
- Validación de datos antes de insertar

## 🚦 Estado del Proyecto

| Componente | Status | Detalle |
|-----------|--------|---------|
| ✅ MSSQL ETL | COMPLETADO | Extract, Transform, Load |
| ✅ Reglas de transformación | COMPLETADO | Género, Moneda, Fechas, Precios |
| ✅ Data Warehouse | COMPLETADO | Star Schema con 7 dimensiones |
| ✅ Datos de prueba | COMPLETADO | 5 clientes, 5 productos, 5 órdenes |
| 🔄 MySQL ETL | PENDIENTE | Próxima fase |
| 🔄 MongoDB ETL | PENDIENTE | Próxima fase |
| 🔄 Supabase ETL | PENDIENTE | Próxima fase |
| 🔄 Neo4j ETL | PENDIENTE | Próxima fase |
| 🔄 Power BI Dashboard | PENDIENTE | Semana 15 |
| 🔄 Análisis Apriori | PENDIENTE | Semana 16 |

---

**Versión**: 1.0.0  
**Fecha**: 12 de Noviembre de 2024  
**Autor**: ETL Framework MSSQL
