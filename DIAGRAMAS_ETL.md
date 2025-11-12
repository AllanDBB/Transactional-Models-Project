# DIAGRAMAS: ETL MSSQL → Data Warehouse

## 1. FLUJO GENERAL DEL ETL

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                      PROCESO ETL: MSSQL TRANSACCIONAL → DWH                  │
└──────────────────────────────────────────────────────────────────────────────┘

                              FASE 1: EXTRACT
                                    │
                    ┌───────────────┼───────────────┐
                    │               │               │
            ┌───────▼────────┐ ┌───▼────────┐ ┌──▼──────────┐
            │ sales_ms.      │ │ sales_ms.  │ │ sales_ms.   │
            │ Cliente        │ │ Producto   │ │ Orden       │
            │                │ │            │ │ +           │
            │ 5 registros    │ │ 5 regs     │ │ OrdenDetalle│
            │                │ │            │ │             │
            └───────┬────────┘ └───┬────────┘ │ 5+10 regs   │
                    │               │         │             │
                    └───────────────┼─────────┴─────────────┘
                                    │
                    ┌───────────────▼────────────────┐
                    │   FASE 2: TRANSFORM            │
                    │   (DataTransformer)            │
                    └────────────────────────────────┘
                                    │
        ┌──────────────────────────┼──────────────────────────┐
        │                          │                          │
    ┌───▼──────────┐      ┌────────▼─────────┐      ┌────────▼─────────┐
    │ Normalizar   │      │ Normalizar       │      │ Normalizar       │
    │ Clientes:    │      │ Productos:       │      │ Órdenes:         │
    │              │      │                  │      │                  │
    │ • Género:    │      │ • SKU uppercase  │      │ • Fechas VARCHAR │
    │   M/F/Osc→   │      │ • Nombres trim   │      │ • Totales        │
    │   M/F/O      │      │ • Categoría      │      │ • Moneda USD ✓   │
    │              │      │   normalizada    │      │ • Canales        │
    │ • Email:     │      │                  │      │                  │
    │   lowercase  │      │ RESULTADO:       │      │ RESULTADO:       │
    │              │      │ 5 productos      │      │ 5 órdenes        │
    │ • Fechas:    │      │ 2 categorías     │      │ 3 canales        │
    │   DATE       │      │                  │      │ 5 fechas únicas  │
    │              │      │                  │      │                  │
    │ RESULTADO:   │      │                  │      │                  │
    │ 5 clientes   │      │                  │      │                  │
    │ (normalizados)│     │                  │      │                  │
    └───┬──────────┘      └────────┬─────────┘      └────────┬─────────┘
        │                          │                          │
        └──────────────────────────┼──────────────────────────┘
                                    │
                    ┌───────────────▼────────────────┐
                    │   FASE 3: LOAD                 │
                    │   (DataLoader → MSSQL_DW)      │
                    └────────────────────────────────┘
                                    │
        ┌──────────────────────────┼──────────────────────────┐
        │                          │                          │
    ┌───▼────────┐        ┌────────▼────────┐       ┌────────▼─────┐
    │ DimCustomer│        │ DimProduct      │       │ DimCategory  │
    │            │        │ DimCategory     │       │ DimChannel   │
    │ 5 registros│        │ DimChannel      │       │ DimTime      │
    │            │        │ DimTime         │       │              │
    │ READY ✓    │        │ DimExchangeRate │       │ READY ✓      │
    └────────────┘        │ DimOrder        │       └──────────────┘
                          │                 │
                          │ READY ✓         │
                          └─────────────────┘
                                    │
                        ┌───────────▼────────────┐
                        │   FactSales            │
                        │   (Ready para carga)   │
                        │                        │
                        │   Mapeo de IDs:        │
                        │   • customerId → ✓     │
                        │   • productId → ✓      │
                        │   • channelId → ✓      │
                        │   • timeId → ✓         │
                        │                        │
                        │   READY ✓              │
                        └────────────────────────┘
```

## 2. TRANSFORMACIONES POR TABLA

### Cliente: Masculino/Femenino → M/F

```
┌────────────────────────────────────────┐
│ sales_ms.Cliente (FUENTE)              │
├────────────────────────────────────────┤
│ ClienteId  Nombre          Genero      │
│ 1          Juan Pérez      Masculino   │
│ 2          María García    Femenino    │
│ 3          Carlos López    Masculino   │
└────────────────────────────────────────┘
                  │
              TRANSFORM
                  │
                  ▼
┌────────────────────────────────────────┐
│ DimCustomer (DWH)                      │
├────────────────────────────────────────┤
│ id  name           email                gender│
│ 1   Juan Pérez     juan.perez@...      M     │
│ 2   María García   maria.garcia@...    F     │
│ 3   Carlos López   carlos.lopez@...    M     │
└────────────────────────────────────────┘
```

### Producto: Normalizar SKU

```
┌─────────────────────────────────────────┐
│ sales_ms.Producto (FUENTE)              │
├─────────────────────────────────────────┤
│ ProductoId  SKU        Nombre           │
│ 1           sku-001    Laptop           │
│ 2           SKU-002    Mouse            │
│ 3           sKu-003    Teclado          │
└─────────────────────────────────────────┘
                  │
              TRANSFORM
            (uppercase + trim)
                  │
                  ▼
┌──────────────────────────────────────────┐
│ DimProduct (DWH)                         │
├──────────────────────────────────────────┤
│ id  name      code        categoryId     │
│ 1   Laptop    SKU-001     1 (Electrónica)│
│ 2   Mouse     SKU-002     2 (Accesorios) │
│ 3   Teclado   SKU-003     2 (Accesorios) │
└──────────────────────────────────────────┘
```

### Orden: Validar Totales

```
┌─────────────────────────────────────────┐
│ sales_ms.Orden (FUENTE)                 │
├─────────────────────────────────────────┤
│ OrdenId  Fecha           Total Moneda   │
│ 1        2024-03-15      1500.00 USD   │
│ 2        2024-03-16      -450.00 USD   │ ← INVÁLIDO (negativo)
│ 3        2024-03-17      250.00 USD    │
└─────────────────────────────────────────┘
                  │
              TRANSFORM
         (validar Total >= 0)
                  │
                  ▼
┌──────────────────────────────────────────┐
│ DimOrder (DWH)                           │
├──────────────────────────────────────────┤
│ id  totalOrderUSD                        │
│ 1   1500.00                              │
│ 2   [DESCARTADO]                         │
│ 3   250.00                               │
└──────────────────────────────────────────┘
```

### OrdenDetalle: Calcular Línea Total

```
┌──────────────────────────────────────────────────┐
│ sales_ms.OrdenDetalle (FUENTE)                   │
├──────────────────────────────────────────────────┤
│ OrdenId ProductoId Cantidad PrecioUnit Descto%   │
│ 1       1          1        1200.00   5.0       │
│ 1       2          1        80.00     0         │
│ 2       3          1        150.00    10.0      │
└──────────────────────────────────────────────────┘
                  │
              TRANSFORM
    lineTotalUSD = PrecioUnit * Cantidad * (1 - Descto%/100)
                  │
                  ▼
┌──────────────────────────────────────────────────┐
│ FactSales (DWH) - Línea de Venta                │
├──────────────────────────────────────────────────┤
│ productCant productUnitPriceUSD discountPercentage │ lineTotalUSD
│ 1           1200.00             5.0             │ 1140.00
│ 1           80.00               0               │ 80.00
│ 1           150.00              10.0            │ 135.00
└──────────────────────────────────────────────────┘
```

## 3. SCHEMA ESTRELLA DEL DWH

```
                              DimTime
                                │
                    ┌───────────┼───────────┐
                    │           │           │
                    │           │           │
              DimChannel    FactSales    DimOrder
                    │           │           │
                    │           │           │
                    │           │           │
            DimExchangeRate     │         (vacía)
                    │           │
                    │           │
        ┌───────────┼───────────┴──────────┐
        │           │                      │
    DimCustomer DimProduct          DimCategory
        │           │
        │           │
    (5 regs)    (5 regs + 2 categorías)
    
Forma: ⭐ (STAR SCHEMA)
Centro: FactSales
Puntas: Dimensiones
```

## 4. MAPEO DE IDS EN FACTSALES

```
SalesDB_MSSQL                          MSSQL_DW
(IDs Originales)                       (IDs Normalizados)
        │                                      │
        │                                      │
        │  ClienteId=1                         │  
        ├──────────────────────────────────────┤──→ DimCustomer.id=1
        │  ProductoId=1                        │
        ├──────────────────────────────────────┤──→ DimProduct.id=1
        │  OrdenId=1                           │
        ├──────────────────────────────────────┤──→ DimOrder.id=1
        │  Fecha=2024-03-15                    │
        ├──────────────────────────────────────┤──→ DimTime.id=1
        │  Canal=WEB                           │
        └──────────────────────────────────────┘──→ DimChannel.id=1
                                                    
                        ↓ (Resultado)
                        
            FactSales
            ─────────────────────────────
            customerId    = 1
            productId     = 1
            orderId       = 1
            timeId        = 1
            channelId     = 1
            lineTotalUSD  = 1140.00
```

## 5. TIMELINE DEL PROCESO

```
INICIO
  │
  ▼
┌──────────────────────────────────────┐
│ 1. CONECTAR A SalesDB_MSSQL          │ (1-2 seg)
│    ├─ Verificar conexión ODBC        │
│    └─ Autenticar con SA              │
└──────────────────┬───────────────────┘
                   │
                   ▼
┌──────────────────────────────────────┐
│ 2. EXTRAER DATOS                     │ (2-3 seg)
│    ├─ Query: SELECT from Cliente     │
│    ├─ Query: SELECT from Producto    │
│    ├─ Query: SELECT from Orden       │
│    └─ Query: SELECT from OrdenDetalle│
└──────────────────┬───────────────────┘
                   │
                   ▼
┌──────────────────────────────────────┐
│ 3. TRANSFORMAR (IN-MEMORY)           │ (1-2 seg)
│    ├─ Normalizar 5 clientes          │
│    ├─ Normalizar 5 productos         │
│    ├─ Normalizar 5 órdenes           │
│    ├─ Normalizar 10 detalles         │
│    └─ Generar dimensiones            │
└──────────────────┬───────────────────┘
                   │
                   ▼
┌──────────────────────────────────────┐
│ 4. CONECTAR A MSSQL_DW               │ (1-2 seg)
│    ├─ Verificar conexión ODBC        │
│    └─ Autenticar con ADMIN           │
└──────────────────┬───────────────────┘
                   │
                   ▼
┌──────────────────────────────────────┐
│ 5. TRUNCATE TABLES                   │ (1 seg)
│    ├─ TRUNCATE DimCategory           │
│    ├─ TRUNCATE DimChannel            │
│    ├─ TRUNCATE DimCustomer           │
│    ├─ TRUNCATE DimTime               │
│    ├─ TRUNCATE DimProduct            │
│    └─ TRUNCATE FactSales             │
└──────────────────┬───────────────────┘
                   │
                   ▼
┌──────────────────────────────────────┐
│ 6. CARGAR DIMENSIONES                │ (2-3 seg)
│    ├─ INSERT DimCategory (2 regs)    │
│    ├─ INSERT DimChannel (3 regs)     │
│    ├─ INSERT DimCustomer (5 regs)    │
│    ├─ INSERT DimTime (5 regs)        │
│    ├─ INSERT DimProduct (5 regs)     │
│    └─ INSERT DimOrder (5 regs)       │
└──────────────────┬───────────────────┘
                   │
                   ▼
            ✅ COMPLETADO
            
TIEMPO TOTAL: ~10-15 segundos
```

## 6. VALIDACIONES EN CASCADA

```
Entrada → Validación 1 → Validación 2 → Validación 3 → Salida
   │          │             │              │
   │          │             │              │
   │      Género            Email       Fecha       ✓ OK
   │     válido?            válido?     válida?
   │      ✓                 ✓           ✓
   │                                                ↓
Precio  │    Precio >= 0   │  Desc% 0-100  │  Calcular │  lineTotalUSD
        │     ✓            │     ✓         │    total  │  = P*C*(1-D%)
        │                  │               │           │
        └──────────────────┴───────────────┴───────────┴──→ ✓
```

## 7. DEPENDENCIAS ENTRE MÓDULOS

```
run_etl.py
    │
    ├─→ config.py
    │       ├─ DatabaseConfig.get_source_connection_string()
    │       └─ DatabaseConfig.get_dw_connection_string()
    │
    ├─→ extract/__init__.py (DataExtractor)
    │       ├─ extract_clientes()
    │       ├─ extract_productos()
    │       ├─ extract_ordenes()
    │       └─ extract_orden_detalle()
    │
    ├─→ transform/__init__.py (DataTransformer)
    │       ├─ transform_clientes()
    │       ├─ transform_productos()
    │       ├─ transform_ordenes()
    │       ├─ transform_orden_detalle()
    │       ├─ extract_categorias()
    │       ├─ extract_canales()
    │       └─ generate_dimtime()
    │
    └─→ load/__init__.py (DataLoader)
            ├─ truncate_tables()
            ├─ load_dim_category()
            ├─ load_dim_channel()
            ├─ load_dim_customer()
            ├─ load_dim_time()
            ├─ load_dim_product()
            └─ load_fact_sales()

    Dependencias Python:
    ├─ pyodbc >= 5.0.0       (Conexiones SQL Server)
    ├─ pandas >= 2.1.0       (DataFrames)
    ├─ numpy >= 1.24.0       (Arrays)
    ├─ python-dotenv >= 1.0  (Variables de entorno)
    └─ sqlalchemy >= 2.0.0   (ORM - opcional)
```

---

**Versión**: 1.0.0  
**Última actualización**: 12 de Noviembre de 2024
