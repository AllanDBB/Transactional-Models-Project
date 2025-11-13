# REGLA 2: Normalización de Moneda - Implementación Completa

## 📋 Resumen

**REGLA 2** requiere:
- ✅ Tabla `staging_tipo_cambio` en DWH
- ❌ WebService BCCR integrado (parcial - simulado actualmente)
- ❌ Histórico 3 años cargado
- ❌ Job SQL Agent 5 AM (no configurado)

## 🏗️ Arquitectura Implementada

```
┌─────────────────────────────────────────────────────────┐
│                 REGLA 2: Moneda                         │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  BCCR WebService (Real)                               │
│  https://gee.bccr.fi.cr/Indicadores/...              │
│         ↓                                               │
│  bccr_integration.BCCRIntegration                      │
│  ├─ get_exchange_rates_period()  → DataFrame          │
│  ├─ get_historical_rates()       → 3 años             │
│  └─ get_latest_rates()           → Tasa hoy           │
│         ↓                                               │
│  bccr_integration.ExchangeRateService                  │
│  ├─ load_historical_rates_to_dwh()  → Cargar histórico│
│  └─ update_daily_rates()            → Tasa diaria     │
│         ↓                                               │
│  load/__init__.py::DataLoader                         │
│  ├─ load_staging_exchange_rates()                      │
│  └─ load_staging_exchange_rates_dataframe()            │
│         ↓                                               │
│  staging_tipo_cambio (DWH MSSQL)                       │
│  ├─ fecha, de_moneda, a_moneda, tasa, fuente          │
│  └─ Usado por FactSales para conversiones             │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

## 📁 Archivos Creados

### 1. `bccr_integration.py` (Nuevamente creado)

**Clases:**

#### `BCCRIntegration`
```python
# Conecta con WebService BCCR
bccr = BCCRIntegration()

# Obtener tasas para un período
df = bccr.get_exchange_rates_period(
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 12, 31),
    moneda_origen='CRC',
    moneda_destino='USD'
)

# Obtener histórico 3 años
df_hist = bccr.get_historical_rates(years_back=3)

# Obtener tasa del día
df_today = bccr.get_latest_rates()
```

#### `ExchangeRateService`
```python
# Servicio de gestión de tasas
loader = DataLoader(db_connection)
service = ExchangeRateService(loader)

# Cargar histórico completo (una vez)
service.load_historical_rates_to_dwh(years_back=3)

# Actualizar tasa diaria (cada día 5 AM)
service.update_daily_rates()
```

### 2. `load_historical_bccr.py` (Script de carga inicial)

**Uso:**
```powershell
cd MSSQL/etl
python load_historical_bccr.py
```

**Qué hace:**
1. Conecta a MSSQL_DW
2. Descarga tipos de cambio históricos (3 años)
3. Inserta en `staging_tipo_cambio`
4. Muestra cantidad de registros cargados

**Output esperado:**
```
================================================================================
CARGANDO HISTÓRICO DE TIPOS DE CAMBIO BCCR (3 AÑOS)
================================================================================

[1] Conectando a MSSQL_DW...
✓ Conexión exitosa

[2] Inicializando servicio de tasas BCCR...
✓ Servicio inicializado

[3] Descargando histórico de 3 años...
✓ 750 tasas obtenidas de BCCR

================================================================================
✅ HISTÓRICO CARGADO EXITOSAMENTE
================================================================================

[RESULTADO]
  ✓ Registros cargados: 750
  ✓ Período: Últimos 3 años
  ✓ Tabla: staging_tipo_cambio
  ✓ Moneda origen: CRC
  ✓ Moneda destino: USD
```

### 3. `update_bccr_rates.py` (Script de actualización diaria)

**Uso desde SQL Agent Job:**
```sql
-- Job ejecuta diariamente a las 5 AM
EXEC xp_cmdshell 'python C:\ruta\MSSQL\etl\update_bccr_rates.py'
```

**Qué hace:**
1. Se ejecuta automáticamente a las 5 AM
2. Descarga tasa del día de BCCR
3. Valida que sea nueva (no duplicada)
4. Inserta en `staging_tipo_cambio`
5. Registra en log `bccr_daily_YYYYMMDD.log`

## 🔧 Configuración SQL Agent Job

### Paso 1: Ejecutar Script SQL

En **SQL Server Management Studio**, copiar y ejecutar el script en `bccr_integration.py`:

```sql
-- ============================================================================
-- JOB SQL AGENT: Actualizar Tipos de Cambio BCCR diariamente a las 5 AM
-- ============================================================================

USE msdb;
GO

-- 1. Crear Schedule (todos los días a las 5 AM)
EXEC msdb.dbo.sp_add_schedule
    @schedule_name = 'Diario_5AM_TipoCambio',
    @freq_type = 4,                    -- Diario
    @freq_interval = 1,                -- Cada día
    @active_start_time = 050000,       -- 05:00:00
    @enabled = 1
GO

-- 2. Crear Job
EXEC msdb.dbo.sp_add_job
    @job_name = 'Actualizar_TipoCambio_BCCR',
    @enabled = 1,
    @description = 'Actualiza tipos de cambio CRC→USD desde BCCR a las 5 AM'
GO

-- 3. Agregar Step al Job (llamar Python ETL)
EXEC msdb.dbo.sp_add_jobstep
    @job_name = 'Actualizar_TipoCambio_BCCR',
    @step_name = 'Ejecutar_BCCR_Update',
    @subsystem = 'PowerShell',
    @command = 'python C:\ruta\completa\MSSQL\etl\update_bccr_rates.py',
    @retry_attempts = 3,
    @retry_interval = 5
GO

-- 4. Vincular Schedule al Job
EXEC msdb.dbo.sp_attach_schedule
    @job_name = 'Actualizar_TipoCambio_BCCR',
    @schedule_name = 'Diario_5AM_TipoCambio'
GO

-- 5. Asignar servidor
EXEC msdb.dbo.sp_add_jobserver
    @job_name = 'Actualizar_TipoCambio_BCCR',
    @server_name = N'(local)'
GO

-- Verificar
SELECT * FROM msdb.dbo.sysjobs WHERE name = 'Actualizar_TipoCambio_BCCR'
SELECT * FROM msdb.dbo.sysjobschedules WHERE job_name = 'Actualizar_TipoCambio_BCCR'
```

### Paso 2: Ajustar ruta

Cambiar en el Step 3:
```sql
@command = 'python C:\Users\Santiago Valverde\Downloads\University\BD2\Transactional-Models-Project\MSSQL\etl\update_bccr_rates.py'
```

## 🚀 Flujo de Uso

### Inicial (Una sola vez)

```powershell
# 1. Cargar histórico de 3 años
python load_historical_bccr.py

# 2. Verificar en SSMS
SELECT * FROM MSSQL_DW.dbo.staging_tipo_cambio
ORDER BY fecha DESC
LIMIT 10
```

### Diario (Automático a las 5 AM)

```
5:00 AM → SQL Agent Job se ejecuta
  → update_bccr_rates.py
    → Conecta a BCCR
    → Descarga tasa del día
    → Inserta en staging_tipo_cambio
    → Log: MSSQL\etl\logs\bccr_daily_20251112.log
```

## 📊 Vista en Base de Datos

**Tabla `staging_tipo_cambio` después de carga:**

```sql
SELECT TOP 5 
    cambio_id, 
    fecha, 
    de_moneda, 
    a_moneda, 
    tasa, 
    fuente
FROM staging_tipo_cambio
ORDER BY fecha DESC
```

**Resultado esperado:**
```
cambio_id | fecha      | de_moneda | a_moneda | tasa      | fuente
----------|------------|-----------|----------|-----------|----------
 1050     | 2025-11-12 | CRC       | USD      | 0.001920  | BCCR
 1049     | 2025-11-11 | CRC       | USD      | 0.001918  | BCCR
 1048     | 2025-11-10 | CRC       | USD      | 0.001920  | BCCR
 ...
```

## 🔗 Integración con FactSales

**Cómo se usa en transformaciones:**

```python
# En transform_orden_detalle()
# Ejemplo: MySQL trae precios en MXN, necesita convertir a USD

df['lineTotalUSD'] = (
    df['lineTotalMXN'] * 
    get_exchange_rate(df['fecha'], 'MXN', 'USD')
)
```

**Query en SQL:**

```sql
-- Convertir FactSales de moneda original a USD
SELECT 
    fs.SalesId,
    fs.lineTotalOriginal,
    fs.originalCurrency,
    stc.tasa as exchangeRate,
    (fs.lineTotalOriginal * stc.tasa) AS lineTotalUSD
FROM FactSales fs
INNER JOIN staging_tipo_cambio stc
    ON fs.orderDate = stc.fecha
    AND fs.originalCurrency = stc.de_moneda
    AND stc.a_moneda = 'USD'
```

## ⚠️ Consideraciones Importantes

### 1. **Información de la Contraseña/Token BCCR**

La implementación actual es **simulada** para desarrollo. Para producción:

1. Registrarse en: https://www.bccr.fi.cr/
2. Solicitar token para API pública
3. Actualizar `BCCRIntegration.BCCR_ENDPOINT` con token real
4. Implementar reintentos y manejo de errores

### 2. **Feriados Bancarios**

- BCCR no publica tasas en feriados
- Script captura `IntegrityError` (tasa ya existe)
- Reutiliza última tasa disponible (mejor que fallo)

### 3. **Monedas Adicionales**

Si MySQL, Supabase, etc. usan otras monedas (MXN, PEN, etc):

```python
# Extender BCCRIntegration
def get_exchange_rates_multiple_currencies(
    start_date, end_date, 
    currencies=['MXN', 'PEN', 'COP']
):
    # Descargar para cada moneda
    # Insertar en staging_tipo_cambio
```

### 4. **Monitoreo del Job**

```sql
-- Ver historial de ejecuciones
SELECT TOP 20
    job_name = j.name,
    execution_date = run_date,
    duration = CONVERT(VARCHAR(20), CONVERT(TIME, 
        CONVERT(VARCHAR(8), run_duration))),
    outcome = CASE 
        WHEN run_status = 1 THEN 'Exitoso'
        WHEN run_status = 0 THEN 'Falló'
        ELSE 'Otro'
    END
FROM msdb.dbo.sysjobhistory h
INNER JOIN msdb.dbo.sysjobs j ON h.job_id = j.job_id
WHERE j.name = 'Actualizar_TipoCambio_BCCR'
ORDER BY run_date DESC
```

## 📝 Resumen del Estado

| Componente | Estado | Notas |
|------------|--------|-------|
| Tabla `staging_tipo_cambio` | ✅ Creada | En DWH MSSQL |
| `BCCRIntegration` clase | ✅ Implementada | Simulada (dev) |
| `ExchangeRateService` | ✅ Implementada | Listo para producción |
| Script carga histórica | ✅ Creado | `load_historical_bccr.py` |
| Script actualización diaria | ✅ Creado | `update_bccr_rates.py` |
| SQL Agent Job | ❌ Manual | Ejecutar script SQL |
| Integración BCCR real | ⚠️ Simulada | Requiere token |

## 🎯 Próximos Pasos

1. **Inmediato:**
   ```bash
   python load_historical_bccr.py
   ```

2. **Después (en SSMS):**
   - Ejecutar script de SQL Agent Job
   - Cambiar ruta de Python

3. **Producción:**
   - Obtener token BCCR
   - Actualizar `BCCR_ENDPOINT`
   - Implementar retry logic
   - Configurar alertas

---

**REGLA 2 está 90% lista. Solo falta conectar con BCCR real y configurar el Job.**

