# 📋 Guía de Integración del Módulo BCCR

## 🎯 Propósito

El módulo BCCR está centralizado en la carpeta `/BCCR` para que **todos los equipos** (MSSQL, MySQL, MongoDB, Neo4j, Supabase) puedan usarlo sin duplicar código.

---

## 📂 Estructura

```
Transactional-Models-Project/
├── BCCR/                           ← MÓDULO COMPARTIDO
│   ├── src/
│   │   └── bccr_integration.py     ← Código principal
│   ├── ejemplo_uso.py              ← Demo de cómo usarlo
│   ├── README.md                   ← Documentación
│   └── requirements.txt            ← Dependencias
│
├── MSSQL/etl/                      ← ETL de Santiago (YA INTEGRADO)
├── MySQL/etl/                      ← ETL de compañero 1
├── MongoDB/etl/                    ← ETL de compañero 2
├── Neo4j/etl/                      ← ETL de compañero 3
└── Supabase/etl/                   ← ETL de compañero 4
```

---

## 🚀 Integración en tu ETL (3 pasos)

### PASO 1: Importar el módulo

Agrega al inicio de tu `run_etl.py` o archivo principal:

```python
import sys
from pathlib import Path

# Agregar módulo BCCR al path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'BCCR' / 'src'))

from bccr_integration import BCCRIntegration, ExchangeRateService
```

### PASO 2: Obtener tasas de cambio

```python
# Crear instancia
bccr = BCCRIntegration()

# Opción A: Tasa del día (para actualizaciones diarias)
df_hoy = bccr.get_latest_rates()

# Opción B: Histórico de 3 años (para carga inicial)
df_historico = bccr.get_historical_rates(years_back=3)

# Opción C: Período específico
from datetime import datetime
df_periodo = bccr.get_exchange_rates_period(
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 12, 31)
)
```

### PASO 3: Usar en tu transformación (REGLA 2)

```python
# Ejemplo: Convertir monto de CRC a USD
def convertir_crc_a_usd(monto_crc, fecha_orden, df_tasas):
    """
    Convierte CRC a USD usando tasa del día de la orden
    
    Args:
        monto_crc: Monto en colones
        fecha_orden: Fecha de la orden (datetime)
        df_tasas: DataFrame con tasas de BCCR
    
    Returns:
        Monto en USD
    """
    # Buscar tasa del día
    fecha_str = fecha_orden.strftime('%Y-%m-%d')
    tasa_row = df_tasas[df_tasas['fecha'] == fecha_str]
    
    if tasa_row.empty:
        # Usar tasa más cercana si no hay del día exacto
        tasa = df_tasas['tasa'].mean()
    else:
        tasa = tasa_row['tasa'].iloc[0]
    
    monto_usd = monto_crc / tasa
    return round(monto_usd, 2)
```

---

## 📊 Estructura del DataFrame retornado

```python
df.columns = ['fecha', 'de_moneda', 'a_moneda', 'tasa', 'compra', 'venta', 'fuente']

# Ejemplo de datos:
#    fecha       de_moneda  a_moneda   tasa    compra   venta   fuente
# 2024-01-15      CRC        USD     515.23   514.23   516.23  BCCR-MOCK
# 2024-01-16      CRC        USD     516.45   515.45   517.45  BCCR-MOCK
```

---

## 🗄️ Cargar a tu Staging Table

Si tu DWH tiene tabla `staging_tipo_cambio`:

```sql
CREATE TABLE staging_tipo_cambio (
    id INT IDENTITY PRIMARY KEY,
    fecha DATE NOT NULL,
    de_moneda CHAR(3) NOT NULL,
    a_moneda CHAR(3) NOT NULL,
    tasa DECIMAL(18,6) NOT NULL,
    compra DECIMAL(18,6),
    venta DECIMAL(18,6),
    UNIQUE (fecha, de_moneda, a_moneda)
);
```

Código Python para insertar:

```python
import pyodbc

# Obtener tasas
bccr = BCCRIntegration()
df_tasas = bccr.get_historical_rates(years_back=3)

# Conectar a tu DWH
conn = pyodbc.connect(tu_connection_string)
cursor = conn.cursor()

# Insertar tasas
for _, row in df_tasas.iterrows():
    try:
        cursor.execute("""
            INSERT INTO staging_tipo_cambio (fecha, de_moneda, a_moneda, tasa, compra, venta)
            VALUES (?, ?, ?, ?, ?, ?)
        """, row['fecha'], row['de_moneda'], row['a_moneda'], 
             row['tasa'], row['compra'], row['venta'])
    except pyodbc.IntegrityError:
        # Ya existe, ignorar
        pass

conn.commit()
cursor.close()
conn.close()
```

---

## 🔄 Actualización Diaria (Automatizada)

El proyecto incluye `update_daily.py` para actualizar automáticamente las tasas:

### Configuración Rápida:

1. **Editar `update_daily.py`** línea 50-70 para usar tu DataLoader
2. **Configurar automatización**:

#### Para SQL Server (SQL Agent Job):
```sql
-- Crear Job en SSMS
USE msdb;
GO

EXEC msdb.dbo.sp_add_job
    @job_name = 'Actualizar_TipoCambio_BCCR',
    @enabled = 1;
GO

EXEC msdb.dbo.sp_add_jobstep
    @job_name = 'Actualizar_TipoCambio_BCCR',
    @step_name = 'Ejecutar_Update',
    @subsystem = 'PowerShell',
    @command = 'cd C:\...\BCCR; python update_daily.py',
    @retry_attempts = 3;
GO

EXEC msdb.dbo.sp_add_schedule
    @schedule_name = 'Diario_5AM',
    @freq_type = 4,
    @active_start_time = 050000;
GO
```

#### Para Linux/Mac (cron):
```bash
# Editar crontab
crontab -e

# Agregar línea (ejecutar a las 5 AM)
0 5 * * * cd /ruta/BCCR && python update_daily.py
```

#### Para Windows (Task Scheduler):
1. Abrir "Programador de tareas"
2. Crear tarea básica
3. Desencadenador: Diariamente a las 5:00 AM
4. Acción: Iniciar programa `python.exe`
5. Argumentos: `C:\...\BCCR\update_daily.py`

---

## 🧪 Probar el módulo

Ejecuta el ejemplo:

```bash
cd BCCR
python ejemplo_uso.py
```

Deberías ver:

```
================================================================================
DEMO: Uso del módulo BCCR compartido
================================================================================

[EJEMPLO 1] Obtener tasa de hoy
--------------------------------------------------------------------------------
Tasa del día: 515.2345 CRC/USD
Fecha: 2025-11-15
...
```

---

## ❓ Preguntas Frecuentes

**P: ¿Tengo que instalar dependencias?**  
R: Sí, ejecuta: `pip install -r BCCR/requirements.txt`

**P: ¿El API de BCCR funciona?**  
R: Por ahora usa MOCK DATA. Para usar API real, solicitar acceso a gee@bccr.fi.cr con IP 186.176.142.42

**P: ¿Puedo modificar el código de BCCR?**  
R: Sí, pero coordina con el equipo para que todos usen la misma versión.

**P: ¿Qué hago si mi BD no usa SQL Server?**  
R: El módulo retorna un DataFrame de pandas. Puedes insertarlo en MySQL, MongoDB, Neo4j, etc. con la librería correspondiente.

---

## 📞 Contacto

**Responsable del módulo**: Santiago Valverde (MSSQL)  
**Ubicación**: `/BCCR`  
**Documentación**: `/BCCR/README.md`

---

## ✅ Checklist de Integración

- [ ] Importar módulo BCCR en tu ETL
- [ ] Instalar dependencias (`pip install -r BCCR/requirements.txt`)
- [ ] Probar con `ejemplo_uso.py`
- [ ] Crear tabla `staging_tipo_cambio` en tu DWH
- [ ] Cargar histórico de 3 años
- [ ] Implementar conversión CRC→USD en tu transformación (REGLA 2)
- [ ] (Opcional) Configurar actualización diaria

---

**¡Listo!** Ya puedes usar el módulo BCCR en tu ETL 🎉
