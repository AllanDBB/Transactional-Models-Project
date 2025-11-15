# Módulo BCCR - Integración con Banco Central de Costa Rica

Módulo compartido para todos los equipos del proyecto ETL multi-fuente.

## 📦 Uso desde cualquier ETL

```python
# Desde MSSQL/etl/
import sys
sys.path.insert(0, '../../BCCR/src')
from bccr_integration import BCCRIntegration, ExchangeRateService

# Desde MySQL/etl/
import sys
sys.path.insert(0, '../../BCCR/src')
from bccr_integration import BCCRIntegration, ExchangeRateService

# Desde MongoDB/etl/
import sys
sys.path.insert(0, '../../BCCR/src')
from bccr_integration import BCCRIntegration, ExchangeRateService
```

## 🔑 Configuración

- **Token BCCR**: `AVMGEIZILV`
- **Endpoint**: `https://gee.bccr.fi.cr/Indicadores/Suscripciones/API/API_Token/consultaPublica/`
- **Indicador**: 318 (USD compra/venta promedio)
- **IP Whitelist**: 186.176.142.42 (solicitar acceso a gee@bccr.fi.cr)

## 📊 Funcionalidades

### 1. Obtener histórico (3 años)
```python
bccr = BCCRIntegration()
df_historico = bccr.get_historical_rates(years_back=3)
# Retorna DataFrame con columnas: fecha, de_moneda, a_moneda, tasa, compra, venta
```

### 2. Obtener tasa del día
```python
bccr = BCCRIntegration()
df_hoy = bccr.get_latest_rates()
# Retorna DataFrame con tasa de hoy
```

### 3. Obtener período específico
```python
from datetime import datetime
bccr = BCCRIntegration()
df_periodo = bccr.get_exchange_rates_period(
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 12, 31),
    moneda_origen='CRC',
    moneda_destino='USD'
)
```

## 🎯 Servicio con Carga al DWH

```python
from bccr_integration import ExchangeRateService

# db_loader debe tener método load_staging_exchange_rates_dataframe()
service = ExchangeRateService(db_loader)

# Cargar histórico
service.load_historical_rates_to_dwh(years_back=3)

# Actualización diaria (para SQL Agent Job o cron)
service.update_daily_rates()
```

## 📋 Dependencias

```bash
pip install requests pandas
```

## 🔄 Conversión

- **CRC → USD**: Tasa típica ~515 colones por dólar
- **Actualización**: Diaria a las 5 AM (días laborales)
- **Mock Data**: Por defecto usa datos simulados (API real requiere IP en whitelist)

## 🚀 Activar API Real

1. Solicitar acceso: gee@bccr.fi.cr
2. Proveer IP: 186.176.142.42
3. Esperar aprobación (24-48 horas)
4. Modificar código en `bccr_integration.py` línea ~70

## 📞 Contacto

- Email BCCR: gee@bccr.fi.cr
- Website: https://gee.bccr.fi.cr
