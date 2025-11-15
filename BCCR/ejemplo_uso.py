#!/usr/bin/env python3
"""
Ejemplo de uso del módulo BCCR desde cualquier ETL

Este script demuestra cómo MySQL, MongoDB, Neo4j o Supabase
pueden usar el módulo BCCR compartido
"""
import sys
from pathlib import Path

# Agregar módulo BCCR al path
# Ajustar según la ubicación de tu ETL:
# - Desde MSSQL/etl/ -> '../../BCCR/src'
# - Desde MySQL/etl/ -> '../../BCCR/src'
# - Desde MongoDB/etl/ -> '../../BCCR/src'
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from bccr_integration import BCCRIntegration, ExchangeRateService
from datetime import datetime

print("=" * 80)
print("DEMO: Uso del módulo BCCR compartido")
print("=" * 80)

# ============================================================================
# EJEMPLO 1: Obtener tasa del día
# ============================================================================
print("\n[EJEMPLO 1] Obtener tasa de hoy")
print("-" * 80)

bccr = BCCRIntegration()
df_hoy = bccr.get_latest_rates()

print(f"Tasa del día: {df_hoy['tasa'].iloc[0]} CRC/USD")
print(f"Fecha: {df_hoy['fecha'].iloc[0]}")
print(f"Compra: {df_hoy['compra'].iloc[0]}")
print(f"Venta: {df_hoy['venta'].iloc[0]}")

# ============================================================================
# EJEMPLO 2: Convertir montos de CRC a USD
# ============================================================================
print("\n[EJEMPLO 2] Convertir CRC a USD")
print("-" * 80)

monto_crc = 150000  # 150,000 colones
tasa = df_hoy['tasa'].iloc[0]
monto_usd = monto_crc / tasa

print(f"Monto en CRC: ₡{monto_crc:,.2f}")
print(f"Tasa de cambio: {tasa}")
print(f"Monto en USD: ${monto_usd:,.2f}")

# ============================================================================
# EJEMPLO 3: Obtener histórico de 1 año
# ============================================================================
print("\n[EJEMPLO 3] Obtener histórico de 1 año")
print("-" * 80)

df_historico = bccr.get_historical_rates(years_back=1)
print(f"Total de registros históricos: {len(df_historico)}")
print(f"Fecha mínima: {df_historico['fecha'].min()}")
print(f"Fecha máxima: {df_historico['fecha'].max()}")
print(f"Tasa promedio: {df_historico['tasa'].mean():.4f}")
print(f"Tasa mínima: {df_historico['tasa'].min():.4f}")
print(f"Tasa máxima: {df_historico['tasa'].max():.4f}")

# ============================================================================
# EJEMPLO 4: Obtener período específico
# ============================================================================
print("\n[EJEMPLO 4] Obtener período específico (Enero 2024)")
print("-" * 80)

df_enero = bccr.get_exchange_rates_period(
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 1, 31),
    moneda_origen='CRC',
    moneda_destino='USD'
)

print(f"Registros en Enero 2024: {len(df_enero)}")
print(f"Tasa promedio: {df_enero['tasa'].mean():.4f}")

# ============================================================================
# EJEMPLO 5: DataFrame para cargar en staging
# ============================================================================
print("\n[EJEMPLO 5] Formato de datos para staging_tipo_cambio")
print("-" * 80)

print("\nPrimeros 5 registros:")
print(df_hoy[['fecha', 'de_moneda', 'a_moneda', 'tasa', 'compra', 'venta']].head())

print("\nEstructura del DataFrame:")
print(df_hoy.dtypes)

print("\n" + "=" * 80)
print("FIN DEL DEMO")
print("=" * 80)
