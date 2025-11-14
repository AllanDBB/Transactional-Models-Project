"""
Script para limpiar y recargar tabla de tipos de cambio históricos
"""
from config import DatabaseConfig
import pyodbc
from bccr_integration import ExchangeRateService
from load import DataLoader

conn = pyodbc.connect(DatabaseConfig.get_dw_connection_string())
cursor = conn.cursor()

# Truncar tabla para limpiar
print('[*] Truncando staging_tipo_cambio...')
cursor.execute('TRUNCATE TABLE staging_tipo_cambio')
conn.commit()
print('[OK] Tabla truncada')

cursor.close()
conn.close()

# Recargar histórico
print('[*] Cargando histórico de 3 años...')
loader = DataLoader(DatabaseConfig.get_dw_connection_string())
service = ExchangeRateService(loader)

inserted = service.load_historical_rates_to_dwh(3)
print(f'[OK] {inserted} registros históricos cargados')
