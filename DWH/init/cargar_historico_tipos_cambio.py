#!/usr/bin/env python3
"""
Script para cargar el historial de tipos de cambio 3 años en DimExchangeRate
Obtiene datos reales del BCCR (Banco Central de Costa Rica)
Este será ejecutado una sola vez para popular la tabla
Luego, cada ETL consultará esta tabla para sus conversiones
"""

import pyodbc
import requests
from datetime import datetime, timedelta
from pathlib import Path
import sys
import logging

# Agregar el módulo de configuración
sys.path.insert(0, str(Path(__file__).parent.parent / 'MSSQL' / 'etl'))

try:
    from config import DatabaseConfig
except ImportError:
    print("⚠️  No se encontró config.py, usando valores hardcoded")
    class DatabaseConfig:
        @staticmethod
        def get_dw_connection_string():
            return "DRIVER={ODBC Driver 17 for SQL Server};SERVER=192.168.100.50,1434;DATABASE=MSSQL_DW;UID=sa;PWD=Pass@1234"


# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class BCCRAPIClient:
    """Cliente para obtener tipos de cambio del BCCR"""
    
    # Token del BCCR
    TOKEN = "AVMGEIZILV"
    BASE_URL = "https://www.bccr.fi.cr/json/bccr"
    
    # Monedas soportadas (código BCCR)
    MONEDAS = {
        'USD': 318,      # Dólar USA
        'EUR': 303,      # Euro
        'MXN': 332,      # Peso Mexicano
        'GTQ': 320,      # Quetzal Guatemalteco
        'PEN': 427,      # Sol Peruano
        'COP': 313,      # Peso Colombiano
        'BRL': 301,      # Real Brasileño
        'CRC': 336,      # Colón Costarricense
    }
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Mozilla/5.0'})
    
    def obtener_tasa(self, de_moneda: str, a_moneda: str, fecha: datetime):
        """Obtiene tasa de cambio para una fecha específica"""
        try:
            if de_moneda not in self.MONEDAS or a_moneda not in self.MONEDAS:
                return None
            
            de_codigo = self.MONEDAS[de_moneda]
            a_codigo = self.MONEDAS[a_moneda]
            
            # Formatear fecha para BCCR
            fecha_str = fecha.strftime('%d/%m/%Y')
            
            # Construir URL
            url = f"{self.BASE_URL}/{de_codigo}/{a_codigo}/{fecha_str}/{self.TOKEN}"
            
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            # Extraer tasa del resultado
            if data.get('resultados') and len(data['resultados']) > 0:
                return float(data['resultados'][0]['TC'])
            else:
                logger.warning(f"Sin datos para {de_moneda}→{a_moneda} en {fecha_str}")
                return None
                
        except Exception as e:
            logger.warning(f"Error obteniendo tasa {de_moneda}→{a_moneda} ({fecha}): {e}")
            return None
    
    def obtener_rango_tasas(self, de_moneda: str, a_moneda: str, fecha_inicio: datetime, fecha_fin: datetime):
        """Obtiene tasas para un rango de fechas"""
        try:
            if de_moneda not in self.MONEDAS or a_moneda not in self.MONEDAS:
                return []
            
            de_codigo = self.MONEDAS[de_moneda]
            a_codigo = self.MONEDAS[a_moneda]
            
            # Formatear fechas
            inicio_str = fecha_inicio.strftime('%d/%m/%Y')
            fin_str = fecha_fin.strftime('%d/%m/%Y')
            
            # Construir URL para rango
            url = f"{self.BASE_URL}/{de_codigo}/{a_codigo}/{inicio_str}/{fin_str}/{self.TOKEN}"
            
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            tasas = []
            if data.get('resultados'):
                for resultado in data['resultados']:
                    tasas.append({
                        'fecha': datetime.strptime(resultado['Fecha'], '%d/%m/%Y').date(),
                        'tasa': float(resultado['TC'])
                    })
            
            logger.info(f"Obtenidas {len(tasas)} tasas para {de_moneda}→{a_moneda}")
            return tasas
            
        except Exception as e:
            logger.error(f"Error obteniendo rango de tasas {de_moneda}→{a_moneda}: {e}")
            return []
    
    def cerrar(self):
        """Cierra la sesión"""
        self.session.close()


class HistoricoTiposCambio:
    """Carga histórico de tipos de cambio 3 años desde BCCR"""
    
    def __init__(self):
        self.conn = None
        self.bccr_client = BCCRAPIClient()
        self.fecha_inicio = None
        self.fecha_fin = None
        self.datos_cargados = []
    
    def conectar(self):
        """Conecta a DWH"""
        try:
            conn_str = DatabaseConfig.get_dw_connection_string()
            self.conn = pyodbc.connect(conn_str)
            print("[✅] Conectado a DWH")
            return True
        except Exception as e:
            print(f"[❌] Error conectando a DWH: {e}")
            return False
    
    def cerrar(self):
        """Cierra conexiones"""
        if self.conn:
            self.conn.close()
        self.bccr_client.cerrar()
    
    def cargar_historico_3_anos(self):
        """Carga histórico de 3 años desde BCCR"""
        try:
            print("\n" + "=" * 80)
            print("CARGANDO HISTORICO DE TIPOS DE CAMBIO (3 AÑOS DESDE BCCR)")
            print("=" * 80)
            print()
            
            # Calcular rango de fechas
            fecha_fin = datetime.now().date()
            fecha_inicio = fecha_fin - timedelta(days=365*3)
            
            print(f"[RANGO] Período: {fecha_inicio} a {fecha_fin}")
            print(f"[BCCR] Token: {self.bccr_client.TOKEN}")
            print()
            
            cursor = self.conn.cursor()
            
            # Limpiar tabla
            print("[LIMPIANDO] Tabla DimExchangeRate...")
            cursor.execute("TRUNCATE TABLE DimExchangeRate")
            self.conn.commit()
            print()
            
            # Obtener tasas por cada moneda
            monedas = ['CRC', 'USD', 'EUR', 'MXN', 'GTQ', 'PEN', 'COP', 'BRL']
            registros_totales = 0
            
            for de_moneda in monedas:
                print(f"[OBTENIENDO] Tasas para {de_moneda}→USD ({fecha_inicio} a {fecha_fin})")
                
                # Obtener rango de tasas desde BCCR
                tasas = self.bccr_client.obtener_rango_tasas(de_moneda, 'USD', fecha_inicio, fecha_fin)
                
                if tasas:
                    registros_insertados = 0
                    for tasa_data in tasas:
                        try:
                            cursor.execute("""
                                INSERT INTO DimExchangeRate (fromCurrency, toCurrency, date, rate)
                                VALUES (?, ?, ?, ?)
                            """, (de_moneda, 'USD', tasa_data['fecha'], tasa_data['tasa']))
                            registros_insertados += 1
                        except Exception as e:
                            logger.warning(f"Error insertando {de_moneda}→USD ({tasa_data['fecha']}): {e}")
                    
                    if registros_insertados > 0:
                        self.conn.commit()
                        print(f"  ✅ {registros_insertados} registros insertados")
                        registros_totales += registros_insertados
                    else:
                        print(f"  ⚠️  No se pudieron insertar registros")
                else:
                    print(f"  ⚠️  No se obtuvieron datos del BCCR")
            
            cursor.close()
            
            print()
            print("=" * 80)
            print(f"[✅] HISTORICO CARGADO: {registros_totales} registros")
            print("=" * 80)
            
            # Verificar carga
            self.verificar_carga()
            
            return True
            
        except Exception as e:
            print(f"\n[❌] Error cargando histórico: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def verificar_carga(self):
        """Verifica que la carga fue exitosa"""
        try:
            print("\n[VERIFICACION] Conteos por moneda:")
            print("-" * 80)
            
            cursor = self.conn.cursor()
            
            cursor.execute("""
                SELECT 
                    fromCurrency AS 'Moneda',
                    COUNT(*) AS 'Registros',
                    MIN(rate) AS 'Tasa Min',
                    MAX(rate) AS 'Tasa Max',
                    AVG(rate) AS 'Tasa Promedio'
                FROM DimExchangeRate
                GROUP BY fromCurrency
                ORDER BY fromCurrency
            """)
            
            rows = cursor.fetchall()
            
            for row in rows:
                print(f"  {row[0]:4} → USD:  {row[1]:6} registros  |  "
                      f"Min: {row[2]:.6f}  |  Max: {row[3]:.6f}  |  Prom: {row[4]:.6f}")
            
            cursor.close()
            
            # Total
            cursor = self.conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM DimExchangeRate")
            total = cursor.fetchone()[0]
            cursor.close()
            
            print("-" * 80)
            print(f"  TOTAL: {total:,} registros de tipos de cambio")
            print()
            
        except Exception as e:
            print(f"[⚠️ ] Error en verificación: {e}")
    
    def crear_vista_consulta_rapida(self):
        """Crea vista para que los ETLs consulten fácilmente"""
        try:
            print("\n[CREANDO] Vista para consultas rápidas...")
            
            cursor = self.conn.cursor()
            
            # Eliminar vista si existe
            cursor.execute("""
                IF OBJECT_ID('dbo.vw_exchange_rate_latest', 'V') IS NOT NULL
                    DROP VIEW dbo.vw_exchange_rate_latest;
            """)
            
            # Crear vista con las tasas más recientes por moneda
            cursor.execute("""
                CREATE VIEW dbo.vw_exchange_rate_latest AS
                SELECT 
                    fromCurrency,
                    toCurrency,
                    MAX(date) AS 'fecha_ultima',
                    (SELECT rate FROM DimExchangeRate der2 
                     WHERE der2.fromCurrency = der1.fromCurrency 
                     AND der2.toCurrency = der1.toCurrency
                     AND der2.date = MAX(der1.date)) AS 'tasa_ultima'
                FROM DimExchangeRate der1
                GROUP BY fromCurrency, toCurrency;
            """)
            
            cursor.close()
            self.conn.commit()
            
            print("[✅] Vista vw_exchange_rate_latest creada")
            print()
            print("Uso desde ETL:")
            print("  SELECT tasa_ultima FROM vw_exchange_rate_latest")
            print("  WHERE fromCurrency = 'CRC' AND toCurrency = 'USD'")
            
        except Exception as e:
            print(f"[⚠️ ] Error creando vista: {e}")
    
    def mostrar_ejemplo_uso(self):
        """Muestra ejemplos de cómo usar la tabla"""
        print()
        print("=" * 80)
        print("EJEMPLOS DE USO PARA LOS ETLs")
        print("=" * 80)
        print()
        
        print("1. OBTENER TASA PARA UNA FECHA ESPECIFICA:")
        print("-" * 80)
        print("""
    SELECT rate 
    FROM DimExchangeRate
    WHERE fromCurrency = 'CRC'
    AND toCurrency = 'USD'
    AND date = CAST(GETDATE() AS DATE)
        """)
        
        print("\n2. OBTENER TASA MAS RECIENTE:")
        print("-" * 80)
        print("""
    SELECT TOP 1 rate 
    FROM DimExchangeRate
    WHERE fromCurrency = 'CRC'
    AND toCurrency = 'USD'
    ORDER BY date DESC
        """)
        
        print("\n3. OBTENER TASA APROXIMADA (Si no existe exactamente para esa fecha):")
        print("-" * 80)
        print("""
    SELECT TOP 1 rate 
    FROM DimExchangeRate
    WHERE fromCurrency = 'CRC'
    AND toCurrency = 'USD'
    AND date <= @fecha_transaccion
    ORDER BY date DESC
        """)
        
        print("\n4. USAR EN JOIN CON FACT TABLE:")
        print("-" * 80)
        print("""
    SELECT 
        fs.*,
        der.rate,
        fs.lineTotalUSD * der.rate AS 'monto_convertido'
    FROM FactSales fs
    INNER JOIN DimExchangeRate der 
        ON fs.timeId = CAST(der.date AS INT)
        AND der.fromCurrency = 'CRC'
        AND der.toCurrency = 'USD'
        """)
        
        print()
        print("=" * 80)
    
    def ejecutar(self):
        """Ejecuta el flujo completo"""
        if not self.conectar():
            return False
        
        try:
            if not self.cargar_historico_3_anos():
                return False
            
            self.crear_vista_consulta_rapida()
            self.mostrar_ejemplo_uso()
            
            return True
        finally:
            self.cerrar()


if __name__ == "__main__":
    print()
    print("╔" + "=" * 78 + "╗")
    print("║" + " CARGA DE HISTORICO DE TIPOS DE CAMBIO (3 AÑOS) ".center(78) + "║")
    print("║" + " para DimExchangeRate en MSSQL_DW ".center(78) + "║")
    print("╚" + "=" * 78 + "╝")
    print()
    
    loader = HistoricoTiposCambio()
    success = loader.ejecutar()
    
    if success:
        print("\n[✅] Proceso completado exitosamente")
        sys.exit(0)
    else:
        print("\n[❌] Proceso finalizado con errores")
        sys.exit(1)
