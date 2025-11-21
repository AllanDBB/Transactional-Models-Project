#!/usr/bin/env python3
"""
Módulo ExchangeRateHelper
Proporciona métodos para que los ETLs consulten tipos de cambio de DWH
Uso: Cada ETL importa esto y llama a get_exchange_rate() con sus parámetros
"""

import pyodbc
from datetime import datetime, date
import logging
from typing import Optional, Dict

logger = logging.getLogger(__name__)


class ExchangeRateHelper:
    """Helper para obtener tipos de cambio desde DimExchangeRate en DWH"""
    
    def __init__(self, dw_connection_string: str):
        """
        Inicializa el helper con la conexión al DWH
        
        Args:
            dw_connection_string: String de conexión a MSSQL_DW
        """
        self.connection_string = dw_connection_string
        self.cache: Dict = {}  # Cache de tasas consultadas
        self.conn = None
    
    def conectar(self):
        """Establece conexión al DWH"""
        try:
            self.conn = pyodbc.connect(self.connection_string)
            logger.info("[ExchangeRateHelper] Conectado a DWH")
            return True
        except Exception as e:
            logger.error(f"[ExchangeRateHelper] Error conectando: {e}")
            return False
    
    def cerrar(self):
        """Cierra la conexión"""
        if self.conn:
            self.conn.close()
            logger.info("[ExchangeRateHelper] Conexión cerrada")
    
    def __enter__(self):
        """Context manager: entrada"""
        self.conectar()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager: salida"""
        self.cerrar()
    
    def obtener_tasa_para_fecha(
        self,
        de_moneda: str,
        a_moneda: str,
        fecha: Optional[date] = None,
        usar_cache: bool = True
    ) -> Optional[float]:
        """
        Obtiene la tasa de cambio para una fecha específica
        
        Args:
            de_moneda: Moneda origen (ej: 'CRC', 'EUR', 'MXN')
            a_moneda: Moneda destino (ej: 'USD')
            fecha: Fecha de la tasa (si None, usa hoy)
            usar_cache: Si True, consulta cache primero
        
        Returns:
            float: Tasa de cambio o None si no existe
        
        Ejemplo:
            tasa = helper.obtener_tasa_para_fecha('CRC', 'USD', date(2024, 1, 15))
            monto_convertido = monto_crc * tasa
        """
        
        if fecha is None:
            fecha = datetime.now().date()
        
        # Validar formato de fecha
        if isinstance(fecha, datetime):
            fecha = fecha.date()
        
        # Clave de cache
        cache_key = f"{de_moneda}_{a_moneda}_{fecha}"
        
        # Consultar cache
        if usar_cache and cache_key in self.cache:
            logger.debug(f"[Cache] Tasa desde cache: {cache_key}")
            return self.cache[cache_key]
        
        # Consultar base de datos
        try:
            if not self.conn:
                self.conectar()
            
            cursor = self.conn.cursor()
            
            # Consultar tasa exacta para la fecha
            cursor.execute("""
                SELECT rate 
                FROM DimExchangeRate
                WHERE fromCurrency = ?
                AND toCurrency = ?
                AND date = ?
            """, de_moneda, a_moneda, fecha)
            
            row = cursor.fetchone()
            
            if row:
                tasa = float(row[0])
                # Guardar en cache
                self.cache[cache_key] = tasa
                logger.debug(f"[DB] Tasa obtenida: {de_moneda} → {a_moneda} = {tasa}")
                return tasa
            
            cursor.close()
            
            # Si no existe la fecha exacta, buscar la más cercana anterior
            logger.warning(f"No existe tasa exacta para {fecha}, buscando anterior...")
            cursor = self.conn.cursor()
            
            cursor.execute("""
                SELECT TOP 1 rate, date
                FROM DimExchangeRate
                WHERE fromCurrency = ?
                AND toCurrency = ?
                AND date <= ?
                ORDER BY date DESC
            """, de_moneda, a_moneda, fecha)
            
            row = cursor.fetchone()
            
            if row:
                tasa = float(row[0])
                fecha_encontrada = row[1]
                # Guardar con fecha encontrada como clave secundaria
                cache_key_secundaria = f"{de_moneda}_{a_moneda}_{fecha_encontrada}"
                self.cache[cache_key_secundaria] = tasa
                logger.warning(f"Usando tasa de {fecha_encontrada}: {tasa}")
                return tasa
            
            cursor.close()
            logger.error(f"No existe tasa para {de_moneda} → {a_moneda}")
            return None
            
        except Exception as e:
            logger.error(f"Error obtiendo tasa: {e}")
            return None
    
    def obtener_tasa_reciente(
        self,
        de_moneda: str,
        a_moneda: str
    ) -> Optional[float]:
        """
        Obtiene la tasa más reciente disponible
        Útil cuando no tenemos fecha exacta de la transacción
        
        Args:
            de_moneda: Moneda origen
            a_moneda: Moneda destino
        
        Returns:
            float: Tasa de cambio más reciente
        
        Ejemplo:
            tasa = helper.obtener_tasa_reciente('CRC', 'USD')
        """
        
        cache_key = f"{de_moneda}_{a_moneda}_latest"
        
        # Consultar cache
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        try:
            if not self.conn:
                self.conectar()
            
            cursor = self.conn.cursor()
            
            cursor.execute("""
                SELECT TOP 1 rate
                FROM DimExchangeRate
                WHERE fromCurrency = ?
                AND toCurrency = ?
                ORDER BY date DESC
            """, de_moneda, a_moneda)
            
            row = cursor.fetchone()
            cursor.close()
            
            if row:
                tasa = float(row[0])
                self.cache[cache_key] = tasa
                logger.info(f"Tasa reciente: {de_moneda} → {a_moneda} = {tasa}")
                return tasa
            
            return None
            
        except Exception as e:
            logger.error(f"Error obteniendo tasa reciente: {e}")
            return None
    
    def convertir_monto(
        self,
        monto: float,
        de_moneda: str,
        a_moneda: str,
        fecha: Optional[date] = None
    ) -> Optional[float]:
        """
        Convierte un monto de una moneda a otra
        
        Args:
            monto: Cantidad a convertir
            de_moneda: Moneda origen
            a_moneda: Moneda destino
            fecha: Fecha de conversión
        
        Returns:
            float: Monto convertido o None si error
        
        Ejemplo:
            usd = helper.convertir_monto(1000, 'CRC', 'USD', date(2024, 1, 15))
            # usd ≈ 1.67 (si tasa ≈ 600)
        """
        
        # Si es la misma moneda, retornar sin cambios
        if de_moneda == a_moneda:
            return monto
        
        tasa = self.obtener_tasa_para_fecha(de_moneda, a_moneda, fecha)
        
        if tasa is None:
            logger.error(f"No se pudo obtener tasa para {de_moneda} → {a_moneda}")
            return None
        
        monto_convertido = monto / tasa  # Dividir porque la tasa es de_moneda → USD
        logger.debug(f"Conversión: {monto} {de_moneda} → {monto_convertido:.2f} {a_moneda}")
        
        return monto_convertido
    
    def obtener_rango_tasas(
        self,
        de_moneda: str,
        a_moneda: str,
        fecha_inicio: date,
        fecha_fin: date
    ) -> list:
        """
        Obtiene todas las tasas en un rango de fechas
        Útil para análisis históricos
        
        Args:
            de_moneda: Moneda origen
            a_moneda: Moneda destino
            fecha_inicio: Fecha inicial
            fecha_fin: Fecha final
        
        Returns:
            List de tuplas (fecha, tasa)
        """
        
        try:
            if not self.conn:
                self.conectar()
            
            cursor = self.conn.cursor()
            
            cursor.execute("""
                SELECT date, rate
                FROM DimExchangeRate
                WHERE fromCurrency = ?
                AND toCurrency = ?
                AND date BETWEEN ? AND ?
                ORDER BY date
            """, de_moneda, a_moneda, fecha_inicio, fecha_fin)
            
            rows = cursor.fetchall()
            cursor.close()
            
            resultado = [(row[0], float(row[1])) for row in rows]
            logger.info(f"Rango obtenido: {len(resultado)} tasas de {fecha_inicio} a {fecha_fin}")
            
            return resultado
            
        except Exception as e:
            logger.error(f"Error obteniendo rango de tasas: {e}")
            return []
    
    def limpiar_cache(self):
        """Limpia el cache en memoria"""
        self.cache.clear()
        logger.info("Cache limpiado")


# Ejemplo de uso en un ETL
if __name__ == "__main__":
    
    # Configurar logging
    logging.basicConfig(level=logging.DEBUG)
    
    # Conexión a DWH
    DW_CONNECTION_STRING = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=192.168.100.50,1434;DATABASE=MSSQL_DW;UID=sa;PWD=Pass@1234"
    
    print("\n" + "=" * 80)
    print("EJEMPLO DE USO: ExchangeRateHelper")
    print("=" * 80 + "\n")
    
    # Usar como context manager
    with ExchangeRateHelper(DW_CONNECTION_STRING) as helper:
        
        # Ejemplo 1: Obtener tasa para una fecha
        print("[EJEMPLO 1] Obtener tasa para fecha específica:")
        tasa = helper.obtener_tasa_para_fecha('CRC', 'USD', date(2024, 1, 15))
        print(f"  CRC → USD (2024-01-15) = {tasa}\n")
        
        # Ejemplo 2: Obtener tasa reciente
        print("[EJEMPLO 2] Obtener tasa más reciente:")
        tasa_reciente = helper.obtener_tasa_reciente('EUR', 'USD')
        print(f"  EUR → USD (más reciente) = {tasa_reciente}\n")
        
        # Ejemplo 3: Convertir monto
        print("[EJEMPLO 3] Convertir monto:")
        crc_amount = 100000
        usd_amount = helper.convertir_monto(crc_amount, 'CRC', 'USD', date(2024, 1, 15))
        print(f"  {crc_amount} CRC → {usd_amount:.2f} USD\n")
        
        # Ejemplo 4: Obtener rango de tasas
        print("[EJEMPLO 4] Obtener rango de tasas:")
        tasas = helper.obtener_rango_tasas('CRC', 'USD', date(2024, 1, 1), date(2024, 1, 31))
        print(f"  Tasas en enero 2024: {len(tasas)} registros")
        if tasas:
            print(f"  Primera: {tasas[0]}")
            print(f"  Última: {tasas[-1]}\n")
    
    print("=" * 80)
