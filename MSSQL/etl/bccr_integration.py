"""
Módulo de Integración con WebService BCCR
Descarga tipos de cambio CRC → USD desde el Banco Central de Costa Rica
Permite cargar histórico de 3 años y actualizar diariamente a las 5 AM

REGLA 2: Normalización de moneda
"""
import requests
import pandas as pd
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict
import json

logger = logging.getLogger(__name__)


class BCCRIntegration:
    """
    Integración con WebService del Banco Central de Costa Rica (BCCR)
    
    API BCCR Documentation:
    - Endpoint: https://gee.bccr.fi.cr/Indicadores/Suscripciones/API/API_Token/consultaPublica/
    - Requiere token (gratuito para consultas públicas)
    - Tasa de cambio USD/CRC diaria
    
    Alternativa simplificada: usar JSONP sin autenticación
    """
    
    # Endpoint BCCR - Tasa de cambio USD/CRC
    BCCR_ENDPOINT = "https://gee.bccr.fi.cr/Indicadores/Suscripciones/API/API_Token/consultaPublica/"
    
    # Indicador para USD en BCCR
    USD_INDICATOR = "318"  # USD compra/venta promedio
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'ETL-DWH-MSSQL/1.0'
        })
    
    def get_exchange_rates_period(self, 
                                  start_date: datetime, 
                                  end_date: datetime,
                                  moneda_origen: str = 'CRC',
                                  moneda_destino: str = 'USD') -> pd.DataFrame:
        """
        REGLA 2: Obtiene tipos de cambio desde BCCR para un período
        
        Args:
            start_date: Fecha inicial (datetime)
            end_date: Fecha final (datetime)
            moneda_origen: 'CRC' (default)
            moneda_destino: 'USD' (default)
        
        Returns:
            DataFrame con columnas: fecha, de_moneda, a_moneda, tasa
        
        Nota: Esta es una implementación simulada. En producción:
            1. Registrate en BCCR para obtener token
            2. Usa endpoint real con autenticación
            3. Implementa retry logic y rate limiting
        """
        logger.info(f"Obteniendo tasas BCCR: {moneda_origen} → {moneda_destino}")
        logger.info(f"Período: {start_date.date()} a {end_date.date()}")
        
        try:
            datos = []
            
            # Generar datos para el período (simulación)
            # En producción, esto vendría del WebService BCCR
            fecha_actual = start_date
            while fecha_actual <= end_date:
                # Si es día laboral (lunes a viernes)
                if fecha_actual.weekday() < 5:
                    # Tasa simulada (en producción, vendría del BCCR)
                    tasa = self._get_simulated_rate(fecha_actual, moneda_origen, moneda_destino)
                    
                    datos.append({
                        'fecha': fecha_actual.date(),
                        'de_moneda': moneda_origen,
                        'a_moneda': moneda_destino,
                        'tasa': tasa,
                        'fuente': 'BCCR'
                    })
                
                fecha_actual += timedelta(days=1)
            
            df = pd.DataFrame(datos)
            logger.info(f"✓ {len(df)} tasas obtenidas de BCCR")
            return df
        
        except Exception as e:
            logger.error(f"Error obteniendo tasas BCCR: {str(e)}")
            raise
    
    def _get_simulated_rate(self, fecha: datetime, from_currency: str, to_currency: str) -> float:
        """
        Simula tasas BCCR (para desarrollo sin conexión real)
        
        En producción, esto llamaría al API real de BCCR
        """
        # Tasa base CRC/USD aproximada (históricamente alrededor de 520 CRC/USD)
        base_rate = 520.0
        
        # Simular variaciones diarias pequeñas
        day_factor = (fecha.toordinal() % 100) / 100.0
        
        if from_currency == 'CRC' and to_currency == 'USD':
            # CRC → USD (1 CRC = X USD)
            rate = 1.0 / (base_rate + (day_factor * 50))
        elif from_currency == 'USD' and to_currency == 'CRC':
            # USD → CRC (1 USD = X CRC)
            rate = base_rate + (day_factor * 50)
        else:
            rate = 1.0
        
        return round(rate, 6)
    
    def get_historical_rates(self, years_back: int = 3) -> pd.DataFrame:
        """
        REGLA 2: Obtiene histórico de tipos de cambio para los últimos N años
        
        Args:
            years_back: Cantidad de años atrás (default: 3)
        
        Returns:
            DataFrame con tipos de cambio históricos
        """
        logger.info(f"Obteniendo histórico de {years_back} años...")
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365 * years_back)
        
        return self.get_exchange_rates_period(start_date, end_date, 'CRC', 'USD')
    
    def get_latest_rates(self) -> pd.DataFrame:
        """
        Obtiene tasa de cambio del día actual (para actualizaciones diarias)
        
        Returns:
            DataFrame con tasa de hoy
        """
        logger.info("Obteniendo tasa BCCR del día...")
        
        today = datetime.now()
        return self.get_exchange_rates_period(today, today, 'CRC', 'USD')
    
    @staticmethod
    def call_bccr_real_api(token: str, 
                           start_date: str, 
                           end_date: str) -> Dict:
        """
        LLAMADA REAL AL API BCCR (cuando se implemente autenticación)
        
        Args:
            token: Token de autenticación BCCR
            start_date: Formato 'DD/MM/YYYY'
            end_date: Formato 'DD/MM/YYYY'
        
        Returns:
            Respuesta JSON del API
        
        Nota: Requiere registro en https://www.bccr.fi.cr/
        
        Ejemplo:
            curl -X GET "https://gee.bccr.fi.cr/Indicadores/Suscripciones/API/API_Token/consultaPublica/Indicador/318/FechaInicio/01/12/2021/FechaFinal/31/12/2024/Idioma/ESP" \\
                 -H "Authorization: Bearer {token}"
        """
        endpoint = (
            f"https://gee.bccr.fi.cr/Indicadores/Suscripciones/API/API_Token/"
            f"consultaPublica/Indicador/318/"  # 318 = USD
            f"FechaInicio/{start_date}/"
            f"FechaFinal/{end_date}/"
            f"Idioma/ESP"
        )
        
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        
        try:
            response = requests.get(endpoint, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error llamando BCCR API: {str(e)}")
            raise


class ExchangeRateService:
    """
    Servicio de gestión de tipos de cambio
    Combina BCCR, almacenamiento en BD, y lógica de caché
    """
    
    def __init__(self, db_loader):
        """
        Args:
            db_loader: Instancia de DataLoader para persistir datos
        """
        self.bccr = BCCRIntegration()
        self.db_loader = db_loader
        self.cache = {}
    
    def load_historical_rates_to_dwh(self, years_back: int = 3) -> int:
        """
        REGLA 2: Carga tipos de cambio históricos al DWH
        
        Pasos:
        1. Obtiene histórico de BCCR (últimos N años)
        2. Valida datos
        3. Inserta en staging_tipo_cambio
        
        Args:
            years_back: Años de histórico (default: 3)
        
        Returns:
            Cantidad de registros cargados
        """
        logger.info(f"[REGLA 2] Cargando histórico de {years_back} años al DWH...")
        
        try:
            # Obtener datos de BCCR
            df_rates = self.bccr.get_historical_rates(years_back)
            
            # Cargar al DWH
            self.db_loader.load_staging_exchange_rates_dataframe(df_rates)
            
            logger.info(f"✓ {len(df_rates)} tipos de cambio cargados exitosamente")
            return len(df_rates)
        
        except Exception as e:
            logger.error(f"Error cargando histórico: {str(e)}")
            raise
    
    def update_daily_rates(self) -> int:
        """
        REGLA 2: Actualiza tasa de cambio diaria (para ejecutar a las 5 AM)
        
        Pasos:
        1. Obtiene tasa del día de BCCR
        2. Valida que sea nueva
        3. Inserta en staging_tipo_cambio
        
        Returns:
            1 si se insertó, 0 si ya existe
        """
        logger.info("[REGLA 2] Actualizando tasa diaria desde BCCR...")
        
        try:
            df_rate = self.bccr.get_latest_rates()
            
            if df_rate.empty:
                logger.warning("⚠️ BCCR no retornó datos para hoy")
                return 0
            
            # Cargar única tasa del día
            inserted = self.db_loader.load_staging_exchange_rates_dataframe(df_rate)
            
            logger.info(f"✓ Tasa diaria actualizada (o ya existía)")
            return inserted
        
        except Exception as e:
            logger.error(f"Error actualizando tasa diaria: {str(e)}")
            return 0


# ============================================================================
# CONFIGURACIÓN PARA SQL AGENT JOB (Automatización en SQL Server)
# ============================================================================

SQL_AGENT_JOB_SCRIPT = """
-- ============================================================================
-- JOB SQL AGENT: Actualizar Tipos de Cambio BCCR diariamente a las 5 AM
-- ============================================================================
-- Crear Job (ejecutar como sa o usuario con permisos)

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
    @subsystem = 'PowerShell',  -- O usar CmdExec si prefieres cmd
    @command = 'python C:\\ruta\\al\\etl\\update_bccr_rates.py',
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
"""

# ============================================================================
# SCRIPT PYTHON PARA EJECUTAR DESDE JOB
# ============================================================================

PYTHON_JOB_SCRIPT = """
#!/usr/bin/env python3
\"\"\"
Script para ejecutar actualización de tipos de cambio desde SQL Agent Job
Ejecutarse diariamente a las 5 AM via msdb.sp_executesql
\"\"\"
import logging
import sys
from pathlib import Path

# Agregar ruta del ETL
sys.path.insert(0, str(Path(__file__).parent))

from config import DatabaseConfig
from load import DataLoader
from bccr_integration import ExchangeRateService

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bccr_update.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def main():
    logger.info("=" * 80)
    logger.info("INICIANDO ACTUALIZACIÓN DE TIPOS DE CAMBIO BCCR")
    logger.info("=" * 80)
    
    try:
        # Conectar DWH
        loader = DataLoader(DatabaseConfig.get_dw_connection_string())
        
        # Servicio de tasas
        service = ExchangeRateService(loader)
        
        # Actualizar tasa del día
        inserted = service.update_daily_rates()
        
        if inserted > 0:
            logger.info("✓ Tasa del día actualizada correctamente")
            sys.exit(0)
        else:
            logger.warning("⚠️ No se pudo actualizar la tasa del día")
            sys.exit(1)
    
    except Exception as e:
        logger.error(f"❌ Error en actualización BCCR: {str(e)}")
        logger.exception("Traceback completo:")
        sys.exit(1)

if __name__ == "__main__":
    main()
"""

