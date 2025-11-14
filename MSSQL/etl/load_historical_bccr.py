"""
Script para cargar histórico de tipos de cambio desde BCCR
Ejecutar una sola vez para llenar los últimos 3 años

REGLA 2: Normalización de moneda
"""
import logging
import sys
import traceback
from pathlib import Path

# Agregar ruta del ETL
sys.path.insert(0, str(Path(__file__).parent))

from config import DatabaseConfig
from load import DataLoader
from bccr_integration import ExchangeRateService


def setup_logging():
    """Configura logging con UTF-8 encoding para Windows"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('bccr_historical_load.log', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


def main():
    """Carga histórico de 3 años de tipos de cambio"""
    logger = setup_logging()
    
    logger.info("=" * 80)
    logger.info("CARGANDO HISTORICO DE TIPOS DE CAMBIO BCCR (3 ANOS)")
    logger.info("=" * 80)
    logger.info("\nREGLA 2: Normalizacion de moneda")
    logger.info("  - Obtiene historico de 3 anos desde BCCR")
    logger.info("  - Almacena en staging_tipo_cambio")
    logger.info("  - Permite conversiones CRC -> USD")
    logger.info("\n" + "=" * 80)
    
    try:
        # Conectar DWH
        logger.info("\n[1] Conectando a MSSQL_DW...")
        loader = DataLoader(DatabaseConfig.get_dw_connection_string())
        logger.info("[OK] Conexion exitosa")
        
        # Servicio de tasas
        logger.info("\n[2] Inicializando servicio de tasas BCCR...")
        service = ExchangeRateService(loader)
        logger.info("[OK] Servicio inicializado")
        
        # Cargar histórico
        logger.info("\n[3] Descargando historico de 3 anos...")
        inserted = service.load_historical_rates_to_dwh(years_back=3)
        
        logger.info("\n" + "=" * 80)
        logger.info("[EXITO] HISTORICO CARGADO EXITOSAMENTE")
        logger.info("=" * 80)
        logger.info(f"\n[RESULTADO]")
        logger.info(f"  [OK] Registros cargados: {inserted}")
        logger.info(f"  [OK] Periodo: Ultimos 3 anos")
        logger.info(f"  [OK] Tabla: staging_tipo_cambio")
        logger.info(f"  [OK] Moneda origen: CRC")
        logger.info(f"  [OK] Moneda destino: USD")
        logger.info("\n[PROXIMOS PASOS]")
        logger.info("  1. Configurar SQL Agent Job para actualizar diariamente a las 5 AM")
        logger.info("  2. Ver script: bccr_integration.SQL_AGENT_JOB_SCRIPT")
        logger.info("  3. Ejecutar: python etl/update_bccr_rates.py en el job")
        logger.info("\n" + "=" * 80)
        
        return 0
    
    except Exception as e:
        logger.error(f"\n[ERROR] ERROR CARGANDO HISTORICO")
        logger.error(f"Detalle: {str(e)}")
        logger.error("Traceback completo:")
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
