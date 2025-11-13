"""
Script para cargar histórico de tipos de cambio desde BCCR
Ejecutar una sola vez para llenar los últimos 3 años

REGLA 2: Normalización de moneda
"""
import logging
import sys
from pathlib import Path

# Agregar ruta del ETL
sys.path.insert(0, str(Path(__file__).parent))

from config import DatabaseConfig
from load import DataLoader
from bccr_integration import ExchangeRateService


def setup_logging():
    """Configura logging"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('bccr_historical_load.log'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


def main():
    """Carga histórico de 3 años de tipos de cambio"""
    logger = setup_logging()
    
    logger.info("=" * 80)
    logger.info("CARGANDO HISTÓRICO DE TIPOS DE CAMBIO BCCR (3 AÑOS)")
    logger.info("=" * 80)
    logger.info("\nREGLA 2: Normalización de moneda")
    logger.info("  - Obtiene histórico de 3 años desde BCCR")
    logger.info("  - Almacena en staging_tipo_cambio")
    logger.info("  - Permite conversiones CRC → USD")
    logger.info("\n" + "=" * 80)
    
    try:
        # Conectar DWH
        logger.info("\n[1] Conectando a MSSQL_DW...")
        loader = DataLoader(DatabaseConfig.get_dw_connection_string())
        logger.info("✓ Conexión exitosa")
        
        # Servicio de tasas
        logger.info("\n[2] Inicializando servicio de tasas BCCR...")
        service = ExchangeRateService(loader)
        logger.info("✓ Servicio inicializado")
        
        # Cargar histórico
        logger.info("\n[3] Descargando histórico de 3 años...")
        inserted = service.load_historical_rates_to_dwh(years_back=3)
        
        logger.info("\n" + "=" * 80)
        logger.info("✅ HISTÓRICO CARGADO EXITOSAMENTE")
        logger.info("=" * 80)
        logger.info(f"\n[RESULTADO]")
        logger.info(f"  ✓ Registros cargados: {inserted}")
        logger.info(f"  ✓ Período: Últimos 3 años")
        logger.info(f"  ✓ Tabla: staging_tipo_cambio")
        logger.info(f"  ✓ Moneda origen: CRC")
        logger.info(f"  ✓ Moneda destino: USD")
        logger.info("\n[PRÓXIMOS PASOS]")
        logger.info("  1. Configurar SQL Agent Job para actualizar diariamente a las 5 AM")
        logger.info("  2. Ver script: bccr_integration.SQL_AGENT_JOB_SCRIPT")
        logger.info("  3. Ejecutar: python etl/update_bccr_rates.py en el job")
        logger.info("\n" + "=" * 80)
        
        return 0
    
    except Exception as e:
        logger.error(f"\n❌ ERROR CARGANDO HISTÓRICO")
        logger.error(f"Detalle: {str(e)}")
        logger.exception("Traceback completo:")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
