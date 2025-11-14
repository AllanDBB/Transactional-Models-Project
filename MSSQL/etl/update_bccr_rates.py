"""
Script para actualizar tipos de cambio diariamente a las 5 AM
Diseñado para ejecutarse desde SQL Agent Job

REGLA 2: Normalización de moneda - Actualización automática

Para configurar en SQL Server:
1. Ejecutar: MSSQL/etl/bccr_integration.py -> SQL_AGENT_JOB_SCRIPT
2. El Job llamará a este script cada día a las 5 AM
3. Descargará la tasa del día y la insertará en staging_tipo_cambio
"""
import logging
import sys
from pathlib import Path
from datetime import datetime

# Agregar ruta del ETL
sys.path.insert(0, str(Path(__file__).parent))

from config import DatabaseConfig
from load import DataLoader
from bccr_integration import ExchangeRateService


def setup_logging():
    """Configura logging para Job"""
    log_dir = Path(__file__).parent / 'logs'
    log_dir.mkdir(exist_ok=True)
    
    log_file = log_dir / f"bccr_daily_{datetime.now().strftime('%Y%m%d')}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


def main():
    """Actualiza tasa de cambio del día"""
    logger = setup_logging()
    
    logger.info("=" * 80)
    logger.info(f"ACTUALIZACIÓN DIARIA DE TIPOS DE CAMBIO BCCR")
    logger.info(f"Fecha/Hora: {datetime.now().isoformat()}")
    logger.info("=" * 80)
    
    try:
        # Conectar DWH
        logger.info("\n[1] Conectando a MSSQL_DW...")
        loader = DataLoader(DatabaseConfig.get_dw_connection_string())
        logger.info("[OK] Conexion exitosa")
        
        # Servicio de tasas
        logger.info("\n[2] Inicializando servicio BCCR...")
        service = ExchangeRateService(loader)
        logger.info("[OK] Servicio inicializado")
        
        # Obtener y actualizar tasa del día
        logger.info("\n[3] Descargando tasa del día desde BCCR...")
        inserted = service.update_daily_rates()
        
        if inserted > 0:
            logger.info("\n" + "=" * 80)
            logger.info("[OK] TASA DIARIA ACTUALIZADA")
            logger.info("=" * 80)
            logger.info(f"\n[RESULTADO]")
            logger.info(f"  [OK] Registros insertados: {inserted}")
            logger.info(f"  [OK] Fecha: {datetime.now().date()}")
            logger.info(f"  [OK] Par: CRC/USD")
            logger.info(f"  [OK] Tabla: staging_tipo_cambio")
            return 0
        else:
            logger.warning("\n" + "=" * 80)
            logger.warning("[WARN] TASA DIARIA NO SE PUDO ACTUALIZAR")
            logger.warning("=" * 80)
            logger.warning("\nPosibles causas:")
            logger.warning("  - Feriado bancario (BCCR cerrado)")
            logger.warning("  - Error de conexión a BCCR")
            logger.warning("  - Tasa ya existe en BD (no duplicada)")
            return 1
    
    except Exception as e:
        logger.error("\n" + "=" * 80)
        logger.error("[ERROR] ERROR EN ACTUALIZACION DIARIA")
        logger.error("=" * 80)
        logger.error(f"\nDetalle: {str(e)}")
        logger.exception("Traceback completo:")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
