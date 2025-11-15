#!/usr/bin/env python3
"""
Script para actualizar tipos de cambio diariamente a las 5 AM
Diseñado para ejecutarse desde SQL Agent Job, cron, o Task Scheduler

REGLA 2: Normalización de moneda - Actualización automática

CONFIGURACIÓN:
- Configurar connection string del DWH en la línea 28
- Para SQL Agent Job: Ejecutar desde PowerShell o CmdExec
- Para cron (Linux): 0 5 * * * python /ruta/a/update_daily.py
- Para Task Scheduler (Windows): Diariamente a las 5:00 AM
"""
import logging
import sys
from pathlib import Path
from datetime import datetime

# Agregar módulo BCCR al path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from bccr_integration import BCCRIntegration


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


def get_dwh_loader():
    """
    IMPORTANTE: Cada equipo debe implementar su propio loader
    Este es un ejemplo genérico. Ajustar según tu DWH.
    
    Returns:
        Objeto con método load_staging_exchange_rates_dataframe(df)
    """
    # OPCIÓN 1: SQL Server (MSSQL)
    try:
        sys.path.insert(0, str(Path(__file__).parent.parent / 'MSSQL' / 'etl'))
        from config import DatabaseConfig
        from load import DataLoader
        loader = DataLoader(DatabaseConfig.get_dw_connection_string())
        return loader
    except Exception as e:
        print(f"[WARNING] No se pudo cargar DataLoader de MSSQL: {e}")
    
    # OPCIÓN 2: MySQL
    try:
        sys.path.insert(0, str(Path(__file__).parent.parent / 'MYSQL' / 'etl'))
        from config import DatabaseConfig
        from load import DataLoader
        loader = DataLoader(DatabaseConfig.get_dw_connection_string())
        return loader
    except Exception as e:
        print(f"[WARNING] No se pudo cargar DataLoader de MySQL: {e}")
    
    # OPCIÓN 3: Crear loader genérico aquí
    raise Exception("No se encontró DataLoader. Configurar en get_dwh_loader()")


def main():
    """Actualiza tasa de cambio del día"""
    logger = setup_logging()
    
    logger.info("=" * 80)
    logger.info(f"ACTUALIZACIÓN DIARIA DE TIPOS DE CAMBIO BCCR")
    logger.info(f"Fecha/Hora: {datetime.now().isoformat()}")
    logger.info("=" * 80)
    
    try:
        # Obtener loader del DWH
        logger.info("\n[1] Inicializando conexión al DWH...")
        loader = get_dwh_loader()
        logger.info("[OK] Conexión exitosa")
        
        # Crear servicio BCCR
        logger.info("\n[2] Inicializando servicio BCCR...")
        from bccr_integration import ExchangeRateService
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
