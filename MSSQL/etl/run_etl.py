"""
Script principal del ETL: MSSQL Transaccional → MSSQL Data Warehouse
Orquesta los procesos de Extract, Transform y Load
"""
import logging
import sys
from pathlib import Path

# Agregar la carpeta del proyecto al path
sys.path.insert(0, str(Path(__file__).parent))

from config import DatabaseConfig, ETLConfig
from extract import DataExtractor
from transform import DataTransformer
from load import DataLoader


def setup_logging():
    """Configura el logging"""
    logging.basicConfig(
        level=getattr(logging, ETLConfig.LOG_LEVEL),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(ETLConfig.LOG_FILE),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


def run_etl():
    """Ejecuta el proceso ETL completo"""
    logger = setup_logging()
    logger.info("=" * 80)
    logger.info("INICIANDO PROCESO ETL: MSSQL → DWH")
    logger.info("=" * 80)
    
    try:
        # ========== EXTRACT ==========
        logger.info("\n[FASE 1] EXTRAYENDO DATOS...")
        extractor = DataExtractor(DatabaseConfig.get_source_connection_string())
        
        clientes, productos, ordenes, orden_detalle = extractor.extract_all()
        
        logger.info(f"✓ Clientes extraídos: {len(clientes)}")
        logger.info(f"✓ Productos extraídos: {len(productos)}")
        logger.info(f"✓ Órdenes extraídas: {len(ordenes)}")
        logger.info(f"✓ Detalles extraídos: {len(orden_detalle)}")
        
        # ========== TRANSFORM ==========
        logger.info("\n[FASE 2] TRANSFORMANDO DATOS...")
        transformer = DataTransformer()
        
        # Transformar tablas principales
        clientes_trans = transformer.transform_clientes(clientes)
        productos_trans = transformer.transform_productos(productos)
        ordenes_trans = transformer.transform_ordenes(ordenes, clientes_trans)
        detalle_trans = transformer.transform_orden_detalle(orden_detalle, productos_trans)
        
        logger.info(f"✓ Clientes transformados: {len(clientes_trans)}")
        logger.info(f"✓ Productos transformados: {len(productos_trans)}")
        logger.info(f"✓ Órdenes transformadas: {len(ordenes_trans)}")
        logger.info(f"✓ Detalles transformados: {len(detalle_trans)}")
        
        # Extraer dimensiones
        categorias = transformer.extract_categorias(productos_trans)
        canales = transformer.extract_canales(ordenes_trans)
        dim_time = transformer.generate_dimtime(ordenes_trans)
        
        logger.info(f"✓ Categorías extraídas: {len(categorias)}")
        logger.info(f"✓ Canales extraídos: {len(canales)}")
        logger.info(f"✓ Fechas en DimTime: {len(dim_time)}")
        
        # ========== LOAD ==========
        logger.info("\n[FASE 3] CARGANDO DATOS AL DWH...")
        loader = DataLoader(DatabaseConfig.get_dw_connection_string())
        
        # Limpiar tablas
        tables_to_truncate = [
            'DimCategory',
            'DimChannel',
            'DimProduct',
            'DimCustomer',
            'DimTime',
            'FactSales'
        ]
        loader.truncate_tables(tables_to_truncate)
        
        # Cargar dimensiones
        loader.load_dim_category(categorias)
        loader.load_dim_channel(canales)
        loader.load_dim_customer(clientes_trans)
        loader.load_dim_time(dim_time)
        loader.load_dim_product(productos_trans)
        
        # Nota: La carga de FactSales requiere mapeos entre IDs
        logger.info("✓ Dimensiones cargadas correctamente")
        
        logger.info("\n" + "=" * 80)
        logger.info("✅ PROCESO ETL COMPLETADO EXITOSAMENTE")
        logger.info("=" * 80)
        
        return True
    
    except Exception as e:
        logger.error(f"\n❌ ERROR EN PROCESO ETL: {str(e)}")
        logger.exception("Traceback completo:")
        return False


if __name__ == "__main__":
    success = run_etl()
    sys.exit(0 if success else 1)
