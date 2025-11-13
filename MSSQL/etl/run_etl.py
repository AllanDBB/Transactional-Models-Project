"""
Script principal del ETL: MSSQL Transaccional → MSSQL Data Warehouse
Orquesta los procesos de Extract, Transform y Load
Implementa las 5 reglas de integración:
1. Homologación de productos
2. Normalización de moneda
3. Estandarización de género
4. Conversión de fechas
5. Transformación de totales
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
    """Ejecuta el proceso ETL completo con 5 reglas de integración"""
    logger = setup_logging()
    logger.info("=" * 80)
    logger.info("INICIANDO PROCESO ETL: MSSQL → DWH (5 Reglas de Integración)")
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
        logger.info("\n[FASE 2] TRANSFORMANDO DATOS (5 REGLAS)...")
        transformer = DataTransformer()
        
        # REGLA 3: Estandarización de género
        # REGLA 4: Conversión de fechas
        clientes_trans, track_cli = transformer.transform_clientes(clientes)
        
        # REGLA 1: Homologación de productos (tabla puente)
        productos_trans, track_prod = transformer.transform_productos(productos)
        
        # REGLA 2: Normalización de moneda (USD - homogénea en MSSQL)
        # REGLA 4: Conversión de fechas
        ordenes_trans, track_ord = transformer.transform_ordenes(ordenes)
        
        # REGLA 5: Transformación de totales
        detalle_trans, track_det = transformer.transform_orden_detalle(orden_detalle)
        
        logger.info(f"✓ Clientes transformados: {len(clientes_trans)}")
        logger.info(f"✓ Productos transformados: {len(productos_trans)}")
        logger.info(f"✓ Órdenes transformadas: {len(ordenes_trans)}")
        logger.info(f"✓ Detalles transformados: {len(detalle_trans)}")
        
        # Extraer dimensiones
        categorias = transformer.extract_categorias(productos_trans)
        canales = transformer.extract_canales(ordenes_trans)
        dim_time = transformer.generate_dimtime(ordenes_trans)
        
        # REGLA 1: Construir tabla puente de mapeo
        product_mapping = transformer.build_product_mapping(productos_trans)
        
        logger.info(f"✓ Categorías extraídas: {len(categorias)}")
        logger.info(f"✓ Canales extraídos: {len(canales)}")
        logger.info(f"✓ Fechas en DimTime: {len(dim_time)}")
        logger.info(f"✓ Mapeos de productos (REGLA 1): {len(product_mapping)}")
        
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
        logger.info("\n[Cargando Dimensiones]")
        loader.load_dim_category(categorias)
        loader.load_dim_channel(canales)
        loader.load_dim_customer(clientes_trans)
        loader.load_dim_time(dim_time)
        loader.load_dim_product(productos_trans)
        
        # Cargar tablas de staging (5 reglas)
        logger.info("\n[Cargando Tablas de Staging - 5 Reglas]")
        
        # REGLA 1: Cargar tabla puente de mapeo
        logger.info("  REGLA 1: Homologación de productos (tabla puente)")
        loader.load_staging_product_mapping(product_mapping)
        
        # REGLA 2: Cargar tipos de cambio
        logger.info("  REGLA 2: Normalización de moneda (tipos de cambio)")
        loader.load_staging_exchange_rates()
        
        # Consideración 5: Cargar trazabilidad
        logger.info("  Consideración 5: Trazabilidad (source_tracking)")
        loader.load_source_tracking('DimCustomer', clientes_trans)
        loader.load_source_tracking('DimProduct', productos_trans)
        
        logger.info("\n" + "=" * 80)
        logger.info("✅ PROCESO ETL COMPLETADO EXITOSAMENTE")
        logger.info("=" * 80)
        logger.info("\n[RESUMEN DE CARGAS]")
        logger.info(f"  ✓ Clientes: {len(clientes_trans)}")
        logger.info(f"  ✓ Productos: {len(productos_trans)}")
        logger.info(f"  ✓ Órdenes: {len(ordenes_trans)}")
        logger.info(f"  ✓ Detalles: {len(detalle_trans)}")
        logger.info(f"  ✓ Mapeos (REGLA 1): {len(product_mapping)}")
        logger.info("\n[REGLAS APLICADAS]")
        logger.info("  ✓ REGLA 1: Homologación de productos (SKU ↔ codigo_alt ↔ codigo_mongo)")
        logger.info("  ✓ REGLA 2: Normalización de moneda (CRC → USD con tabla tipo_cambio)")
        logger.info("  ✓ REGLA 3: Estandarización de género (M/F → Masculino/Femenino)")
        logger.info("  ✓ REGLA 4: Conversión de fechas (VARCHAR → DATE/DATETIME)")
        logger.info("  ✓ REGLA 5: Transformación de totales (string → DECIMAL, validación)")
        logger.info("=" * 80)
        
        return True
    
    except Exception as e:
        logger.error(f"\n❌ ERROR EN PROCESO ETL: {str(e)}")
        logger.exception("Traceback completo:")
        return False


if __name__ == "__main__":
    success = run_etl()
    sys.exit(0 if success else 1)

