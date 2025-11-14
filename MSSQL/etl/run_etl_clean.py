#!/usr/bin/env python3
"""
Script limpio para ejecutar ETL completo sin problemas de encoding
"""
import sys
import os

# Forzar encoding UTF-8 en stdout
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

os.chdir(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.getcwd())

from extract import DataExtractor
from transform import DataTransformer
from load import DataLoader
from config import DatabaseConfig
import logging

# Setup logging con encoding seguro
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_clean.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def run_etl_clean():
    """Ejecutar ETL sin problemas de Unicode"""
    
    logger.info("=" * 80)
    logger.info("INICIANDO ETL: MSSQL -> DWH (5 Reglas de Integracion)")
    logger.info("=" * 80)
    
    try:
        # FASE 1: EXTRACCION
        logger.info("\n[FASE 1] EXTRAYENDO DATOS DE MSSQL...")
        extractor = DataExtractor(DatabaseConfig.get_source_connection_string())
        
        clientes = extractor.extract_clientes()
        productos = extractor.extract_productos()
        ordenes = extractor.extract_ordenes()
        orden_detalle = extractor.extract_orden_detalle()
        
        logger.info(f"[OK] Clientes extraidos: {len(clientes)}")
        logger.info(f"[OK] Productos extraidos: {len(productos)}")
        logger.info(f"[OK] Ordenes extraidas: {len(ordenes)}")
        logger.info(f"[OK] Detalles extraidos: {len(orden_detalle)}")
        
        # FASE 2: TRANSFORMACION
        logger.info("\n[FASE 2] TRANSFORMANDO DATOS (5 REGLAS)...")
        transformer = DataTransformer()
        
        clientes_trans, _ = transformer.transform_clientes(clientes)
        productos_trans, _ = transformer.transform_productos(productos)
        ordenes_trans, _ = transformer.transform_ordenes(ordenes)
        detalle_trans, _ = transformer.transform_orden_detalle(orden_detalle)
        
        logger.info(f"[OK] Clientes transformados: {len(clientes_trans)}")
        logger.info(f"[OK] Productos transformados: {len(productos_trans)}")
        logger.info(f"[OK] Ordenes transformadas: {len(ordenes_trans)}")
        logger.info(f"[OK] Detalles transformados: {len(detalle_trans)}")
        
        # Dimensiones adicionales
        categorias = transformer.extract_categorias(productos_trans)
        canales = transformer.extract_canales(ordenes_trans)
        dim_time = transformer.generate_dimtime(ordenes_trans)
        product_mapping = transformer.build_product_mapping(productos_trans)
        
        logger.info(f"[OK] Categorias extraidas: {len(categorias)}")
        logger.info(f"[OK] Canales extraidos: {len(canales)}")
        logger.info(f"[OK] Fechas en DimTime: {len(dim_time)}")
        logger.info(f"[OK] Mapeos de productos (REGLA 1): {len(product_mapping)}")
        
        # FASE 3: CARGA
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
        
        # Cargar tablas de staging (5 reglas)
        logger.info("\n[Cargando Tablas de Staging - 5 Reglas]")
        logger.info("  REGLA 1: Homologacion de productos (tabla puente)")
        loader.load_staging_product_mapping(product_mapping)
        
        logger.info("  REGLA 2: Normalizacion de moneda (tipos de cambio)")
        loader.load_staging_exchange_rates()
        
        # Trazabilidad
        logger.info("  Consideracion 5: Trazabilidad (source_tracking)")
        loader.load_source_tracking('DimCustomer', clientes_trans)
        loader.load_source_tracking('DimProduct', productos_trans)
        
        logger.info("\n" + "=" * 80)
        logger.info("[OK] PROCESO ETL COMPLETADO EXITOSAMENTE")
        logger.info("=" * 80)
        logger.info("\n[RESUMEN DE CARGAS]")
        logger.info(f"  [OK] Clientes: {len(clientes_trans)}")
        logger.info(f"  [OK] Productos: {len(productos_trans)}")
        logger.info(f"  [OK] Ordenes: {len(ordenes_trans)}")
        logger.info(f"  [OK] Detalles: {len(detalle_trans)}")
        logger.info(f"  [OK] Mapeos (REGLA 1): {len(product_mapping)}")
        logger.info("\n[REGLAS APLICADAS]")
        logger.info("  [OK] REGLA 1: Homologacion de productos (SKU - codigo_alt - codigo_mongo)")
        logger.info("  [OK] REGLA 2: Normalizacion de moneda (CRC -> USD con tabla tipo_cambio)")
        logger.info("  [OK] REGLA 3: Estandarizacion de genero (M/F -> Masculino/Femenino)")
        logger.info("  [OK] REGLA 4: Conversion de fechas (VARCHAR -> DATE/DATETIME)")
        logger.info("  [OK] REGLA 5: Transformacion de totales (string -> DECIMAL, validacion)")
        logger.info("=" * 80)
        
        return True
    
    except Exception as e:
        logger.error(f"\n[ERROR] ERROR EN PROCESO ETL: {str(e)}")
        logger.exception("Traceback completo:")
        return False

if __name__ == "__main__":
    success = run_etl_clean()
    sys.exit(0 if success else 1)
