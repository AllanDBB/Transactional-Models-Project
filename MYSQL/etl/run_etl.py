"""
Script principal del ETL: MySQL Transaccional -> MSSQL Data Warehouse
Orquesta los procesos de Extract, Transform y Load
Implementa las 5 reglas de integracion:
1. Homologacion de productos (codigo_alt -> tabla puente)
2. Normalizacion de moneda (CRC -> USD)
3. Estandarizacion de genero (M/F/X -> valores unicos)
4. Conversion de fechas (VARCHAR -> DATE/DATETIME)
5. Transformacion de totales (string con comas/puntos -> decimal)
"""
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'BCCR' / 'src'))

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


def load_exchange_rates(dw_connection_string: str):
    """Carga tipos de cambio desde el DWH para conversion CRC->USD"""
    import pyodbc
    import pandas as pd

    try:
        conn = pyodbc.connect(dw_connection_string)
        query = """
            SELECT fecha, tasa
            FROM staging_tipo_cambio
            WHERE de_moneda = 'CRC' AND a_moneda = 'USD'
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        logging.warning(f"No se pudieron cargar tipos de cambio: {str(e)}")
        return None


def run_etl():
    """Ejecuta el proceso ETL completo con 5 reglas de integracion"""
    logger = setup_logging()
    logger.info("=" * 80)
    logger.info("INICIANDO PROCESO ETL: MySQL -> DWH (5 Reglas de Integracion)")
    logger.info("=" * 80)

    try:
        # ========== EXTRACT ==========
        logger.info("\n[FASE 1] EXTRAYENDO DATOS DE MySQL...")
        extractor = DataExtractor(DatabaseConfig.get_source_connection_params())

        clientes, productos, ordenes, orden_detalle = extractor.extract_all()

        logger.info(f"[OK] Clientes extraidos: {len(clientes)}")
        logger.info(f"[OK] Productos extraidos: {len(productos)}")
        logger.info(f"[OK] Ordenes extraidas: {len(ordenes)}")
        logger.info(f"[OK] Detalles extraidos: {len(orden_detalle)}")

        # ========== TRANSFORM ==========
        logger.info("\n[FASE 2] TRANSFORMANDO DATOS (5 REGLAS)...")

        # Cargar tipos de cambio para REGLA 2
        exchange_rates = load_exchange_rates(DatabaseConfig.get_dw_connection_string())

        transformer = DataTransformer(exchange_rates=exchange_rates)

        # REGLA 3: Estandarizacion de genero (M/F/X -> valores unicos)
        # REGLA 4: Conversion de fechas (VARCHAR -> DATE)
        clientes_trans, track_cli = transformer.transform_clientes(clientes)

        # REGLA 1: Homologacion de productos (codigo_alt -> tabla puente)
        productos_trans, track_prod = transformer.transform_productos(productos)

        # REGLA 2: Normalizacion de moneda (CRC -> USD)
        # REGLA 4: Conversion de fechas (VARCHAR -> DATETIME)
        # REGLA 5: Transformacion de totales (string -> decimal)
        ordenes_trans, track_ord = transformer.transform_ordenes(ordenes)

        # REGLA 5: Transformacion de totales (limpiar comas/puntos)
        detalle_trans, track_det = transformer.transform_orden_detalle(orden_detalle)

        logger.info(f"[OK] Clientes transformados: {len(clientes_trans)}")
        logger.info(f"[OK] Productos transformados: {len(productos_trans)}")
        logger.info(f"[OK] Ordenes transformadas: {len(ordenes_trans)}")
        logger.info(f"[OK] Detalles transformados: {len(detalle_trans)}")

        # Extraer dimensiones
        categorias = transformer.extract_categorias(productos_trans)
        canales = transformer.extract_canales(ordenes_trans)
        dim_time = transformer.generate_dimtime(ordenes_trans)

        # REGLA 1: Construir tabla puente de mapeo
        product_mapping = transformer.build_product_mapping(productos_trans)

        logger.info(f"[OK] Categorias extraidas: {len(categorias)}")
        logger.info(f"[OK] Canales extraidos: {len(canales)}")
        logger.info(f"[OK] Fechas en DimTime: {len(dim_time)}")
        logger.info(f"[OK] Mapeos de productos (REGLA 1): {len(product_mapping)}")

        # ========== LOAD ==========
        logger.info("\n[FASE 3] CARGANDO DATOS AL DWH...")
        loader = DataLoader(DatabaseConfig.get_dw_connection_string())

        # Cargar dimensiones
        logger.info("\n[Cargando Dimensiones]")
        loader.load_dim_category(categorias)
        loader.load_dim_channel(canales)

        # Obtener mapping de categorias
        import pyodbc
        conn = pyodbc.connect(DatabaseConfig.get_dw_connection_string())
        cursor = conn.cursor()
        cursor.execute("SELECT id, name FROM DimCategory")
        category_map = {name: id for id, name in cursor.fetchall()}
        cursor.close()
        conn.close()

        loader.load_dim_customer(clientes_trans)
        loader.load_dim_time(dim_time)
        loader.load_dim_product(productos_trans, category_map)

        # Cargar DimOrder
        logger.info("\n[Cargando DimOrder]")
        loader.load_dim_order(ordenes_trans)

        # Construir y cargar FactSales
        logger.info("\n[Construyendo FactSales]")
        fact_sales = transformer.build_fact_sales(
            detalle_trans,
            ordenes_trans,
            productos_trans,
            clientes_trans,
            DatabaseConfig.get_dw_connection_string()
        )
        loader.load_fact_sales(fact_sales)

        # Cargar tablas de staging (5 reglas)
        logger.info("\n[Cargando Tablas de Staging - 5 Reglas]")

        # REGLA 1: Cargar tabla puente de mapeo
        logger.info("  REGLA 1: Homologacion de productos (tabla puente)")
        loader.load_staging_product_mapping(product_mapping)

        # REGLA 2: Cargar tipos de cambio
        logger.info("  REGLA 2: Normalizacion de moneda (tipos de cambio)")
        loader.load_staging_exchange_rates()

        # Consideracion 5: Cargar trazabilidad
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
        logger.info(f"  [OK] FactSales: {len(fact_sales)}")
        logger.info(f"  [OK] Mapeos (REGLA 1): {len(product_mapping)}")
        logger.info("\n[REGLAS APLICADAS]")
        logger.info("  [OK] REGLA 1: Homologacion de productos (codigo_alt - tabla puente)")
        logger.info("  [OK] REGLA 2: Normalizacion de moneda (CRC -> USD con tabla tipo_cambio)")
        logger.info("  [OK] REGLA 3: Estandarizacion de genero (M/F/X -> Masculino/Femenino/No especificado)")
        logger.info("  [OK] REGLA 4: Conversion de fechas (VARCHAR -> DATE/DATETIME)")
        logger.info("  [OK] REGLA 5: Transformacion de totales (string con comas/puntos -> DECIMAL)")
        logger.info("=" * 80)

        return True

    except Exception as e:
        logger.error(f"\n[ERROR] ERROR EN PROCESO ETL: {str(e)}")
        logger.exception("Traceback completo:")
        return False


if __name__ == "__main__":
    success = run_etl()
    sys.exit(0 if success else 1)
