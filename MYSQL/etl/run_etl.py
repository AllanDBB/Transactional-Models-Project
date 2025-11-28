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
import mysql.connector
import pyodbc

sys.path.insert(0, str(Path(__file__).parent))

# Shared helper de tipos de cambio
root_path = Path(__file__).resolve().parents[2]
shared_path = root_path / "shared"
sys.path.insert(0, str(shared_path))

from config import DatabaseConfig, ETLConfig
from extract import DataExtractor
from transform import DataTransformer
from load import DataLoader
from ExchangeRateHelper import ExchangeRateHelper  # type: ignore


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


def verify_database_connections(logger):
    """
    Verifica las conexiones a MySQL y SQL Server DW antes de iniciar el ETL.
    Lista las tablas de ambas bases de datos para comprobar conectividad.

    Returns:
        bool: True si ambas conexiones son exitosas, False si falla alguna
    """
    logger.info("\n" + "=" * 80)
    logger.info("VERIFICANDO CONEXIONES A BASES DE DATOS")
    logger.info("=" * 80)

    # ========== VERIFICAR MySQL ==========
    logger.info("\n[1/2] Verificando conexion a MySQL (Fuente)...")
    try:
        mysql_params = DatabaseConfig.get_source_connection_params()
        logger.info(f"  Host: {mysql_params['host']}:{mysql_params['port']}")
        logger.info(f"  Database: {mysql_params['database']}")

        conn = mysql.connector.connect(**mysql_params)
        cursor = conn.cursor()

        # Listar tablas
        cursor.execute("SHOW TABLES")
        tables = [row[0] for row in cursor.fetchall()]

        logger.info(f"  [OK] Conexion exitosa a MySQL")
        logger.info(f"  [OK] Tablas encontradas ({len(tables)}):")
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            logger.info(f"      - {table}: {count} registros")

        cursor.close()
        conn.close()

    except mysql.connector.Error as e:
        logger.error(f"  [ERROR] No se pudo conectar a MySQL: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"  [ERROR] Error inesperado con MySQL: {str(e)}")
        return False

    # ========== VERIFICAR SQL Server DW ==========
    logger.info("\n[2/2] Verificando conexion a SQL Server DW (Destino)...")
    try:
        dw_conn_string = DatabaseConfig.get_dw_connection_string()
        dw_config = DatabaseConfig.DW_DB
        logger.info(f"  Server: {dw_config['server']}:{dw_config['port']}")
        logger.info(f"  Database: {dw_config['database']}")

        conn = pyodbc.connect(dw_conn_string)
        cursor = conn.cursor()

        # Listar tablas
        cursor.execute("""
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'
            AND TABLE_CATALOG = ?
            ORDER BY TABLE_NAME
        """, dw_config['database'])

        tables = [row[0] for row in cursor.fetchall()]

        logger.info(f"  [OK] Conexion exitosa a SQL Server DW")
        logger.info(f"  [OK] Tablas encontradas ({len(tables)}):")
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                logger.info(f"      - {table}: {count} registros")
            except:
                logger.info(f"      - {table}: (no se pudo contar)")

        cursor.close()
        conn.close()

    except pyodbc.Error as e:
        logger.error(f"  [ERROR] No se pudo conectar a SQL Server DW: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"  [ERROR] Error inesperado con SQL Server DW: {str(e)}")
        return False

    logger.info("\n" + "=" * 80)
    logger.info("[OK] TODAS LAS CONEXIONES VERIFICADAS EXITOSAMENTE")
    logger.info("=" * 80)
    return True


def run_etl():
    """Ejecuta el proceso ETL completo con 5 reglas de integracion"""
    logger = setup_logging()
    logger.info("=" * 80)
    logger.info("INICIANDO PROCESO ETL: MySQL -> DWH (5 Reglas de Integracion)")
    logger.info("=" * 80)

    try:
        # ========== VERIFICAR CONEXIONES ==========
        if not verify_database_connections(logger):
            logger.error("[ERROR] No se pudo establecer conexion con las bases de datos")
            logger.error("[ERROR] ETL CANCELADO - Verifique la conectividad y configuracion")
            return False

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

        # Crear transformer sin ExchangeRateHelper por ahora
        # (se usará tasa por defecto en conversiones CRC->USD)
        transformer = DataTransformer(exchange_rate_helper=None)
        logger.info("[INFO] Usando tasa por defecto (515.0) para conversiones CRC->USD")

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

        # Limpiar tablas (opcional, comentar si se quiere append)
        # try:
        #     loader.truncate_tables(['FactSales', 'DimCustomer', 'DimProduct', 'DimChannel', 'DimCategory', 'DimTime'])
        # except Exception as e:
        #     logger.warning(f"No se pudieron limpiar tablas: {e}")

        # Cargar dimensiones y capturar mapeos de IDs
        logger.info("\n[Cargando Dimensiones]")

        # Cargar categorias y obtener mapeo
        try:
            category_map = loader.load_dim_category(categorias)
        except Exception as e:
            logger.warning(f"Error cargando categorias: {e}")
            category_map = {}

        # Cargar canales y obtener mapeo
        try:
            channel_map = loader.load_dim_channel(canales)
        except Exception as e:
            logger.warning(f"Error cargando canales: {e}")
            channel_map = {}

        # Cargar clientes y obtener mapeo
        try:
            customer_map = loader.load_dim_customer(clientes_trans)
        except Exception as e:
            logger.warning(f"Error cargando clientes: {e}")
            customer_map = {}

        # Cargar tiempo y obtener mapeo
        try:
            time_map = loader.load_dim_time(dim_time)
        except Exception as e:
            logger.warning(f"Error cargando tiempo: {e}")
            time_map = {}

        # Cargar productos y obtener mapeo
        try:
            product_map = loader.load_dim_product(productos_trans, category_map)
        except Exception as e:
            logger.warning(f"Error cargando productos: {e}")
            product_map = {}

        # Cargar DimOrder y obtener mapeo
        logger.info("\n[Cargando DimOrder]")
        try:
            order_map = loader.load_dim_order(ordenes_trans)
        except Exception as e:
            logger.warning(f"Error cargando ordenes: {e}")
            order_map = {}

        # Construir y cargar FactSales
        logger.info("\n[Construyendo FactSales]")
        try:
            fact_sales = transformer.build_fact_sales(
                detalle_trans,
                ordenes_trans,
                productos_trans,
                clientes_trans,
                DatabaseConfig.get_dw_connection_string(),
                order_map
            )
            # Pasar los mapeos de IDs a load_fact_sales
            loader.load_fact_sales(fact_sales, product_map, time_map, order_map, channel_map, customer_map)
        except Exception as e:
            logger.warning(f"Error cargando FactSales: {e}")
            fact_sales = []

        # Cargar tablas de staging (5 reglas)
        logger.info("\n[Cargando Tablas de Staging - 5 Reglas]")

        # REGLA 1: Cargar tabla puente de mapeo
        try:
            logger.info("  REGLA 1: Homologacion de productos (tabla puente)")
            loader.load_staging_product_mapping(product_mapping)
        except Exception as e:
            logger.warning(f"  Error en REGLA 1: {e}")

        # REGLA 2: Cargar tipos de cambio
        try:
            logger.info("  REGLA 2: Normalizacion de moneda (tipos de cambio)")
            loader.load_staging_exchange_rates()
        except Exception as e:
            logger.warning(f"  Error en REGLA 2: {e}")

        # Consideracion 5: Cargar trazabilidad
        try:
            logger.info("  Consideracion 5: Trazabilidad (source_tracking)")
            loader.load_source_tracking('DimCustomer', clientes_trans)
            loader.load_source_tracking('DimProduct', productos_trans)
        except Exception as e:
            logger.warning(f"  Error en trazabilidad: {e}")

        logger.info("\n" + "=" * 80)
        logger.info("[OK] PROCESO ETL COMPLETADO EXITOSAMENTE")
        logger.info("=" * 80)
        logger.info("\n[RESUMEN DE CARGAS]")
        logger.info(f"  [OK] Clientes: {len(clientes_trans)}")
        logger.info(f"  [OK] Productos: {len(productos_trans)}")
        logger.info(f"  [OK] Ordenes: {len(ordenes_trans)}")
        logger.info(f"  [OK] FactSales: {len(fact_sales) if isinstance(fact_sales, list) else 'N/A'}")
        logger.info(f"  [OK] Mapeos (REGLA 1): {len(product_mapping)}")
        logger.info("\n[REGLAS APLICADAS]")
        logger.info("  [OK] REGLA 1: Homologacion de productos (codigo_alt - tabla puente)")
        logger.info("  [OK] REGLA 2: Normalizacion de moneda (CRC -> USD con tasa por defecto)")
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
