"""
Main ETL orchestration: MySQL Transactional -> MSSQL Data Warehouse

Implements 5 integration rules:
1. Product homologation (codigo_alt -> official SKU mapping)
2. Currency normalization (CRC -> USD conversion)
3. Gender standardization (M/F/X -> standardized values)
4. Date conversion (VARCHAR -> DATE/DATETIME)
5. Amount transformation (string with formatting -> decimal)

Idempotent behavior: Can be run multiple times safely without duplicating data.
"""
import logging
import sys
from pathlib import Path
import mysql.connector
import pyodbc
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))

# Shared utilities
root_path = Path(__file__).resolve().parents[1]
shared_path = root_path / "shared"
sys.path.insert(0, str(shared_path))

from config import DatabaseConfig, ETLConfig
from extract import DataExtractor
from transform import DataTransformer
from load import DataLoader

try:
    from ExchangeRateHelper import ExchangeRateHelper
except (ImportError, ModuleNotFoundError):
    ExchangeRateHelper = None


def setup_logging():
    """Configure logging to file and console."""
    logging.basicConfig(
        level=getattr(logging, ETLConfig.LOG_LEVEL),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(ETLConfig.LOG_FILE),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


def verify_connections(logger) -> bool:
    """
    Verify connectivity to MySQL source and MSSQL DW.

    Returns True if both connections successful, False otherwise.
    """
    logger.info("\n" + "=" * 80)
    logger.info("VERIFYING DATABASE CONNECTIONS")
    logger.info("=" * 80)

    # Check MySQL
    logger.info("\n[MySQL] Checking transactional source...")
    try:
        mysql_params = DatabaseConfig.get_source_connection_params()
        logger.info(f"  Host: {mysql_params['host']}:{mysql_params['port']}")
        logger.info(f"  Database: {mysql_params['database']}")

        # Use pandas.read_sql to avoid cursor management
        conn = mysql.connector.connect(**mysql_params)
        try:
            tables_df = pd.read_sql("SHOW TABLES", conn)
            tables = tables_df.iloc[:, 0].tolist()

            logger.info(f"  Tables found: {len(tables)}")
            for table in sorted(tables):
                count_df = pd.read_sql(f"SELECT COUNT(*) as cnt FROM {table}", conn)
                count = count_df.iloc[0, 0]
                logger.info(f"    - {table}: {count} rows")
        finally:
            conn.close()

        logger.info("  Connection: SUCCESS")

    except Exception as e:
        logger.error(f"  Connection FAILED: {str(e)}")
        return False

    # Check MSSQL DW
    logger.info("\n[MSSQL] Checking data warehouse...")
    try:
        dw_config = DatabaseConfig.DW_DB
        logger.info(f"  Server: {dw_config['server']}:{dw_config['port']}")
        logger.info(f"  Database: {dw_config['database']}")

        with pyodbc.connect(DatabaseConfig.get_dw_connection_string()) as conn:
            tables = [row[0] for row in conn.execute("""
                SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_CATALOG = ?
                ORDER BY TABLE_NAME
            """, dw_config['database']).fetchall()]

            logger.info(f"  Tables found: {len(tables)}")
            for table in sorted(tables):
                try:
                    count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                    logger.info(f"    - {table}: {count} rows")
                except:
                    logger.info(f"    - {table}: (unable to count)")

        logger.info("  Connection: SUCCESS")

    except Exception as e:
        logger.error(f"  Connection FAILED: {str(e)}")
        return False

    logger.info("\n" + "=" * 80)
    return True


def run_etl():
    """Execute complete ETL pipeline."""
    logger = setup_logging()

    logger.info("\n" + "=" * 80)
    logger.info("MYSQL ETL PIPELINE - 5 INTEGRATION RULES")
    logger.info("=" * 80)

    try:
        # ====================================================================
        # PHASE 1: VERIFY CONNECTIVITY
        # ====================================================================
        if not verify_connections(logger):
            logger.error("Database connection verification failed - aborting ETL")
            return False

        # ====================================================================
        # PHASE 2: EXTRACT
        # ====================================================================
        logger.info("\n[PHASE 1/4] EXTRACTING DATA FROM MYSQL...")
        extractor = DataExtractor(DatabaseConfig.get_source_connection_params())
        clientes, productos, ordenes, orden_detalle = extractor.extract_all()

        logger.info(f"Extracted: {len(clientes)} customers, {len(productos)} products, "
                   f"{len(ordenes)} orders, {len(orden_detalle)} details")

        # ====================================================================
        # PHASE 3: INITIALIZE TRANSFORMER WITH EXCHANGE RATES
        # ====================================================================
        logger.info("\n[PHASE 2/4] INITIALIZING TRANSFORMER...")

        exchange_helper = None
        try:
            exchange_helper = ExchangeRateHelper(DatabaseConfig.get_dw_connection_string())
            if exchange_helper.conectar():
                count = exchange_helper.conn.execute(
                    "SELECT COUNT(*) FROM DimExchangeRate WHERE fromCurrency = 'CRC' AND toCurrency = 'USD'"
                ).fetchone()[0]

                if count > 0:
                    logger.info(f"Exchange rates enabled: {count} CRC->USD rates available")
                else:
                    logger.warning("No exchange rates found - will use default rate (515.0)")
                    exchange_helper.cerrar()
                    exchange_helper = None
            else:
                logger.warning("Could not connect to DWH for exchange rates - using default")
                exchange_helper = None
        except Exception as e:
            logger.warning(f"Exchange rate initialization failed: {e} - using default rate")
            exchange_helper = None

        # ====================================================================
        # PHASE 4: TRANSFORM
        # ====================================================================
        logger.info("\n[PHASE 2/4] TRANSFORMING DATA (5 INTEGRATION RULES)...")
        transformer = DataTransformer(exchange_rate_helper=exchange_helper)

        # Rule 3: Gender standardization, Rule 4: Date parsing
        clientes_trans, track_cli = transformer.transform_clientes(clientes)

        # Rule 1: Product homologation
        productos_trans, track_prod = transformer.transform_productos(productos)

        # Rule 2: Currency conversion, Rule 4: Date conversion, Rule 5: Amount formatting
        ordenes_trans, track_ord = transformer.transform_ordenes(ordenes)

        # Rule 2: Currency conversion for line items, Rule 5: Amount formatting
        detalle_trans, track_det = transformer.transform_orden_detalle(orden_detalle, ordenes_trans)

        logger.info(f"Transformed: {len(clientes_trans)} customers, {len(productos_trans)} products, "
                   f"{len(ordenes_trans)} orders, {len(detalle_trans)} details")

        # Extract dimension data
        categorias = transformer.extract_categorias(productos_trans)
        canales = transformer.extract_canales(ordenes_trans)
        dim_time = transformer.generate_dimtime(ordenes_trans)
        product_mapping = transformer.build_product_mapping(productos_trans)

        logger.info(f"Dimensions: {len(categorias)} categories, {len(canales)} channels, "
                   f"{len(dim_time)} dates, {len(product_mapping)} product mappings")

        # Build fact table
        fact_sales = transformer.build_fact_sales(
            detalle_trans, ordenes_trans, productos_trans, clientes_trans,
            DatabaseConfig.get_dw_connection_string(), {}
        )

        # ====================================================================
        # PHASE 5: LOAD
        # ====================================================================
        logger.info("\n[PHASE 3/4] LOADING DATA TO DWH...")
        loader = DataLoader(DatabaseConfig.get_dw_connection_string())

        # Uncomment next line to clean all tables before loading (fresh load)
        # loader.truncate_tables(['FactSales', 'DimProduct', 'DimCustomer', 'DimChannel', 'DimCategory', 'DimTime'])

        logger.info("\nLoading dimensions...")
        category_map = loader.load_dim_category(categorias)
        channel_map = loader.load_dim_channel(canales)
        customer_map = loader.load_dim_customer(clientes_trans)
        time_map = loader.load_dim_time(dim_time)
        product_map = loader.load_dim_product(productos_trans, category_map)
        order_map = loader.load_dim_order(ordenes_trans)

        logger.info("\nLoading fact table...")
        fact_count = loader.load_fact_sales(fact_sales, product_map, time_map, order_map, channel_map, customer_map)

        logger.info("\nLoading staging tables...")
        loader.load_staging_product_mapping(product_mapping)
        loader.load_staging_exchange_rates()

        # ====================================================================
        # PHASE 6: SUMMARY
        # ====================================================================
        logger.info("\n" + "=" * 80)
        logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)

        logger.info("\nLoad Summary:")
        logger.info(f"  Customers: {len(customer_map)} loaded")
        logger.info(f"  Products: {len(product_map)} loaded")
        logger.info(f"  Orders: {len(order_map)} loaded")
        logger.info(f"  Fact Sales: {fact_count} rows loaded")
        logger.info(f"  Categories: {len(category_map)} loaded")
        logger.info(f"  Channels: {len(channel_map)} loaded")

        logger.info("\nIntegration Rules Applied:")
        logger.info("  Rule 1: Product homologation (codigo_alt -> SKU mapping table)")
        logger.info("  Rule 2: Currency normalization (CRC -> USD with exchange rates)")
        logger.info("  Rule 3: Gender standardization (M/F/X -> Male/Female/Unspecified)")
        logger.info("  Rule 4: Date conversion (VARCHAR -> DATE/DATETIME)")
        logger.info("  Rule 5: Amount transformation (string with commas/dots -> DECIMAL)")

        logger.info("\nIdempotent Features:")
        logger.info("  - Dimensions check for existing records before inserting")
        logger.info("  - Fact table validates foreign keys before loading")
        logger.info("  - Missing references are logged and skipped")
        logger.info("  - Can be safely re-run without duplicating data")

        logger.info("=" * 80)

        if exchange_helper:
            exchange_helper.cerrar()

        return True

    except Exception as e:
        logger.error(f"\nETL FAILED: {str(e)}")
        logger.exception("Full traceback:")
        if exchange_helper:
            exchange_helper.cerrar()
        return False


if __name__ == "__main__":
    success = run_etl()
    sys.exit(0 if success else 1)
