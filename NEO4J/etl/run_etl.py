"""
Orquestador del ETL Neo4j -> DWH MSSQL aplicando las 5 reglas.
"""
import logging
import sys
from pathlib import Path

from config import ETLConfig, DWConfig
from extract.extract_data import DataExtractor
from load.load_data import DataLoader
from transform.transform_data import DataTransformer

# Shared helper de tipos de cambio
root_path = Path(__file__).resolve().parents[2]
shared_path = root_path / "shared"
sys.path.insert(0, str(shared_path))
from ExchangeRateHelper import ExchangeRateHelper  # type: ignore


def setup_logging():
    logging.basicConfig(
        level=getattr(logging, ETLConfig.LOG_LEVEL),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(ETLConfig.LOG_FILE), logging.StreamHandler()],
    )
    return logging.getLogger(__name__)


def run_etl() -> bool:
    logger = setup_logging()
    logger.info("=" * 80)
    logger.info("INICIANDO ETL: Neo4j -> MSSQL_DW")
    logger.info("=" * 80)

    try:
        extractor = DataExtractor()
        exchange_helper = ExchangeRateHelper(DWConfig.connection_string())
        transformer = DataTransformer(exchange_helper)
        loader = DataLoader(DWConfig.connection_string())

        clientes_raw, productos_raw, ordenes_raw, detalle_raw = extractor.extract()

        clientes, _ = transformer.transform_clientes(clientes_raw)
        productos, _ = transformer.transform_productos(productos_raw)
        ordenes, _ = transformer.transform_ordenes(ordenes_raw)
        detalle, _ = transformer.transform_orden_detalle(detalle_raw)

        categorias = transformer.extract_categorias(productos)
        canales = transformer.extract_canales(ordenes)
        dim_time = transformer.generate_dimtime(ordenes)
        product_mapping = transformer.build_product_mapping(productos)

        logger.info("[Cargando Dimensiones]")
        loader.load_dim_category(categorias)
        loader.load_dim_channel(canales)

        import pyodbc

        conn = pyodbc.connect(DWConfig.connection_string())
        cursor = conn.cursor()
        cursor.execute("SELECT id, name FROM DimCategory")
        category_map = {name: id for id, name in cursor.fetchall()}
        cursor.close()
        conn.close()

        loader.load_dim_customer(clientes)
        loader.load_dim_time(dim_time)
        loader.load_dim_product(productos, category_map)

        logger.info("[Cargando DimOrder]")
        loader.load_dim_order(ordenes)

        fact_sales = transformer.build_fact_sales(
            detalle, ordenes, productos, clientes, DWConfig.connection_string()
        )
        loader.load_fact_sales(fact_sales)

        logger.info("[Cargando Staging]")
        loader.load_staging_product_mapping(product_mapping)
        loader.load_source_tracking("DimCustomer", clientes)
        loader.load_source_tracking("DimProduct", productos)

        extractor.close()
        logger.info("[OK] ETL Neo4j completado")
        return True
    except Exception as exc:  # pragma: no cover - logging de error
        logger.exception("Error ejecutando ETL Neo4j: %s", exc)
        return False


if __name__ == "__main__":
    sys.exit(0 if run_etl() else 1)
