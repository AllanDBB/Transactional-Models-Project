"""
MySQL transactional database extraction module.

Extracts raw data from MySQL source tables.
Note: Dates and amounts are stored as VARCHAR and will be transformed.
"""
import mysql.connector
import pandas as pd
from typing import Tuple, Dict
import logging

logger = logging.getLogger(__name__)


class DataExtractor:
    """Extracts data from MySQL transactional database."""

    def __init__(self, connection_params: Dict):
        self.connection_params = connection_params

    def extract_clientes(self) -> pd.DataFrame:
        """Extract customer data from MySQL."""
        query = """
            SELECT id, nombre, correo, genero, pais, created_at
            FROM Cliente
            ORDER BY id
        """
        return self._execute_query(query, "customers")

    def extract_productos(self) -> pd.DataFrame:
        """Extract product data from MySQL."""
        query = """
            SELECT id, codigo_alt, nombre, categoria
            FROM Producto
            ORDER BY id
        """
        return self._execute_query(query, "products")

    def extract_ordenes(self) -> pd.DataFrame:
        """Extract order data from MySQL."""
        query = """
            SELECT id, cliente_id, fecha, canal, moneda, total
            FROM Orden
            ORDER BY id
        """
        return self._execute_query(query, "orders")

    def extract_orden_detalle(self) -> pd.DataFrame:
        """Extract order detail line items from MySQL."""
        query = """
            SELECT id, orden_id, producto_id, cantidad, precio_unit
            FROM OrdenDetalle
            ORDER BY id
        """
        return self._execute_query(query, "order details")

    def _execute_query(self, query: str, entity_name: str) -> pd.DataFrame:
        """Execute SQL query and return results as DataFrame."""
        try:
            conn = mysql.connector.connect(**self.connection_params)
            df = pd.read_sql(query, conn)
            conn.close()
            logger.info(f"Successfully extracted {entity_name}: {len(df)} records")
            return df
        except Exception as e:
            logger.error(f"Error extracting {entity_name}: {str(e)}")
            raise

    def extract_all(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Extract all required tables from MySQL source."""
        clientes = self.extract_clientes()
        productos = self.extract_productos()
        ordenes = self.extract_ordenes()
        orden_detalle = self.extract_orden_detalle()

        logger.info(f"Extraction completed: {len(clientes)} customers, {len(productos)} products, "
                   f"{len(ordenes)} orders, {len(orden_detalle)} details")

        return clientes, productos, ordenes, orden_detalle
