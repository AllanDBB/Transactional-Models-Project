"""
MySQL data extraction module.

Key differences from MSSQL DW:
- codigo_alt: Alternative code, not the official SKU
- Dates stored as VARCHAR
- Amounts stored as VARCHAR with varying formats
- Gender as ENUM('M','F','X')
- Mixed currency (USD/CRC)
- No discount field in OrdenDetalle
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
        logger.info("Extrayendo clientes...")
        query = """
            SELECT id, nombre, correo, genero, pais, created_at
            FROM Cliente
            ORDER BY id
        """
        return self._execute_query(query)

    def extract_productos(self) -> pd.DataFrame:
        logger.info("Extrayendo productos...")
        query = """
            SELECT id, codigo_alt, nombre, categoria
            FROM Producto
            ORDER BY id
        """
        return self._execute_query(query)

    def extract_ordenes(self) -> pd.DataFrame:
        logger.info("Extrayendo ordenes...")
        query = """
            SELECT id, cliente_id, fecha, canal, moneda, total
            FROM Orden
            ORDER BY id
        """
        return self._execute_query(query)

    def extract_orden_detalle(self) -> pd.DataFrame:
        logger.info("Extrayendo detalles de ordenes...")
        query = """
            SELECT id, orden_id, producto_id, cantidad, precio_unit
            FROM OrdenDetalle
            ORDER BY id
        """
        return self._execute_query(query)

    def _execute_query(self, query: str) -> pd.DataFrame:
        try:
            conn = mysql.connector.connect(**self.connection_params)
            df = pd.read_sql(query, conn)
            conn.close()
            logger.info(f"Consulta exitosa: {len(df)} registros extraidos")
            return df
        except Exception as e:
            logger.error(f"Error en extraccion: {str(e)}")
            raise

    def extract_all(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Extracts all required data from MySQL."""
        clientes = self.extract_clientes()
        productos = self.extract_productos()
        ordenes = self.extract_ordenes()
        orden_detalle = self.extract_orden_detalle()

        logger.info(f"Extracción completada: {len(clientes)} clientes, {len(productos)} productos")
        logger.info(f"  {len(ordenes)} órdenes, {len(orden_detalle)} detalles")

        return clientes, productos, ordenes, orden_detalle
