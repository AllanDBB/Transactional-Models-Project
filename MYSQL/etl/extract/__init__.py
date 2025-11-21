"""
Modulo Extract: Extrae datos de la base de datos transaccional MySQL
Heterogeneidades:
- codigo_alt (no es el SKU oficial)
- Fechas y montos como texto (requiere limpieza)
- Genero M/F/X
- Moneda mezclada USD/CRC
"""
import mysql.connector
import pandas as pd
from typing import Tuple, Dict
import logging

logger = logging.getLogger(__name__)


class DataExtractor:
    """Extrae datos de MySQL transactional_db"""

    def __init__(self, connection_params: Dict):
        self.connection_params = connection_params

    def extract_clientes(self) -> pd.DataFrame:
        """Extrae tabla Cliente"""
        logger.info("Extrayendo clientes...")
        query = """
            SELECT
                id,
                nombre,
                correo,
                genero,
                pais,
                created_at
            FROM Cliente
        """
        return self._execute_query(query)

    def extract_productos(self) -> pd.DataFrame:
        """Extrae tabla Producto"""
        logger.info("Extrayendo productos...")
        query = """
            SELECT
                id,
                codigo_alt,
                nombre,
                categoria
            FROM Producto
        """
        return self._execute_query(query)

    def extract_ordenes(self) -> pd.DataFrame:
        """Extrae tabla Orden"""
        logger.info("Extrayendo ordenes...")
        query = """
            SELECT
                id,
                cliente_id,
                fecha,
                canal,
                moneda,
                total
            FROM Orden
        """
        return self._execute_query(query)

    def extract_orden_detalle(self) -> pd.DataFrame:
        """Extrae tabla OrdenDetalle"""
        logger.info("Extrayendo detalles de ordenes...")
        query = """
            SELECT
                id,
                orden_id,
                producto_id,
                cantidad,
                precio_unit
            FROM OrdenDetalle
        """
        return self._execute_query(query)

    def _execute_query(self, query: str) -> pd.DataFrame:
        """Ejecuta una query y retorna un DataFrame"""
        try:
            conn = mysql.connector.connect(**self.connection_params)
            df = pd.read_sql(query, conn)
            conn.close()
            logger.info(f"Consulta exitosa: {len(df)} registros")
            return df
        except Exception as e:
            logger.error(f"Error en extraccion: {str(e)}")
            raise

    def extract_all(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Extrae todos los datos necesarios"""
        clientes = self.extract_clientes()
        productos = self.extract_productos()
        ordenes = self.extract_ordenes()
        orden_detalle = self.extract_orden_detalle()

        return clientes, productos, ordenes, orden_detalle
