"""
Modulo Extract: Extrae datos de la base de datos transaccional MySQL
Heterogeneidades específicas de MySQL:
- codigo_alt: Código alternativo (NO es el SKU oficial)
- Fechas como VARCHAR: 'YYYY-MM-DD' para created_at, 'YYYY-MM-DD HH:MM:SS' para fecha
- Montos como VARCHAR: Pueden tener comas o puntos ('1,200.50' o '1200.50')
- Género ENUM('M','F','X'): Diferente a MSSQL
- Moneda mezclada: USD o CRC
- SIN campo descuento en OrdenDetalle
"""
import mysql.connector
import pandas as pd
from typing import Tuple, Dict
import logging

logger = logging.getLogger(__name__)


class DataExtractor:
    """Extrae datos de MySQL sales_mysql"""

    def __init__(self, connection_params: Dict):
        self.connection_params = connection_params

    def extract_clientes(self) -> pd.DataFrame:
        """
        Extrae tabla Cliente

        Campos:
        - id: INT (PK)
        - nombre: VARCHAR(120)
        - correo: VARCHAR(150)
        - genero: ENUM('M','F','X')
        - pais: VARCHAR(60)
        - created_at: VARCHAR(10) formato 'YYYY-MM-DD'
        """
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
            ORDER BY id
        """
        return self._execute_query(query)

    def extract_productos(self) -> pd.DataFrame:
        """
        Extrae tabla Producto

        Campos:
        - id: INT (PK)
        - codigo_alt: VARCHAR(64) UNIQUE - Código alternativo (NO es SKU oficial)
        - nombre: VARCHAR(150)
        - categoria: VARCHAR(80)
        """
        logger.info("Extrayendo productos...")
        query = """
            SELECT
                id,
                codigo_alt,
                nombre,
                categoria
            FROM Producto
            ORDER BY id
        """
        return self._execute_query(query)

    def extract_ordenes(self) -> pd.DataFrame:
        """
        Extrae tabla Orden

        Campos:
        - id: INT (PK)
        - cliente_id: INT (FK)
        - fecha: VARCHAR(19) formato 'YYYY-MM-DD HH:MM:SS'
        - canal: VARCHAR(20) libre (sin restricción)
        - moneda: CHAR(3) 'USD' o 'CRC'
        - total: VARCHAR(20) pueden tener comas/puntos
        """
        logger.info("Extrayendo órdenes...")
        query = """
            SELECT
                id,
                cliente_id,
                fecha,
                canal,
                moneda,
                total
            FROM Orden
            ORDER BY id
        """
        return self._execute_query(query)

    def extract_orden_detalle(self) -> pd.DataFrame:
        """
        Extrae tabla OrdenDetalle

        Campos:
        - id: INT (PK)
        - orden_id: INT (FK)
        - producto_id: INT (FK)
        - cantidad: INT
        - precio_unit: VARCHAR(20) pueden tener comas/puntos

        NOTA: NO tiene campo descuento
        """
        logger.info("Extrayendo detalles de órdenes...")
        query = """
            SELECT
                id,
                orden_id,
                producto_id,
                cantidad,
                precio_unit
            FROM OrdenDetalle
            ORDER BY id
        """
        return self._execute_query(query)

    def _execute_query(self, query: str) -> pd.DataFrame:
        """Ejecuta una query y retorna un DataFrame"""
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
        """
        Extrae todos los datos necesarios de MySQL

        Returns:
            Tuple: (clientes, productos, ordenes, orden_detalle) all as pandas DataFrames
        """
        clientes = self.extract_clientes()
        productos = self.extract_productos()
        ordenes = self.extract_ordenes()
        orden_detalle = self.extract_orden_detalle()

        logger.info(f"Extracción completada: {len(clientes)} clientes, {len(productos)} productos")
        logger.info(f"  {len(ordenes)} órdenes, {len(orden_detalle)} detalles")

        return clientes, productos, ordenes, orden_detalle
