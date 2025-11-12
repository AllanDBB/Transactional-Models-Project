"""
Módulo Extract: Extrae datos de la base de datos transaccional
"""
import pyodbc
import pandas as pd
from typing import Tuple
import logging

logger = logging.getLogger(__name__)


class DataExtractor:
    """Extrae datos de SalesDB_MSSQL"""
    
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
    
    def extract_clientes(self) -> pd.DataFrame:
        """Extrae tabla Cliente"""
        logger.info("Extrayendo clientes...")
        query = """
            SELECT 
                ClienteId,
                Nombre,
                Email,
                Genero,
                Pais,
                FechaRegistro
            FROM sales_ms.Cliente
        """
        return self._execute_query(query)
    
    def extract_productos(self) -> pd.DataFrame:
        """Extrae tabla Producto"""
        logger.info("Extrayendo productos...")
        query = """
            SELECT 
                ProductoId,
                SKU,
                Nombre,
                Categoria
            FROM sales_ms.Producto
        """
        return self._execute_query(query)
    
    def extract_ordenes(self) -> pd.DataFrame:
        """Extrae tabla Orden"""
        logger.info("Extrayendo órdenes...")
        query = """
            SELECT 
                OrdenId,
                ClienteId,
                Fecha,
                Canal,
                Moneda,
                Total
            FROM sales_ms.Orden
        """
        return self._execute_query(query)
    
    def extract_orden_detalle(self) -> pd.DataFrame:
        """Extrae tabla OrdenDetalle"""
        logger.info("Extrayendo detalles de órdenes...")
        query = """
            SELECT 
                OrdenDetalleId,
                OrdenId,
                ProductoId,
                Cantidad,
                PrecioUnit,
                DescuentoPct
            FROM sales_ms.OrdenDetalle
        """
        return self._execute_query(query)
    
    def _execute_query(self, query: str) -> pd.DataFrame:
        """Ejecuta una query y retorna un DataFrame"""
        try:
            conn = pyodbc.connect(self.connection_string)
            df = pd.read_sql(query, conn)
            conn.close()
            logger.info(f"Consulta exitosa: {len(df)} registros")
            return df
        except Exception as e:
            logger.error(f"Error en extracción: {str(e)}")
            raise
    
    def extract_all(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Extrae todos los datos necesarios"""
        clientes = self.extract_clientes()
        productos = self.extract_productos()
        ordenes = self.extract_ordenes()
        orden_detalle = self.extract_orden_detalle()
        
        return clientes, productos, ordenes, orden_detalle
