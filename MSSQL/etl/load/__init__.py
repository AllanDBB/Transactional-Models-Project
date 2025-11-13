"""
Módulo Load: Carga datos transformados al Data Warehouse
Incluye carga en tablas de staging para trazabilidad y mapeos
"""
import pyodbc
import pandas as pd
import logging
from typing import Dict, List

logger = logging.getLogger(__name__)


class DataLoader:
    """Carga datos en el Data Warehouse MSSQL con trazabilidad"""
    
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
    
    def truncate_tables(self, table_names: List[str]) -> None:
        """Limpia las tablas (TRUNCATE) antes de cargar"""
        logger.info(f"Limpiando tablas: {', '.join(table_names)}")
        
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            
            for table in table_names:
                cursor.execute(f"TRUNCATE TABLE {table}")
            
            conn.commit()
            cursor.close()
            conn.close()
            logger.info("✓ Tablas limpiadas exitosamente")
        except Exception as e:
            logger.error(f"Error al limpiar tablas: {str(e)}")
            raise
    
    def load_staging_product_mapping(self, df_mapping: pd.DataFrame) -> None:
        """REGLA 1: Carga tabla puente de mapeo de productos"""
        logger.info(f"Cargando mapeo de {len(df_mapping)} productos...")
        
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            
            for idx, row in df_mapping.iterrows():
                sql = f"""
                    INSERT INTO staging_map_producto (source_system, source_code, sku_oficial, descripcion)
                    VALUES ('{row['source_system']}', '{row['source_code']}', '{row['sku_oficial']}', '{row['descripcion']}')
                """
                cursor.execute(sql)
            
            conn.commit()
            cursor.close()
            conn.close()
            logger.info(f"✓ {len(df_mapping)} mapeos de productos cargados")
        except Exception as e:
            logger.error(f"Error al cargar mapeo de productos: {str(e)}")
            raise
    
    def load_staging_exchange_rates(self) -> None:
        """REGLA 2: Carga tabla de tipos de cambio (históricamente USD=1.0 para MSSQL)"""
        logger.info("Cargando tipos de cambio...")
        
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            
            # Para MSSQL: insertar USD=1.0 (no requiere conversión)
            sql = """
                INSERT INTO staging_tipo_cambio (fecha, de_moneda, a_moneda, tasa, fuente)
                VALUES (GETDATE(), 'USD', 'USD', 1.0, 'MSSQL_NATIVE')
            """
            cursor.execute(sql)
            
            conn.commit()
            cursor.close()
            conn.close()
            logger.info("✓ Tipos de cambio cargados")
        except Exception as e:
            logger.error(f"Error al cargar tipos de cambio: {str(e)}")
    
    def load_dim_category(self, df_categorias: pd.DataFrame) -> None:
        """Carga DimCategory"""
        logger.info(f"Cargando {len(df_categorias)} categorías...")
        self._load_dataframe(df_categorias, 'DimCategory', ['name'])
    
    def load_dim_channel(self, df_canales: pd.DataFrame) -> None:
        """Carga DimChannel"""
        logger.info(f"Cargando {len(df_canales)} canales...")
        
        df_canales = df_canales.copy()
        df_canales['channelType'] = df_canales['name'].map(self._map_channel_type)
        
        self._load_dataframe(df_canales, 'DimChannel', ['name', 'channelType'])
    
    def load_dim_product(self, df_productos: pd.DataFrame) -> None:
        """Carga DimProduct"""
        logger.info(f"Cargando {len(df_productos)} productos...")
        # Filtrar solo columnas necesarias (sin source_system, source_key)
        df_load = df_productos[['name', 'code', 'categoryId']].copy()
        self._load_dataframe(df_load, 'DimProduct', ['name', 'code', 'categoryId'])
    
    def load_dim_customer(self, df_clientes: pd.DataFrame) -> None:
        """Carga DimCustomer"""
        logger.info(f"Cargando {len(df_clientes)} clientes...")
        
        df_clientes = df_clientes.copy()
        df_clientes['created_at'] = pd.to_datetime(df_clientes['created_at'])
        
        df_load = df_clientes[['name', 'email', 'gender', 'country', 'created_at']].copy()
        self._load_dataframe(df_load, 'DimCustomer', 
                           ['name', 'email', 'gender', 'country', 'created_at'])
    
    def load_dim_time(self, df_dimtime: pd.DataFrame) -> None:
        """Carga DimTime"""
        logger.info(f"Cargando {len(df_dimtime)} fechas...")
        
        df = df_dimtime.copy()
        df['date'] = pd.to_datetime(df['date']).dt.date
        
        self._load_dataframe(df, 'DimTime', 
                           ['id', 'year', 'month', 'day', 'date', 'exchangeRateToUSD'],
                           identity=False)
    
    def load_dim_order(self, df_ordenes: pd.DataFrame) -> None:
        """Carga DimOrder"""
        logger.info(f"Cargando {len(df_ordenes)} órdenes...")
        
        df_dim_order = df_ordenes[['totalOrderUSD']].copy()
        
        self._load_dataframe(df_dim_order, 'DimOrder', ['totalOrderUSD'], identity=False)
    
    def load_fact_sales(self, df_fact_sales: pd.DataFrame) -> None:
        """Carga FactSales"""
        logger.info(f"Cargando {len(df_fact_sales)} registros de ventas...")
        
        df = df_fact_sales.copy()
        df['created_at'] = pd.to_datetime(df['created_at'])
        
        columns = ['productId', 'timeId', 'orderId', 'channelId', 'customerId',
                  'productCant', 'productUnitPriceUSD', 'lineTotalUSD',
                  'discountPercentage', 'created_at', 'exchangeRateId']
        
        self._load_dataframe(df, 'FactSales', columns)
    
    def load_source_tracking(self, tabla: str, df_source: pd.DataFrame) -> None:
        """Carga tracking de registros para trazabilidad (Consideración 5)"""
        logger.info(f"Cargando tracking para {tabla}...")
        
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            
            for idx, row in df_source.iterrows():
                if 'source_system' in row and 'source_key' in row:
                    sql = f"""
                        INSERT INTO staging_source_tracking (source_system, source_key, tabla_destino, id_destino)
                        VALUES ('{row['source_system']}', '{row['source_key']}', '{tabla}', {row['id']})
                    """
                    cursor.execute(sql)
            
            conn.commit()
            cursor.close()
            conn.close()
            logger.info(f"✓ Tracking para {tabla} cargado")
        except Exception as e:
            logger.error(f"Error al cargar tracking: {str(e)}")
    
    def _load_dataframe(self, df: pd.DataFrame, table_name: str, 
                       columns: List[str], identity: bool = True) -> None:
        """Carga un DataFrame a una tabla"""
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            
            for idx, row in df.iterrows():
                values = ', '.join([self._format_value(row[col]) for col in columns])
                cols_str = ', '.join(columns)
                
                sql = f"INSERT INTO {table_name} ({cols_str}) VALUES ({values})"
                
                cursor.execute(sql)
            
            conn.commit()
            cursor.close()
            conn.close()
            logger.info(f"✓ {len(df)} registros cargados en {table_name}")
        
        except Exception as e:
            logger.error(f"Error al cargar {table_name}: {str(e)}")
            raise
    
    @staticmethod
    def _format_value(value) -> str:
        """Formatea valores para SQL"""
        if pd.isna(value):
            return "NULL"
        elif isinstance(value, str):
            # Escapar comillas simples
            escaped = value.replace("'", "''")
            return f"'{escaped}'"
        elif isinstance(value, bool):
            return "1" if value else "0"
        elif isinstance(value, (int, float)):
            return str(value)
        else:
            escaped = str(value).replace("'", "''")
            return f"'{escaped}'"
    
    @staticmethod
    def _map_channel_type(channel_name: str) -> str:
        """Mapea nombre de canal a tipo"""
        channel_map = {
            'WEB': 'Website',
            'TIENDA': 'Store',
            'APP': 'App'
        }
        return channel_map.get(channel_name, 'Other')

