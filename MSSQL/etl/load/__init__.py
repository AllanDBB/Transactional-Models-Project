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
            
            # Orden importante: primero FactSales (depende de dimensiones),
            # luego las dimensiones
            order = ['FactSales', 'DimCustomer', 'DimProduct', 'DimChannel', 'DimCategory', 'DimTime']
            tables_ordered = [t for t in order if t in table_names]
            # Agregar tablas que no están en la lista de orden
            tables_ordered += [t for t in table_names if t not in tables_ordered]
            
            for table in tables_ordered:
                try:
                    cursor.execute(f"TRUNCATE TABLE {table}")
                except:
                    # Si no puede truncate, intenta delete (más lento pero funciona)
                    cursor.execute(f"DELETE FROM {table}")
            
            conn.commit()
            cursor.close()
            conn.close()
            logger.info("[OK] Tablas limpiadas exitosamente")
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
                    INSERT INTO staging_map_producto (source_system, source_code, sku_oficial, descripcion, created_at)
                    VALUES ('{row['source_system']}', '{row['source_code']}', '{row['sku_oficial']}', '{row['descripcion']}', 
                            DATEADD(HOUR, -6, GETDATE()))
                """
                cursor.execute(sql)
            
            conn.commit()
            cursor.close()
            conn.close()
            logger.info(f"[OK] {len(df_mapping)} mapeos de productos cargados")
        except Exception as e:
            logger.error(f"Error al cargar mapeo de productos: {str(e)}")
            raise
    
    def load_staging_exchange_rates(self) -> None:
        """
        REGLA 2: Carga tabla de tipos de cambio
        Intenta actualizar la tasa diaria desde BCCR.
        Si falla o no hay datos nuevos, se omite (el histórico ya existe).
        
        En producción, llamar a bccr_integration.ExchangeRateService.load_historical_rates_to_dwh()
        para cargar histórico real desde BCCR
        """
        logger.info("Cargando tipos de cambio (REGLA 2)...")
        
        try:
            # Intentar cargar la tasa diaria del día de hoy desde BCCR
            from bccr_integration import ExchangeRateService
            service = ExchangeRateService(self)
            inserted = service.update_daily_rates()
            logger.info(f"[OK] Tasa diaria procesada (insertadas/actualizadas: {inserted})")
        except Exception as e:
            logger.warning(f"No se pudo actualizar tasa diaria: {str(e)}")
            logger.info("[OK] Continuando con histórico existente (REGLA 2)")
    
    def load_staging_exchange_rates_dataframe(self, df_rates: pd.DataFrame) -> int:
        """
        REGLA 2: Carga tipos de cambio desde DataFrame
        Usado por bccr_integration.ExchangeRateService
        
        Args:
            df_rates: DataFrame con columnas: fecha, de_moneda, a_moneda, tasa, fuente
        
        Returns:
            Cantidad de registros insertados
        """
        logger.info(f"Cargando {len(df_rates)} tipos de cambio desde DataFrame...")
        
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            
            inserted = 0
            for idx, row in df_rates.iterrows():
                try:
                    sql = f"""
                        INSERT INTO staging_tipo_cambio (fecha, de_moneda, a_moneda, tasa, fuente)
                        VALUES ('{row['fecha']}', '{row['de_moneda']}', '{row['a_moneda']}', {row['tasa']}, '{row['fuente']}')
                    """
                    cursor.execute(sql)
                    inserted += 1
                except pyodbc.IntegrityError:
                    # Tipo de cambio ya existe para esa fecha/moneda
                    logger.debug(f"Tasa {row['fecha']} {row['de_moneda']}/{row['a_moneda']} ya existe")
                    pass
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"[OK] {inserted} tipos de cambio cargados/verificados")
            return inserted
        
        except Exception as e:
            logger.error(f"Error al cargar tipos de cambio desde DataFrame: {str(e)}")
            raise
    
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
    
    def load_dim_product(self, df_productos: pd.DataFrame, category_map: Dict = None) -> None:
        """
        Carga DimProduct
        
        Args:
            df_productos: DataFrame con productos transformados
            category_map: Diccionario {nombre_categoria: id} para mapeo
                         Si no se proporciona, intenta obtenerlo de DimCategory
        """
        logger.info(f"Cargando {len(df_productos)} productos...")
        
        df_load = df_productos[['name', 'code', 'categoryId']].copy()
        
        # IMPORTANTE: categoryId contiene nombres de categoría, no IDs
        # Hacer lookup para obtener los IDs de DimCategory
        if category_map is None:
            try:
                conn = pyodbc.connect(self.connection_string)
                cursor = conn.cursor()
                
                # Obtener mapping de nombre -> id desde DimCategory
                cursor.execute("SELECT id, name FROM DimCategory")
                rows = cursor.fetchall()
                category_map = {name: id for id, name in rows}
                cursor.close()
                conn.close()
                
                logger.info(f"[OK] Obtenido mapping de {len(category_map)} categorías")
            except Exception as e:
                logger.warning(f"No se pudo obtener mapping de categorías: {str(e)}")
                category_map = {}
        
        # Mapear nombres a IDs
        if category_map:
            df_load['categoryId'] = df_load['categoryId'].map(category_map)
            df_load['categoryId'] = df_load['categoryId'].fillna(0).astype('Int64')
            logger.info(f"[OK] Mapeados categoryId para productos")
        else:
            logger.warning("[WARN] Cargando productos sin mapeo de categoría (categoryId = NULL)")
            df_load['categoryId'] = None
        
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
                        INSERT INTO staging_source_tracking (source_system, source_key, tabla_destino, id_destino, created_at)
                        VALUES ('{row['source_system']}', '{row['source_key']}', '{tabla}', {row['id']}, 
                                DATEADD(HOUR, -6, GETDATE()))
                    """
                    cursor.execute(sql)
            
            conn.commit()
            cursor.close()
            conn.close()
            logger.info(f"[OK] Tracking para {tabla} cargado")
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
            logger.info(f"[OK] {len(df)} registros cargados en {table_name}")
        
        except Exception as e:
            logger.error(f"Error al cargar {table_name}: {str(e)}")
            raise
    
    @staticmethod
    def _format_value(value) -> str:
        """Formatea valores para SQL"""
        from datetime import datetime, date
        
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
        elif isinstance(value, (datetime, date)):
            # Formato ISO para SQL Server: 'YYYY-MM-DD HH:MM:SS'
            return f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'"
        elif isinstance(value, pd.Timestamp):
            # Pandas Timestamp
            return f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'"
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

