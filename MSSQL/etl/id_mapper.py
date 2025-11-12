"""
Módulo de mapeo de IDs: Mapea IDs de tablas transaccionales a dimensiones del DWH
Este archivo muestra cómo completar la carga de FactSales
"""
import pandas as pd
import logging
from typing import Dict

logger = logging.getLogger(__name__)


class IDMapper:
    """Mapea IDs entre tablas transaccionales y dimensiones del DWH"""
    
    def __init__(self):
        self.customer_id_map: Dict[int, int] = {}
        self.product_id_map: Dict[int, int] = {}
        self.time_id_map: Dict = {}
        self.channel_id_map: Dict[str, int] = {}
        self.order_id_map: Dict[int, int] = {}
    
    def build_customer_map(self, df_original: pd.DataFrame, 
                          df_dw: pd.DataFrame) -> None:
        """Mapea ClienteId original a DimCustomer.id"""
        logger.info("Mapeando IDs de clientes...")
        
        # Asumir que el email es único y sirve para el mapeo
        merge_df = df_original.merge(
            df_dw[['id', 'email']],
            on='email',
            how='left'
        )
        
        self.customer_id_map = dict(zip(merge_df['ClienteId'], merge_df['id']))
        logger.info(f"Mapeo de {len(self.customer_id_map)} clientes completado")
    
    def build_product_map(self, df_original: pd.DataFrame,
                         df_dw: pd.DataFrame) -> None:
        """Mapea ProductoId original a DimProduct.id"""
        logger.info("Mapeando IDs de productos...")
        
        # Asumir que el código (SKU) es único
        merge_df = df_original.merge(
            df_dw[['id', 'code']],
            left_on='SKU',
            right_on='code',
            how='left'
        )
        
        self.product_id_map = dict(zip(merge_df['ProductoId'], merge_df['id']))
        logger.info(f"Mapeo de {len(self.product_id_map)} productos completado")
    
    def build_time_map(self, df_time: pd.DataFrame) -> None:
        """Mapea fechas a DimTime.id"""
        logger.info("Mapeando IDs de tiempo...")
        
        self.time_id_map = dict(zip(df_time['date'], df_time['id']))
        logger.info(f"Mapeo de {len(self.time_id_map)} fechas completado")
    
    def build_channel_map(self, df_dw_channels: pd.DataFrame) -> None:
        """Mapea canales a DimChannel.id"""
        logger.info("Mapeando IDs de canales...")
        
        self.channel_id_map = dict(zip(df_dw_channels['name'], df_dw_channels['id']))
        logger.info(f"Mapeo de {len(self.channel_id_map)} canales completado")
    
    def build_order_map(self, df_original: pd.DataFrame,
                       df_dw: pd.DataFrame) -> None:
        """Mapea OrdenId original a DimOrder.id"""
        logger.info("Mapeando IDs de órdenes...")
        
        # Mapeo secuencial: la primera orden del DWH corresponde a la primera de origen
        self.order_id_map = dict(enumerate(df_original['OrdenId'].unique(), 1))
        logger.info(f"Mapeo de {len(self.order_id_map)} órdenes completado")
    
    def apply_mappings(self, df_detalle: pd.DataFrame,
                      df_ordenes: pd.DataFrame) -> pd.DataFrame:
        """Aplica los mapeos a los detalles y retorna FactSales lista"""
        logger.info("Aplicando mapeos a FactSales...")
        
        df = df_detalle.copy()
        
        # Mapear IDs
        df['customerId'] = df['customerId'].map(self.customer_id_map)
        df['productId'] = df['productId'].map(self.product_id_map)
        df['orderId'] = df['orderId'].map(self.order_id_map)
        
        # Agregar fecha para mapear timeId
        df_merged = df.merge(
            df_ordenes[['id', 'date', 'channel']],
            left_on='orderId',
            right_on='id',
            how='left'
        )
        
        df_merged['timeId'] = df_merged['date'].map(self.time_id_map)
        df_merged['channelId'] = df_merged['channel'].map(self.channel_id_map)
        
        # Agregar timestamps
        df_merged['created_at'] = pd.Timestamp.now()
        
        logger.info(f"Mapeos aplicados a {len(df_merged)} registros")
        
        return df_merged[['customerId', 'productId', 'orderId', 'timeId', 'channelId',
                         'productCant', 'productUnitPriceUSD', 'lineTotalUSD',
                         'discountPercentage', 'created_at']]


# Ejemplo de uso (sería llamado desde run_etl.py)
def complete_fact_sales_example():
    """
    Ejemplo de cómo completar la carga de FactSales
    """
    # 1. Extraer datos originales
    # clientes_orig, productos_orig, ordenes_orig, detalle_orig = extractor.extract_all()
    
    # 2. Transformar datos
    # clientes_trans = transformer.transform_clientes(clientes_orig)
    # ...etc
    
    # 3. Cargar dimensiones
    # loader.load_dim_customer(clientes_trans)
    # ...etc
    
    # 4. Consultar dimensiones del DWH
    # conn = pyodbc.connect(dw_connection_string)
    # dim_customers = pd.read_sql("SELECT id, email FROM DimCustomer", conn)
    # ...etc
    
    # 5. Mapear IDs
    mapper = IDMapper()
    # mapper.build_customer_map(clientes_orig, dim_customers)
    # mapper.build_product_map(productos_orig, dim_products)
    # ...etc
    
    # 6. Aplicar mapeos
    # fact_sales = mapper.apply_mappings(detalle_trans, ordenes_trans)
    
    # 7. Cargar FactSales
    # loader.load_fact_sales(fact_sales)
    
    pass
