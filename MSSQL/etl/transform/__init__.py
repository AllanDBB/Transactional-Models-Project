"""
Módulo Transform: Aplica reglas de transformación a los datos
"""
import pandas as pd
import numpy as np
from datetime import datetime
import logging
from typing import Dict, List

logger = logging.getLogger(__name__)


class DataTransformer:
    """Transforma y normaliza datos según las reglas ETL"""
    
    # Mapeo de géneros
    GENDER_MAPPING = {
        'M': 'M',
        'F': 'F',
        'X': 'O',
        'Masculino': 'M',
        'Femenino': 'F',
        'Otro': 'O'
    }
    
    def transform_clientes(self, df_clientes: pd.DataFrame) -> pd.DataFrame:
        """
        Transforma la tabla de clientes:
        - Estandariza género a M/F/O
        - Normaliza email
        - Convierte fecha a DATE
        """
        logger.info(f"Transformando {len(df_clientes)} clientes...")
        
        df = df_clientes.copy()
        
        # Estandarizar género: Masculino/Femenino → M/F
        df['Genero'] = df['Genero'].map(self.GENDER_MAPPING)
        
        # Normalizar email (lowercase, trim)
        df['Email'] = df['Email'].str.strip().str.lower()
        
        # Convertir fecha
        df['FechaRegistro'] = pd.to_datetime(df['FechaRegistro']).dt.date
        
        # Renombrar columnas para DWH
        df = df.rename(columns={
            'ClienteId': 'id',
            'Nombre': 'name',
            'Email': 'email',
            'Genero': 'gender',
            'Pais': 'country',
            'FechaRegistro': 'created_at'
        })
        
        logger.info(f"Clientes transformados: {len(df)}")
        return df
    
    def transform_productos(self, df_productos: pd.DataFrame, 
                          mapeo_sku: Dict[str, int] = None) -> pd.DataFrame:
        """
        Transforma la tabla de productos:
        - Normaliza SKU (código oficial)
        - Normaliza nombres
        - Mapea categorías
        """
        logger.info(f"Transformando {len(df_productos)} productos...")
        
        df = df_productos.copy()
        
        # Normalizar SKU (uppercase, trim)
        df['SKU'] = df['SKU'].str.strip().str.upper()
        
        # Normalizar nombre
        df['Nombre'] = df['Nombre'].str.strip()
        
        # Normalizar categoría (uppercase)
        df['Categoria'] = df['Categoria'].str.strip().str.upper()
        
        # Renombrar columnas para DWH
        df = df.rename(columns={
            'ProductoId': 'id',
            'SKU': 'code',
            'Nombre': 'name',
            'Categoria': 'categoryId'  # será mapeado con DimCategory
        })
        
        logger.info(f"Productos transformados: {len(df)}")
        return df
    
    def transform_ordenes(self, df_ordenes: pd.DataFrame,
                         df_clientes_transformados: pd.DataFrame) -> pd.DataFrame:
        """
        Transforma tabla de Orden:
        - Convierte fechas VARCHAR a DATETIME
        - Convierte totales string a DECIMAL
        - Normaliza canales
        - Todas están en USD (homogéneas)
        """
        logger.info(f"Transformando {len(df_ordenes)} órdenes...")
        
        df = df_ordenes.copy()
        
        # Convertir fecha
        df['Fecha'] = pd.to_datetime(df['Fecha'])
        
        # Convertir total a decimal (ya está en USD)
        df['Total'] = pd.to_numeric(df['Total'], errors='coerce')
        
        # Normalizar canal
        df['Canal'] = df['Canal'].str.strip().str.upper()
        
        # Validar valores
        df = df.dropna(subset=['Total'])
        df = df[df['Total'] >= 0]
        
        # Renombrar columnas
        df = df.rename(columns={
            'OrdenId': 'id',
            'ClienteId': 'customerId',
            'Fecha': 'date',
            'Canal': 'channel',
            'Total': 'totalOrderUSD'
        })
        
        logger.info(f"Órdenes transformadas: {len(df)}")
        return df
    
    def transform_orden_detalle(self, df_detalle: pd.DataFrame,
                               df_productos_transformados: pd.DataFrame) -> pd.DataFrame:
        """
        Transforma OrdenDetalle:
        - Convierte precios a DECIMAL
        - Valida cantidades
        - Calcula línea total
        - Normaliza descuentos (0-100%)
        """
        logger.info(f"Transformando {len(df_detalle)} líneas de detalle...")
        
        df = df_detalle.copy()
        
        # Convertir precio unitario (ya está en USD)
        df['PrecioUnit'] = pd.to_numeric(df['PrecioUnit'], errors='coerce')
        
        # Convertir cantidad
        df['Cantidad'] = pd.to_numeric(df['Cantidad'], errors='coerce').astype('Int64')
        
        # Normalizar descuento
        df['DescuentoPct'] = df['DescuentoPct'].fillna(0)
        df['DescuentoPct'] = pd.to_numeric(df['DescuentoPct'], errors='coerce')
        df['DescuentoPct'] = df['DescuentoPct'].clip(0, 100)  # límite 0-100%
        
        # Validar datos
        df = df.dropna(subset=['PrecioUnit', 'Cantidad'])
        df = df[(df['Cantidad'] > 0) & (df['PrecioUnit'] >= 0)]
        
        # Calcular línea total (PrecioUnit * Cantidad * (1 - DescuentoPct/100))
        df['lineTotalUSD'] = df['PrecioUnit'] * df['Cantidad'] * (1 - df['DescuentoPct'] / 100)
        
        # Renombrar columnas
        df = df.rename(columns={
            'OrdenDetalleId': 'id',
            'OrdenId': 'orderId',
            'ProductoId': 'productId',
            'Cantidad': 'productCant',
            'PrecioUnit': 'productUnitPriceUSD',
            'DescuentoPct': 'discountPercentage'
        })
        
        logger.info(f"Detalles transformados: {len(df)}")
        return df
    
    def extract_categorias(self, df_productos: pd.DataFrame) -> pd.DataFrame:
        """
        Extrae categorías únicas para DimCategory
        """
        logger.info("Extrayendo categorías únicas...")
        
        # Obtener categorías únicas de productos transformados
        categorias = df_productos[['categoryId']].drop_duplicates()
        categorias = categorias.rename(columns={'categoryId': 'name'})
        
        logger.info(f"Categorías extraídas: {len(categorias)}")
        return categorias
    
    def extract_canales(self, df_ordenes: pd.DataFrame) -> pd.DataFrame:
        """
        Extrae canales únicos para DimChannel
        """
        logger.info("Extrayendo canales únicos...")
        
        canales = df_ordenes[['channel']].drop_duplicates()
        canales = canales.rename(columns={'channel': 'name'})
        
        logger.info(f"Canales extraídos: {len(canales)}")
        return canales
    
    def generate_dimtime(self, df_ordenes: pd.DataFrame, 
                        min_year: int = 2022, max_year: int = 2025) -> pd.DataFrame:
        """
        Genera tabla DimTime con fechas únicas de las órdenes
        """
        logger.info("Generando DimTime...")
        
        # Extraer fechas únicas
        fechas = df_ordenes['date'].dt.date.unique()
        fechas = pd.to_datetime(fechas)
        
        # Crear dimensión de tiempo
        dim_time = pd.DataFrame({
            'date': fechas,
            'year': fechas.year,
            'month': fechas.month,
            'day': fechas.day,
            'exchangeRateToUSD': 1.0  # Por defecto USD=1.0 para fuente MSSQL
        })
        
        # ID basado en índice
        dim_time['id'] = range(1, len(dim_time) + 1)
        
        logger.info(f"DimTime generada: {len(dim_time)} fechas")
        return dim_time[['id', 'year', 'month', 'day', 'date', 'exchangeRateToUSD']]
