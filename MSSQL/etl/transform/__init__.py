"""
Módulo Transform: Aplica reglas de transformación a los datos
Reglas de Integración (ETL):
1. Homologación de productos: construir tabla puente para SKU ↔ codigo_alt ↔ codigo_mongo
2. Normalización de moneda: convertir CRC a USD con tabla de tipo de cambio
3. Estandarización de género: M/F, Masculino/Femenino, Otro/X → valores únicos
4. Conversión de fechas: castear VARCHAR a DATE/DATETIME
5. Transformación de totales: montos string → decimal; montos enteros (CRC) → decimal
"""
import pandas as pd
import numpy as np
from datetime import datetime
import logging
from typing import Dict, List, Tuple

logger = logging.getLogger(__name__)


class DataTransformer:
    """Transforma y normaliza datos según las 5 reglas de integración ETL"""
    
    # REGLA 3: Mapeo de géneros (estandarización)
    # Regla: M|Masculino → Masculino, F|Femenino → Femenino, X|Otro|NULL → No especificado
    GENDER_MAPPING = {
        'M': 'Masculino',
        'F': 'Femenino',
        'X': 'No especificado',
        'Masculino': 'Masculino',
        'Femenino': 'Femenino',
        'Otro': 'No especificado',
        None: 'No especificado',
        'NULL': 'No especificado',
        '': 'No especificado'
    }
    
    # Constantes para trazabilidad
    SOURCE_SYSTEM = 'MSSQL'  # Sistema fuente
    
    def transform_clientes(self, df_clientes: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """
        REGLA 3: Estandarización de género
        Transforma clientes:
        - M/Masculino → Masculino
        - F/Femenino → Femenino
        - X/Otro/NULL → No especificado
        - Normaliza email
        - Convierte fecha a DATE
        - Agrega trazabilidad (source_key, source_system)
        """
        logger.info(f"Transformando {len(df_clientes)} clientes...")
        
        df = df_clientes.copy()
        
        # Mantener source_key para trazabilidad (Consideración 5)
        df['source_system'] = self.SOURCE_SYSTEM
        df['source_key'] = df['ClienteId'].astype(str)
        
        # REGLA 3: Estandarizar género según especificación
        df['Genero'] = df['Genero'].fillna('No especificado')
        df['Genero'] = df['Genero'].map(self.GENDER_MAPPING)
        
        # Normalizar email (lowercase, trim)
        df['Email'] = df['Email'].str.strip().str.lower()
        
        # REGLA 4: Convertir fecha VARCHAR a DATE
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
        
        # Trackeo de registros
        tracking = {
            'source_system': self.SOURCE_SYSTEM,
            'tabla_destino': 'DimCustomer',
            'registros_procesados': len(df)
        }
        
        logger.info(f"✓ Clientes transformados: {len(df)}")
        logger.info(f"  Géneros únicos: {df['gender'].unique()}")
        return df, tracking
    
    def transform_productos(self, df_productos: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """
        REGLA 1: Homologación de productos
        Normaliza SKU (código oficial) para tabla puente
        Mantiene source_key para mapeo posterior con codigo_alt y codigo_mongo
        """
        logger.info(f"Transformando {len(df_productos)} productos...")
        
        df = df_productos.copy()
        
        # Mantener source_key para trazabilidad
        df['source_system'] = self.SOURCE_SYSTEM
        df['source_key'] = df['ProductoId'].astype(str)
        
        # REGLA 1: Normalizar SKU como código oficial
        df['SKU'] = df['SKU'].str.strip().str.upper()
        
        # Normalizar nombre
        df['Nombre'] = df['Nombre'].str.strip()
        
        # Normalizar categoría
        df['Categoria'] = df['Categoria'].str.strip().upper()
        
        # Renombrar columnas
        df = df.rename(columns={
            'ProductoId': 'id',
            'SKU': 'code',
            'Nombre': 'name',
            'Categoria': 'categoryId'
        })
        
        tracking = {
            'source_system': self.SOURCE_SYSTEM,
            'tabla_destino': 'DimProduct',
            'registros_procesados': len(df)
        }
        
        logger.info(f"✓ Productos transformados: {len(df)}")
        logger.info(f"  SKUs únicos: {df['code'].nunique()}")
        return df, tracking
    
    def transform_ordenes(self, df_ordenes: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """
        REGLA 2: Normalización de moneda (USD - MSSQL solo tiene USD)
        REGLA 4: Conversión de fechas VARCHAR a DATETIME
        REGLA 5: Transformación de totales string → decimal
        """
        logger.info(f"Transformando {len(df_ordenes)} órdenes...")
        
        df = df_ordenes.copy()
        
        # Mantener source_key
        df['source_system'] = self.SOURCE_SYSTEM
        df['source_key'] = df['OrdenId'].astype(str)
        
        # REGLA 4: Convertir fecha VARCHAR a DATETIME
        df['Fecha'] = pd.to_datetime(df['Fecha'])
        
        # REGLA 5: Convertir total string → decimal
        df['Total'] = pd.to_numeric(df['Total'], errors='coerce')
        
        # Normalizar canal
        df['Canal'] = df['Canal'].str.strip().str.upper()
        
        # REGLA 2: Moneda siempre USD en MSSQL (validar homogeneidad)
        df['Moneda'] = df['Moneda'].fillna('USD')
        if (df['Moneda'] != 'USD').any():
            logger.warning("⚠️ Se encontraron monedas diferentes a USD en MSSQL (debería ser homogénea)")
        
        # Validar totales
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
        
        tracking = {
            'source_system': self.SOURCE_SYSTEM,
            'tabla_destino': 'FactSales',
            'registros_procesados': len(df)
        }
        
        logger.info(f"✓ Órdenes transformadas: {len(df)}")
        logger.info(f"  Moneda homogénea: USD")
        return df, tracking
    
    def transform_orden_detalle(self, df_detalle: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """
        REGLA 5: Transformación de totales
        - Convierte precios string → decimal
        - Valida cantidades > 0
        - Normaliza descuentos (0-100%)
        - Calcula línea total
        """
        logger.info(f"Transformando {len(df_detalle)} líneas de detalle...")
        
        df = df_detalle.copy()
        
        # Mantener source_key
        df['source_system'] = self.SOURCE_SYSTEM
        df['source_key'] = df['OrdenDetalleId'].astype(str)
        
        # REGLA 5: Convertir precio string → decimal
        df['PrecioUnit'] = pd.to_numeric(df['PrecioUnit'], errors='coerce')
        
        # Convertir cantidad
        df['Cantidad'] = pd.to_numeric(df['Cantidad'], errors='coerce').astype('Int64')
        
        # Normalizar descuento
        df['DescuentoPct'] = df['DescuentoPct'].fillna(0)
        df['DescuentoPct'] = pd.to_numeric(df['DescuentoPct'], errors='coerce')
        df['DescuentoPct'] = df['DescuentoPct'].clip(0, 100)
        
        # Validar datos
        df = df.dropna(subset=['PrecioUnit', 'Cantidad'])
        df = df[(df['Cantidad'] > 0) & (df['PrecioUnit'] >= 0)]
        
        # REGLA 5: Calcular línea total USD
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
        
        tracking = {
            'source_system': self.SOURCE_SYSTEM,
            'tabla_destino': 'FactSales',
            'registros_procesados': len(df),
            'total_usd_procesado': df['lineTotalUSD'].sum()
        }
        
        logger.info(f"✓ Detalles transformados: {len(df)}")
        logger.info(f"  Total USD procesado: ${df['lineTotalUSD'].sum():,.2f}")
        return df, tracking
    
    def extract_categorias(self, df_productos: pd.DataFrame) -> pd.DataFrame:
        """Extrae categorías únicas para DimCategory"""
        logger.info("Extrayendo categorías únicas...")
        
        categorias = df_productos[['categoryId']].drop_duplicates()
        categorias = categorias.rename(columns={'categoryId': 'name'})
        
        logger.info(f"✓ Categorías extraídas: {len(categorias)}")
        return categorias
    
    def extract_canales(self, df_ordenes: pd.DataFrame) -> pd.DataFrame:
        """Extrae canales únicos para DimChannel"""
        logger.info("Extrayendo canales únicos...")
        
        canales = df_ordenes[['channel']].drop_duplicates()
        canales = canales.rename(columns={'channel': 'name'})
        
        logger.info(f"✓ Canales extraídos: {len(canales)}")
        return canales
    
    def generate_dimtime(self, df_ordenes: pd.DataFrame) -> pd.DataFrame:
        """
        Genera tabla DimTime con tipos de cambio
        Para MSSQL: siempre USD, tasa = 1.0
        """
        logger.info("Generando DimTime...")
        
        fechas = df_ordenes['date'].dt.date.unique()
        fechas = pd.to_datetime(fechas)
        
        # Crear dimensión de tiempo
        dim_time = pd.DataFrame({
            'date': fechas,
            'year': fechas.year,
            'month': fechas.month,
            'day': fechas.day,
            'exchangeRateToUSD': 1.0  # MSSQL siempre USD
        })
        
        dim_time['id'] = range(1, len(dim_time) + 1)
        
        logger.info(f"✓ DimTime generada: {len(dim_time)} fechas")
        return dim_time[['id', 'year', 'month', 'day', 'date', 'exchangeRateToUSD']]
    
    def build_product_mapping(self, df_productos: pd.DataFrame) -> pd.DataFrame:
        """
        REGLA 1: Construye tabla puente de mapeo de productos
        Para MSSQL: source_system='MSSQL', source_code=SKU, sku_oficial=SKU
        (El código de MSSQL ya es el oficial)
        """
        logger.info("Construyendo tabla de mapeo de productos...")
        
        mapping = pd.DataFrame({
            'source_system': self.SOURCE_SYSTEM,
            'source_code': df_productos['code'],
            'sku_oficial': df_productos['code'],
            'descripcion': df_productos['name']
        })
        
        logger.info(f"✓ Mapeo de {len(mapping)} productos generado")
        return mapping
