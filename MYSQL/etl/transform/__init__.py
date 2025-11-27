"""
Modulo Transform: Aplica las 5 reglas de transformación a los datos de MySQL

Reglas de Integración (ETL):
1. Homologación de productos: codigo_alt -> SKU oficial (tabla puente)
2. Normalización de moneda: convertir CRC a USD con tabla de tipo de cambio
3. Estandarización de género: M/F/X -> valores unicos (Masculino/Femenino/No especificado)
4. Conversión de fechas: VARCHAR a DATE/DATETIME
5. Transformación de totales: montos string -> decimal (limpiar comas/puntos)

Heterogeneidades de MySQL:
- codigo_alt (no es SKU oficial) -> REGLA 1
- Fechas en VARCHAR -> REGLA 4
- Montos en VARCHAR con formato variado -> REGLA 5
- Género M/F/X -> REGLA 3
- Moneda mezclada USD/CRC -> REGLA 2
- SIN campo descuento en OrdenDetalle
"""
import pandas as pd
import numpy as np
from datetime import datetime, date
import logging
from typing import Dict, List, Tuple, Optional
import re
import sys
from pathlib import Path

# Importar ExchangeRateHelper desde shared
root_path = Path(__file__).resolve().parents[2]
shared_path = root_path / "shared"
sys.path.insert(0, str(shared_path))

from ExchangeRateHelper import ExchangeRateHelper  # type: ignore
from config import ETLConfig

logger = logging.getLogger(__name__)


class DataTransformer:
    """Transforma y normaliza datos de MySQL según las 5 reglas de integración ETL"""

    # REGLA 3: Mapeo de géneros (MySQL usa M/F/X)
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

    SOURCE_SYSTEM = 'MYSQL'

    def __init__(self, exchange_rate_helper: Optional[ExchangeRateHelper] = None):
        """
        Args:
            exchange_rate_helper: Instancia de ExchangeRateHelper para consultar tasas del DWH
                                  Si es None, usa tasa por defecto
        """
        self.exchange_rate_helper = exchange_rate_helper

    def _clean_numeric_string(self, value: str) -> float:
        """
        REGLA 5: Limpia string numérico con comas/puntos
        Maneja formatos: '1,200.50', '1200,50', '1200.50', '1200'
        """
        if pd.isna(value) or value == '':
            return 0.0

        value = str(value).strip()

        # Formato: 1,200.50 (coma como separador de miles, punto decimal)
        if value.count(',') > 0 and value.count('.') > 0:
            last_comma = value.rfind(',')
            last_dot = value.rfind('.')
            if last_dot > last_comma:
                value = value.replace(',', '')
            else:
                value = value.replace('.', '').replace(',', '.')
        # Formato: 1200,50 (coma como decimal)
        elif value.count(',') == 1 and value.count('.') == 0:
            value = value.replace(',', '.')
        # Formato: 1,200 o 1.200 (sin decimales, detectar separador)
        elif value.count(',') >= 1 and value.count('.') == 0:
            if len(value.split(',')[-1]) == 3:
                value = value.replace(',', '')
            else:
                value = value.replace(',', '.')

        try:
            return float(value)
        except ValueError:
            logger.warning(f"No se pudo convertir '{value}' a float, retornando 0.0")
            return 0.0

    def _parse_fecha(self, fecha_str: str) -> Optional[date]:
        """
        REGLA 4: Convierte VARCHAR a DATE
        Maneja formatos: 'YYYY-MM-DD' y 'YYYY-MM-DD HH:MM:SS'
        """
        if pd.isna(fecha_str) or fecha_str == '':
            return None

        fecha_str = str(fecha_str).strip()

        # Intenta primero con formato completo
        for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%Y/%m/%d']:
            try:
                return pd.to_datetime(fecha_str, format=fmt).date()
            except:
                continue

        # Fallback a conversión automática
        try:
            return pd.to_datetime(fecha_str).date()
        except:
            logger.warning(f"No se pudo parsear fecha '{fecha_str}', retornando None")
            return None

    def transform_clientes(self, df_clientes: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """
        REGLA 3: Estandarización de género (M/F/X -> Masculino/Femenino/No especificado)
        REGLA 4: Conversión de fechas (VARCHAR 'YYYY-MM-DD' -> DATE)

        Args:
            df_clientes: DataFrame extraido de Cliente table

        Returns:
            Tuple: (df_transformado, tracking_dict)
        """
        logger.info(f"Transformando {len(df_clientes)} clientes...")

        df = df_clientes.copy()

        # Agregar trazabilidad (Consideración 5)
        df['source_system'] = self.SOURCE_SYSTEM
        df['source_key'] = df['id'].astype(str)

        # REGLA 3: Estandarizar género M/F/X -> Masculino/Femenino/No especificado
        df['genero'] = df['genero'].fillna('X')
        df['genero'] = df['genero'].map(self.GENDER_MAPPING)

        # Normalizar email
        df['correo'] = df['correo'].fillna('')
        df['correo'] = df['correo'].str.strip().str.lower()

        # REGLA 4: Convertir fecha VARCHAR 'YYYY-MM-DD' a DATE
        df['created_at'] = df['created_at'].apply(self._parse_fecha)

        # Normalizar país
        df['pais'] = df['pais'].str.strip()

        logger.info(f"[OK] {len(df)} clientes transformados")

        track = {
            'total': len(df),
            'genero_standardized': len(df[df['genero'].notna()]),
            'fecha_parsed': len(df[df['created_at'].notna()])
        }

        return df, track

    def transform_productos(self, df_productos: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """
        REGLA 1: Homologación de productos (codigo_alt -> SKU oficial)

        Args:
            df_productos: DataFrame extraido de Producto table

        Returns:
            Tuple: (df_transformado, tracking_dict)
        """
        logger.info(f"Transformando {len(df_productos)} productos...")

        df = df_productos.copy()

        # Agregar trazabilidad
        df['source_system'] = self.SOURCE_SYSTEM
        df['source_key'] = df['id'].astype(str)

        # REGLA 1: codigo_alt se mantiene para mapeo posterior
        # Se generará SKU oficial en tabla puente
        df['sku_oficial'] = 'SKU-' + df['id'].astype(str).str.zfill(5)

        # Normalizar categoria
        df['categoria'] = df['categoria'].str.strip()

        logger.info(f"[OK] {len(df)} productos transformados")

        track = {
            'total': len(df),
            'sku_generated': len(df[df['sku_oficial'].notna()])
        }

        return df, track

    def transform_ordenes(self, df_ordenes: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """
        REGLA 2: Normalización de moneda (CRC -> USD)
        REGLA 4: Conversion de fechas (VARCHAR -> DATETIME)
        REGLA 5: Transformación de totales (string con comas/puntos -> DECIMAL)

        Args:
            df_ordenes: DataFrame extraido de Orden table

        Returns:
            Tuple: (df_transformado, tracking_dict)
        """
        logger.info(f"Transformando {len(df_ordenes)} órdenes...")

        df = df_ordenes.copy()

        # Agregar trazabilidad
        df['source_system'] = self.SOURCE_SYSTEM
        df['source_key'] = df['id'].astype(str)

        # REGLA 4: Convertir fecha VARCHAR a DATETIME
        df['fecha'] = df['fecha'].apply(
            lambda x: pd.to_datetime(self._parse_fecha(x)) if self._parse_fecha(x) else None
        )

        # REGLA 5: Limpiar montos (string -> decimal)
        df['total_limpio'] = df['total'].apply(self._clean_numeric_string)

        # REGLA 2: Normalizar moneda (CRC -> USD)
        # Por ahora usar tasa por defecto. ExchangeRateHelper se usará en load si está disponible
        crc_rows = df['moneda'] == 'CRC'
        if crc_rows.any():
            logger.warning(f"[REGLA 2] {crc_rows.sum()} órdenes en CRC - usando tasa por defecto {ETLConfig.DEFAULT_CRC_USD_RATE}")
            df.loc[crc_rows, 'total_usd'] = df.loc[crc_rows, 'total_limpio'] / ETLConfig.DEFAULT_CRC_USD_RATE
            df.loc[~crc_rows, 'total_usd'] = df.loc[~crc_rows, 'total_limpio']
        else:
            df['total_usd'] = df['total_limpio']

        # Normalizar canal
        df['canal'] = df['canal'].str.strip().str.upper()

        logger.info(f"[OK] {len(df)} órdenes transformadas")

        track = {
            'total': len(df),
            'fecha_parsed': len(df[df['fecha'].notna()]),
            'total_cleaned': len(df[df['total_limpio'] > 0]),
            'crc_converted': crc_rows.sum()
        }

        return df, track

    def transform_orden_detalle(self, df_detalle: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """
        REGLA 5: Transformación de totales (precio_unit string -> decimal)

        Args:
            df_detalle: DataFrame extraido de OrdenDetalle table

        Returns:
            Tuple: (df_transformado, tracking_dict)
        """
        logger.info(f"Transformando {len(df_detalle)} detalles de órdenes...")

        df = df_detalle.copy()

        # Agregar trazabilidad
        df['source_system'] = self.SOURCE_SYSTEM
        df['source_key'] = df['id'].astype(str)

        # REGLA 5: Limpiar precio_unit (string -> decimal)
        df['precio_unit_limpio'] = df['precio_unit'].apply(self._clean_numeric_string)

        logger.info(f"[OK] {len(df)} detalles transformados")

        track = {
            'total': len(df),
            'precio_cleaned': len(df[df['precio_unit_limpio'] > 0])
        }

        return df, track

    def extract_categorias(self, df_productos: pd.DataFrame) -> pd.DataFrame:
        """Extrae dimensión de categorías"""
        categorias = df_productos[['categoria']].drop_duplicates().reset_index(drop=True)
        categorias['categoria_id'] = range(1, len(categorias) + 1)
        return categorias

    def extract_canales(self, df_ordenes: pd.DataFrame) -> pd.DataFrame:
        """Extrae dimensión de canales"""
        canales = df_ordenes[['canal']].drop_duplicates().reset_index(drop=True)
        canales['canal_id'] = range(1, len(canales) + 1)
        canales.columns = ['nombre', 'canal_id']
        return canales

    def generate_dimtime(self, df_ordenes: pd.DataFrame) -> pd.DataFrame:
        """Genera dimensión de tiempo basada en fechas de órdenes"""
        if df_ordenes['fecha'].isna().all():
            logger.warning("No hay fechas válidas en órdenes")
            return pd.DataFrame()

        # Obtener rango de fechas
        fechas = pd.to_datetime(df_ordenes['fecha'].dropna()).dt.date.unique()
        fechas = sorted(fechas)

        if len(fechas) == 0:
            return pd.DataFrame()

        # Generar rango de fechas
        fecha_min = min(fechas)
        fecha_max = max(fechas)
        date_range = pd.date_range(start=fecha_min, end=fecha_max, freq='D')

        dim_time = []
        for idx, fecha in enumerate(date_range, start=1):
            dim_time.append({
                'time_id': idx,
                'fecha': fecha.date(),
                'anio': fecha.year,
                'mes': fecha.month,
                'dia': fecha.day,
                'trimestre': (fecha.month - 1) // 3 + 1
            })

        return pd.DataFrame(dim_time)

    def build_product_mapping(self, df_productos: pd.DataFrame) -> pd.DataFrame:
        """
        REGLA 1: Construye tabla puente de mapeo de productos
        mapea: source_system, source_code (codigo_alt), sku_oficial
        """
        mapping = pd.DataFrame({
            'source_system': df_productos['source_system'],
            'source_code': df_productos['codigo_alt'],
            'sku_oficial': df_productos['sku_oficial'],
            'descripcion': df_productos['nombre']
        })

        logger.info(f"[REGLA 1] {len(mapping)} mapeos de productos construidos")
        return mapping

    def build_fact_sales(
        self,
        df_detalle: pd.DataFrame,
        df_ordenes: pd.DataFrame,
        df_productos: pd.DataFrame,
        df_clientes: pd.DataFrame,
        dw_connection_string: str,
        order_tracking: Dict
    ) -> pd.DataFrame:
        """
        Construye tabla de hechos FactSales
        Realiza joins entre tablas transformadas
        """
        logger.info("Construyendo FactSales...")

        # Join con ordenes
        fact = df_detalle.merge(
            df_ordenes[['id', 'cliente_id', 'fecha', 'canal', 'total_usd']],
            left_on='orden_id',
            right_on='id',
            how='left'
        )

        # Join con productos
        fact = fact.merge(
            df_productos[['id', 'sku_oficial', 'categoria']],
            left_on='producto_id',
            right_on='id',
            how='left'
        )

        # Join con clientes
        fact = fact.merge(
            df_clientes[['id', 'pais']],
            left_on='cliente_id',
            right_on='id',
            how='left'
        )

        # Calcular monto de línea
        fact['monto_linea'] = fact['precio_unit_limpio'] * fact['cantidad']

        logger.info(f"[OK] {len(fact)} registros en FactSales")

        return fact
