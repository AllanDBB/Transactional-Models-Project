"""
Data transformation module applying 5 integration rules:

1. Product homologation: codigo_alt -> official SKU (mapping table)
2. Currency normalization: CRC to USD using exchange rates
3. Gender standardization: M/F/X -> standardized values
4. Date conversion: VARCHAR to DATE/DATETIME
5. Amount transformation: string to decimal (clean formatting)
"""
import pandas as pd
from datetime import datetime, date
import logging
from typing import Dict, Tuple, Optional
import sys
from pathlib import Path

root_path = Path(__file__).resolve().parents[2]
shared_path = root_path / "shared"
sys.path.insert(0, str(shared_path))

from ExchangeRateHelper import ExchangeRateHelper # type: ignore
from config import ETLConfig

logger = logging.getLogger(__name__)


class DataTransformer:
    """Transforms and normalizes MySQL data according to 5 ETL integration rules."""

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
        self.exchange_rate_helper = exchange_rate_helper

    def _clean_numeric_string(self, value: str) -> float:
        """
        Rule 5: Clean numeric strings with various formats (European/American).
        Handles: 1.750,00 (EU), 1,200.50 (US), 1200.50, etc.
        """
        if pd.isna(value) or value == '':
            return 0.0

        value = str(value).strip()
        comma_count = value.count(',')
        dot_count = value.count('.')

        # European format with multiple dots as thousands separator (1.750.000,00)
        if dot_count > 1:
            parts = value.split('.')
            value = ''.join(parts[:-1]) + '.' + parts[-1] if len(parts[-1]) == 2 else ''.join(parts)
        # American format with multiple commas as thousands separator (1,200,300.00)
        elif comma_count > 1:
            value = value.replace(',', '')
        # Mixed: determine which is decimal separator based on position
        elif comma_count > 0 and dot_count > 0:
            value = value.replace(',', '') if value.rfind('.') > value.rfind(',') else value.replace('.', '').replace(',', '.')
        # Single comma: check if decimal (,50) or thousands (1,200)
        elif comma_count == 1:
            parts = value.split(',')
            value = value.replace(',', '.') if len(parts[-1]) == 2 else value.replace(',', '')

        try:
            return float(value)
        except ValueError:
            logger.warning(f"No se pudo convertir '{value}' a float, retornando 0.0")
            return 0.0

    def _parse_fecha(self, fecha_str: str) -> Optional[date]:
        """Rule 4: Convert VARCHAR to DATE."""
        if pd.isna(fecha_str) or fecha_str == '':
            return None

        fecha_str = str(fecha_str).strip()
        for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%Y/%m/%d']:
            try:
                return pd.to_datetime(fecha_str, format=fmt).date()
            except:
                continue

        try:
            return pd.to_datetime(fecha_str).date()
        except:
            logger.warning(f"No se pudo parsear fecha '{fecha_str}', retornando None")
            return None

    def transform_clientes(self, df_clientes: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """Rule 3: Gender standardization. Rule 4: Date conversion."""
        logger.info(f"Transformando {len(df_clientes)} clientes...")

        df = df_clientes.copy()
        df['source_system'] = self.SOURCE_SYSTEM
        df['source_key'] = df['id'].astype(str)

        df['genero'] = df['genero'].fillna('X').map(self.GENDER_MAPPING)
        df['correo'] = df['correo'].fillna('').str.strip().str.lower()
        df['created_at'] = df['created_at'].apply(self._parse_fecha)
        df['pais'] = df['pais'].str.strip()

        logger.info(f"[OK] {len(df)} clientes transformados")

        return df, {
            'total': len(df),
            'genero_standardized': len(df[df['genero'].notna()]),
            'fecha_parsed': len(df[df['created_at'].notna()])
        }

    def transform_productos(self, df_productos: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """Rule 1: Product homologation (codigo_alt -> official SKU)."""
        logger.info(f"Transformando {len(df_productos)} productos...")

        df = df_productos.copy()
        df['source_system'] = self.SOURCE_SYSTEM
        df['source_key'] = df['id'].astype(str)
        df['sku_oficial'] = 'SKU-' + df['id'].astype(str).str.zfill(5)
        df['categoria'] = df['categoria'].str.strip()

        logger.info(f"[OK] {len(df)} productos transformados")

        return df, {
            'total': len(df),
            'sku_generated': len(df[df['sku_oficial'].notna()])
        }

    def transform_ordenes(self, df_ordenes: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """Rule 2: Currency normalization. Rule 4: Date conversion. Rule 5: Amount transformation."""
        logger.info(f"Transformando {len(df_ordenes)} ordenes...")

        df = df_ordenes.copy()
        df['source_system'] = self.SOURCE_SYSTEM
        df['source_key'] = df['id'].astype(str)

        df['fecha'] = df['fecha'].apply(
            lambda x: pd.to_datetime(self._parse_fecha(x)) if self._parse_fecha(x) else None
        )
        df['total_limpio'] = df['total'].apply(self._clean_numeric_string)

        crc_rows = df['moneda'] == 'CRC'
        if crc_rows.any():
            if self.exchange_rate_helper:
                logger.info(f"[REGLA 2] Convirtiendo {crc_rows.sum()} ordenes CRC->USD usando tasas del DWH")
                df['total_usd'] = df.apply(
                    lambda row: self.exchange_rate_helper.convertir_monto(
                        row['total_limpio'], 'CRC', 'USD',
                        row['fecha'].date() if pd.notna(row['fecha']) else None
                    ) if row['moneda'] == 'CRC' else row['total_limpio'],
                    axis=1
                )

                failed_conversions = df[crc_rows & df['total_usd'].isna()]
                if len(failed_conversions) > 0:
                    logger.warning(f"[REGLA 2] {len(failed_conversions)} ordenes sin tasa - usando default")
                    df.loc[crc_rows & df['total_usd'].isna(), 'total_usd'] = \
                        df.loc[crc_rows & df['total_usd'].isna(), 'total_limpio'] / ETLConfig.DEFAULT_CRC_USD_RATE
            else:
                logger.warning(f"[REGLA 2] {crc_rows.sum()} ordenes en CRC - usando tasa por defecto")
                df.loc[crc_rows, 'total_usd'] = df.loc[crc_rows, 'total_limpio'] / ETLConfig.DEFAULT_CRC_USD_RATE
                df.loc[~crc_rows, 'total_usd'] = df.loc[~crc_rows, 'total_limpio']
        else:
            df['total_usd'] = df['total_limpio']

        df['canal'] = df['canal'].str.strip().str.upper()
        logger.info(f"[OK] {len(df)} ordenes transformadas")

        return df, {
            'total': len(df),
            'fecha_parsed': len(df[df['fecha'].notna()]),
            'total_cleaned': len(df[df['total_limpio'] > 0]),
            'crc_converted': crc_rows.sum()
        }

    def transform_orden_detalle(self, df_detalle: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """Rule 5: Amount transformation (precio_unit string to decimal)."""
        logger.info(f"Transformando {len(df_detalle)} detalles de ordenes...")

        df = df_detalle.copy()
        df['source_system'] = self.SOURCE_SYSTEM
        df['source_key'] = df['id'].astype(str)
        df['precio_unit_limpio'] = df['precio_unit'].apply(self._clean_numeric_string)

        logger.info(f"[OK] {len(df)} detalles transformados")

        return df, {
            'total': len(df),
            'precio_cleaned': len(df[df['precio_unit_limpio'] > 0])
        }

    def extract_categorias(self, df_productos: pd.DataFrame) -> pd.DataFrame:
        categorias = df_productos[['categoria']].drop_duplicates().reset_index(drop=True)
        categorias['categoria_id'] = range(1, len(categorias) + 1)
        return categorias

    def extract_canales(self, df_ordenes: pd.DataFrame) -> pd.DataFrame:
        canales = df_ordenes[['canal']].drop_duplicates().reset_index(drop=True)
        canales['canal_id'] = range(1, len(canales) + 1)
        canales.columns = ['nombre', 'canal_id']
        return canales

    def generate_dimtime(self, df_ordenes: pd.DataFrame) -> pd.DataFrame:
        if df_ordenes['fecha'].isna().all():
            logger.warning("No hay fechas validas en ordenes")
            return pd.DataFrame()

        fechas = pd.to_datetime(df_ordenes['fecha'].dropna()).dt.date.unique()
        fechas = sorted(fechas)

        if len(fechas) == 0:
            return pd.DataFrame()

        fecha_min, fecha_max = min(fechas), max(fechas)
        date_range = pd.date_range(start=fecha_min, end=fecha_max, freq='D')

        dim_time = [
            {
                'time_id': idx,
                'fecha': fecha.date(),
                'anio': fecha.year,
                'mes': fecha.month,
                'dia': fecha.day,
                'trimestre': (fecha.month - 1) // 3 + 1
            }
            for idx, fecha in enumerate(date_range, start=1)
        ]

        return pd.DataFrame(dim_time)

    def build_product_mapping(self, df_productos: pd.DataFrame) -> pd.DataFrame:
        """Rule 1: Build product mapping table (codigo_alt -> official SKU)."""
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
        Build FactSales by joining order details with dimensions.
        Uses inner joins to ensure referential integrity.
        Preserves source_keys for ID mapping to DWH.
        """
        logger.info("Construyendo FactSales...")

        # Join details with orders (inner join ensures orphaned details are excluded)
        fact = df_detalle.merge(
            df_ordenes[['id', 'cliente_id', 'fecha', 'canal', 'total_usd', 'source_key']],
            left_on='orden_id', right_on='id', how='inner', suffixes=('_detalle', '_orden')
        )
        # Rename to distinguish detail vs order source keys
        fact.rename(columns={'source_key_orden': 'orden_source_key', 'source_key_detalle': 'detalle_source_key'}, inplace=True)

        # Join with products
        fact = fact.merge(
            df_productos[['id', 'sku_oficial', 'categoria', 'source_key']],
            left_on='producto_id', right_on='id', how='inner', suffixes=('', '_producto')
        )
        fact.rename(columns={'source_key': 'producto_source_key'}, inplace=True)

        # Join with customers
        fact = fact.merge(
            df_clientes[['id', 'pais', 'source_key']],
            left_on='cliente_id', right_on='id', how='inner', suffixes=('', '_cliente')
        )
        fact.rename(columns={'source_key': 'cliente_source_key'}, inplace=True)

        # Calculate line total (quantity * unit price)
        fact['monto_linea'] = fact['precio_unit_limpio'] * fact['cantidad']

        logger.info(f"[OK] {len(fact)} registros en FactSales")

        return fact
