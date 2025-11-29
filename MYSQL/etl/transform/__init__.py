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

try:
    from ExchangeRateHelper import ExchangeRateHelper
except (ImportError, ModuleNotFoundError):
    ExchangeRateHelper = None

from config import ETLConfig

logger = logging.getLogger(__name__)


# ============================================================================
# HELPER FUNCTIONS - Transformations reutilizables (Rule 4 y 5)
# ============================================================================

def clean_numeric_string(value: str) -> float:
    """
    Rule 5: Convert string numbers to float, handling various formats.

    Formats supported:
    - European: 1.750,00 or 1.750.000,50
    - American: 1,200.50 or 1,200,300.50
    - Plain: 1200.50
    """
    if pd.isna(value) or value == '':
        return 0.0

    value = str(value).strip()
    comma_count = value.count(',')
    dot_count = value.count('.')

    if dot_count > 1:
        # European: multiple dots are thousands separators (1.750.000,50)
        parts = value.split('.')
        value = ''.join(parts[:-1]) + '.' + parts[-1] if len(parts[-1]) == 2 else ''.join(parts)
    elif comma_count > 1:
        # American: multiple commas are thousands separators (1,200,300.50)
        value = value.replace(',', '')
    elif comma_count > 0 and dot_count > 0:
        # Mixed: use position to determine decimal separator
        value = value.replace(',', '') if value.rfind('.') > value.rfind(',') else value.replace('.', '').replace(',', '.')
    elif comma_count == 1:
        # Single comma: decimal if 2 digits after, otherwise thousands
        parts = value.split(',')
        value = value.replace(',', '.') if len(parts[-1]) == 2 else value.replace(',', '')

    try:
        return float(value)
    except ValueError:
        logger.warning(f"Could not convert '{value}' to float, returning 0.0")
        return 0.0


def parse_date(fecha_str: str) -> Optional[date]:
    """
    Rule 4: Convert VARCHAR date string to date object.

    Formats: YYYY-MM-DD, YYYY-MM-DD HH:MM:SS, YYYY/MM/DD, etc.
    """
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
        logger.warning(f"Could not parse date '{fecha_str}'")
        return None


def convert_currency(amount: float, from_currency: str, to_currency: str,
                    exchange_helper = None,
                    conversion_date: Optional[date] = None) -> float:
    """
    Rule 2: Convert amount from one currency to another.

    If exchange_helper is available, uses real rates from DWH.
    Falls back to default rate (515.0 for CRC->USD) if helper unavailable.
    """
    if pd.isna(amount) or from_currency == to_currency:
        return amount

    if exchange_helper and from_currency == 'CRC' and to_currency == 'USD':
        converted = exchange_helper.convertir_monto(amount, from_currency, to_currency, conversion_date)
        if pd.notna(converted):
            return converted

    # Fallback to default rate
    if from_currency == 'CRC' and to_currency == 'USD':
        return amount / ETLConfig.DEFAULT_CRC_USD_RATE

    return amount


# ============================================================================
# GENDER MAPPING - Rule 3
# ============================================================================

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


# ============================================================================
# MAIN TRANSFORMER CLASS
# ============================================================================

class DataTransformer:
    """Transforms and normalizes MySQL data according to 5 ETL integration rules."""

    SOURCE_SYSTEM = 'MYSQL'

    def __init__(self, exchange_rate_helper = None):
        self.exchange_rate_helper = exchange_rate_helper

    def transform_clientes(self, df_clientes: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """
        Transform customer data applying Rule 3 and 4.

        Rule 3: Standardize gender (M/F/X -> Male/Female/Unspecified)
        Rule 4: Parse dates from VARCHAR to DATE
        """
        logger.info(f"Transforming {len(df_clientes)} customers...")
        df = df_clientes.copy()

        df['source_system'] = self.SOURCE_SYSTEM
        df['source_key'] = df['id'].astype(str)
        df['genero'] = df['genero'].fillna('X').map(GENDER_MAPPING)
        df['correo'] = df['correo'].fillna('').str.strip().str.lower()
        df['created_at'] = df['created_at'].apply(parse_date)
        df['pais'] = df['pais'].str.strip()

        tracking = {
            'total': len(df),
            'gender_standardized': len(df[df['genero'].notna()]),
            'date_parsed': len(df[df['created_at'].notna()])
        }
        logger.info(f"Transformed {len(df)} customers - Gender: {tracking['gender_standardized']}, Dates: {tracking['date_parsed']}")

        return df, tracking

    def transform_productos(self, df_productos: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """
        Transform product data applying Rule 1.

        Rule 1: Homologate products by creating official SKU from ID.
        codigo_alt is preserved for mapping but sku_oficial becomes the canonical code.
        """
        logger.info(f"Transforming {len(df_productos)} products...")
        df = df_productos.copy()

        df['source_system'] = self.SOURCE_SYSTEM
        df['source_key'] = df['id'].astype(str)
        df['sku_oficial'] = 'SKU-' + df['id'].astype(str).str.zfill(5)
        df['categoria'] = df['categoria'].str.strip()

        tracking = {
            'total': len(df),
            'sku_generated': len(df[df['sku_oficial'].notna()])
        }
        logger.info(f"Transformed {len(df)} products")

        return df, tracking

    def transform_ordenes(self, df_ordenes: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """
        Transform order data applying Rule 2, 4 and 5.

        Rule 2: Normalize currency to USD using exchange rates
        Rule 4: Parse order dates from VARCHAR to DATETIME
        Rule 5: Clean amount strings to decimal numbers
        """
        logger.info(f"Transforming {len(df_ordenes)} orders...")
        df = df_ordenes.copy()

        df['source_system'] = self.SOURCE_SYSTEM
        df['source_key'] = df['id'].astype(str)
        df['fecha'] = df['fecha'].apply(lambda x: pd.to_datetime(parse_date(x)) if parse_date(x) else None)
        df['total_limpio'] = df['total'].apply(clean_numeric_string)

        # Apply Rule 2: Currency conversion
        crc_rows = df['moneda'] == 'CRC'
        if crc_rows.any():
            if self.exchange_rate_helper:
                logger.info(f"Converting {crc_rows.sum()} orders from CRC to USD using DWH rates")
                df['total_usd'] = df.apply(
                    lambda row: convert_currency(
                        row['total_limpio'], 'CRC', 'USD',
                        self.exchange_rate_helper,
                        row['fecha'].date() if pd.notna(row['fecha']) else None
                    ) if row['moneda'] == 'CRC' else row['total_limpio'],
                    axis=1
                )
            else:
                logger.info(f"Converting {crc_rows.sum()} orders using default rate")
                df['total_usd'] = df.apply(
                    lambda row: convert_currency(row['total_limpio'], row['moneda'], 'USD')
                    if row['moneda'] != 'USD' else row['total_limpio'],
                    axis=1
                )
        else:
            df['total_usd'] = df['total_limpio']

        df['canal'] = df['canal'].str.strip().str.upper()

        tracking = {
            'total': len(df),
            'date_parsed': len(df[df['fecha'].notna()]),
            'amount_cleaned': len(df[df['total_limpio'] > 0]),
            'crc_converted': crc_rows.sum()
        }
        logger.info(f"Transformed {len(df)} orders - Dates: {tracking['date_parsed']}, CRC: {tracking['crc_converted']}")

        return df, tracking

    def transform_orden_detalle(self, df_detalle: pd.DataFrame, df_ordenes: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """
        Transform order detail line items applying Rule 2 and 5.

        Rule 2: Convert unit prices from order currency to USD
        Rule 5: Clean unit price strings to decimal numbers
        """
        logger.info(f"Transforming {len(df_detalle)} order details...")
        df = df_detalle.copy()

        df['source_system'] = self.SOURCE_SYSTEM
        df['source_key'] = df['id'].astype(str)
        df['precio_unit_limpio'] = df['precio_unit'].apply(clean_numeric_string)

        # Join with orders to get currency and date
        df = df.merge(
            df_ordenes[['id', 'moneda', 'fecha']],
            left_on='orden_id', right_on='id', how='left', suffixes=('', '_orden')
        )

        # Apply Rule 2: Convert prices to USD
        crc_rows = df['moneda'] == 'CRC'
        if crc_rows.any():
            if self.exchange_rate_helper:
                logger.info(f"Converting {crc_rows.sum()} unit prices from CRC to USD")
                df['precio_unit_usd'] = df.apply(
                    lambda row: convert_currency(
                        row['precio_unit_limpio'], 'CRC', 'USD',
                        self.exchange_rate_helper,
                        row['fecha'].date() if pd.notna(row['fecha']) else None
                    ) if row['moneda'] == 'CRC' else row['precio_unit_limpio'],
                    axis=1
                )
            else:
                logger.info(f"Converting {crc_rows.sum()} unit prices using default rate")
                df['precio_unit_usd'] = df.apply(
                    lambda row: convert_currency(row['precio_unit_limpio'], row['moneda'], 'USD')
                    if row['moneda'] != 'USD' else row['precio_unit_limpio'],
                    axis=1
                )
        else:
            df['precio_unit_usd'] = df['precio_unit_limpio']

        tracking = {
            'total': len(df),
            'price_cleaned': len(df[df['precio_unit_limpio'] > 0]),
            'crc_converted': crc_rows.sum()
        }
        logger.info(f"Transformed {len(df)} details - Prices cleaned: {tracking['price_cleaned']}, CRC: {tracking['crc_converted']}")

        return df, tracking

    def extract_categorias(self, df_productos: pd.DataFrame) -> pd.DataFrame:
        """Extract unique categories from products."""
        categorias = df_productos[['categoria']].drop_duplicates().reset_index(drop=True)
        categorias['categoria_id'] = range(1, len(categorias) + 1)
        return categorias

    def extract_canales(self, df_ordenes: pd.DataFrame) -> pd.DataFrame:
        """Extract unique sales channels from orders."""
        canales = df_ordenes[['canal']].drop_duplicates().reset_index(drop=True)
        canales['canal_id'] = range(1, len(canales) + 1)
        canales.columns = ['nombre', 'canal_id']
        return canales

    def generate_dimtime(self, df_ordenes: pd.DataFrame) -> pd.DataFrame:
        """Generate time dimension from order date range."""
        if df_ordenes['fecha'].isna().all():
            logger.warning("No valid dates found in orders")
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
        """
        Rule 1: Build product mapping table linking codigo_alt to official SKU.

        Used for traceability and cross-source product matching.
        """
        mapping = pd.DataFrame({
            'source_system': df_productos['source_system'],
            'source_code': df_productos['codigo_alt'],
            'sku_oficial': df_productos['sku_oficial'],
            'descripcion': df_productos['nombre']
        })

        logger.info(f"Built {len(mapping)} product mappings for Rule 1")
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

        Uses inner joins to ensure only valid records with complete references are included.
        Orphaned details (with missing orders/products/customers) are excluded.
        Preserves source_keys for traceability to source system.
        """
        logger.info("Building FactSales table...")

        # Join details with orders
        fact = df_detalle.merge(
            df_ordenes[['id', 'cliente_id', 'fecha', 'canal', 'moneda', 'total_usd', 'source_key']],
            left_on='orden_id', right_on='id', how='inner', suffixes=('_detalle', '_orden')
        )
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

        # Calculate line total in USD
        fact['monto_linea'] = fact['precio_unit_usd'] * fact['cantidad']

        logger.info(f"Built FactSales with {len(fact)} rows")
        return fact
