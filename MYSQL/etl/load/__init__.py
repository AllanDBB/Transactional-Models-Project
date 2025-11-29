"""
Data loading module for MySQL to MSSQL Data Warehouse ETL.

Loads transformed data into DW tables with idempotent behavior:
- All dimension loads check for existing records before inserting
- Updates only if necessary, otherwise returns existing ID
- Fact table loading skips records with missing dimension references
"""
import pyodbc
import pandas as pd
import logging
from typing import Dict, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class DataLoader:
    """Loads transformed data into MSSQL Data Warehouse with idempotent behavior."""

    CHANNEL_TYPE_MAP = {
        'WEBSITE': 'Website',
        'TIENDA': 'Store',
        'APP': 'App',
        'SOCIO': 'Partner'
    }

    GENDER_REVERSE_MAP = {
        'Masculino': 'M',
        'Femenino': 'F',
        'No especificado': 'O'
    }

    def __init__(self, connection_string: str):
        self.connection_string = connection_string

    def _get_connection(self):
        """Create database connection."""
        try:
            return pyodbc.connect(self.connection_string)
        except Exception as e:
            logger.error(f"Failed to connect to DWH: {str(e)}")
            raise

    def truncate_tables(self, table_names: List[str]) -> None:
        """Clean specified tables in correct order (respecting foreign keys)."""
        logger.info(f"Cleaning tables: {', '.join(table_names)}")

        # Correct dependency order for DW (fact before dimensions)
        order = ['FactSales', 'DimProduct', 'DimCustomer', 'DimTime', 'DimChannel', 'DimCategory']
        tables_ordered = [t for t in order if t in table_names]
        tables_ordered += [t for t in table_names if t not in tables_ordered]

        with self._get_connection() as conn:
            for table in tables_ordered:
                try:
                    conn.execute(f"TRUNCATE TABLE {table}")
                except:
                    try:
                        conn.execute(f"DELETE FROM {table}")
                    except Exception as e:
                        logger.warning(f"Could not clean {table}: {e}")
            conn.commit()

        logger.info("Tables cleaned successfully")

    # ========================================================================
    # DIMENSION LOADING - IDEMPOTENT OPERATIONS
    # ========================================================================

    def load_dim_category(self, df_categorias: pd.DataFrame) -> Dict:
        """
        Load categories to DWH using idempotent logic.

        Returns mapping: category_name -> DWH category_id
        Existing categories are skipped, new ones are created.
        """
        logger.info(f"Loading {len(df_categorias)} categories...")
        category_map = {}
        inserted = 0

        with self._get_connection() as conn:
            for _, row in df_categorias.iterrows():
                nombre = str(row['categoria']).strip()

                # Check if category already exists
                existing = conn.execute(
                    "SELECT id FROM DimCategory WHERE name = ?", nombre
                ).fetchone()

                if existing:
                    category_map[nombre] = int(existing[0])
                else:
                    try:
                        conn.execute("INSERT INTO DimCategory (name) VALUES (?)", nombre)
                        conn.commit()
                        # Get the inserted ID
                        new_id = conn.execute("SELECT @@IDENTITY").fetchone()[0]
                        category_map[nombre] = int(new_id)
                        inserted += 1
                    except Exception as e:
                        logger.warning(f"Could not insert category '{nombre}': {e}")

        logger.info(f"Categories: {len(category_map)} total ({inserted} new)")
        return category_map

    def load_dim_channel(self, df_canales: pd.DataFrame) -> Dict:
        """Load sales channels to DWH using idempotent logic."""
        logger.info(f"Loading {len(df_canales)} channels...")
        channel_map = {}
        inserted = 0

        with self._get_connection() as conn:
            for _, row in df_canales.iterrows():
                nombre = str(row['nombre']).strip()
                channel_type = self.CHANNEL_TYPE_MAP.get(nombre.upper(), 'Other')

                # Check if channel already exists
                existing = conn.execute(
                    "SELECT id FROM DimChannel WHERE name = ?", nombre
                ).fetchone()

                if existing:
                    channel_map[nombre] = int(existing[0])
                else:
                    try:
                        conn.execute("INSERT INTO DimChannel (name, channelType) VALUES (?, ?)",
                                   nombre, channel_type)
                        conn.commit()
                        new_id = conn.execute("SELECT @@IDENTITY").fetchone()[0]
                        channel_map[nombre] = int(new_id)
                        inserted += 1
                    except Exception as e:
                        logger.warning(f"Could not insert channel '{nombre}': {e}")

        logger.info(f"Channels: {len(channel_map)} total ({inserted} new)")
        return channel_map

    def load_dim_customer(self, df_clientes: pd.DataFrame) -> Dict:
        """
        Load customers to DWH using idempotent logic.

        Key: Email is the unique identifier for customer matching.
        Returns mapping: source_key -> DWH customer_id
        """
        logger.info(f"Loading {len(df_clientes)} customers...")
        customer_map = {}
        inserted = 0

        with self._get_connection() as conn:
            for _, row in df_clientes.iterrows():
                source_key = str(row['source_key']).strip()
                email = str(row.get('correo', '')).strip().lower()
                gender_code = self.GENDER_REVERSE_MAP.get(str(row.get('genero', 'No especificado')), 'O')
                name = str(row.get('nombre', '')).strip()
                country = str(row.get('pais', '')).strip()
                created_at = row.get('created_at', datetime.now().date())

                # Check if customer exists (by email)
                if email:
                    existing = conn.execute(
                        "SELECT id FROM DimCustomer WHERE email = ?", email
                    ).fetchone()

                    if existing:
                        customer_map[source_key] = int(existing[0])
                        continue

                # Insert new customer
                try:
                    conn.execute(
                        "INSERT INTO DimCustomer (name, email, gender, country, created_at) VALUES (?, ?, ?, ?, ?)",
                        name, email, gender_code, country, created_at
                    )
                    conn.commit()
                    new_id = conn.execute("SELECT @@IDENTITY").fetchone()[0]
                    customer_map[source_key] = int(new_id)
                    inserted += 1
                except Exception as e:
                    logger.warning(f"Could not insert customer {source_key} ({email}): {e}")

        logger.info(f"Customers: {len(customer_map)} total ({inserted} new)")
        return customer_map

    def load_dim_time(self, df_tiempo: pd.DataFrame) -> Dict:
        """
        Load date dimension to DWH using idempotent logic.

        Returns mapping: date_string -> DWH time_id
        """
        logger.info(f"Loading {len(df_tiempo)} dates...")
        time_map = {}
        inserted = 0

        with self._get_connection() as conn:
            for _, row in df_tiempo.iterrows():
                fecha = row['fecha']
                fecha_str = str(fecha)

                # Check if date already exists
                existing = conn.execute(
                    "SELECT id FROM DimTime WHERE date = ?", fecha
                ).fetchone()

                if existing:
                    time_map[fecha_str] = int(existing[0])
                else:
                    try:
                        conn.execute(
                            "INSERT INTO DimTime (date, year, month, day) VALUES (?, ?, ?, ?)",
                            fecha, int(row['anio']), int(row['mes']), int(row['dia'])
                        )
                        conn.commit()
                        new_id = conn.execute("SELECT @@IDENTITY").fetchone()[0]
                        time_map[fecha_str] = int(new_id)
                        inserted += 1
                    except Exception as e:
                        logger.warning(f"Could not insert date {fecha_str}: {e}")

        logger.info(f"Dates: {len(time_map)} total ({inserted} new)")
        return time_map

    def load_dim_product(self, df_productos: pd.DataFrame, category_map: Dict) -> Dict:
        """
        Load products to DWH using idempotent logic.

        Key: SKU is the unique identifier for product matching.
        Returns mapping: source_key -> DWH product_id
        """
        logger.info(f"Loading {len(df_productos)} products...")
        product_map = {}
        inserted = 0

        with self._get_connection() as conn:
            for _, row in df_productos.iterrows():
                source_key = str(row['source_key']).strip()
                sku = str(row.get('sku_oficial', '')).strip()
                name = str(row.get('nombre', '')).strip()
                categoria = str(row.get('categoria', '')).strip()
                categoria_id = category_map.get(categoria, 1)

                # Check if product exists (by SKU)
                if sku:
                    existing = conn.execute(
                        "SELECT id FROM DimProduct WHERE code = ?", sku
                    ).fetchone()

                    if existing:
                        product_map[source_key] = int(existing[0])
                        continue

                # Insert new product
                try:
                    conn.execute(
                        "INSERT INTO DimProduct (name, code, categoryId) VALUES (?, ?, ?)",
                        name, sku, int(categoria_id)
                    )
                    conn.commit()
                    new_id = conn.execute("SELECT @@IDENTITY").fetchone()[0]
                    product_map[source_key] = int(new_id)
                    inserted += 1
                except Exception as e:
                    logger.warning(f"Could not insert product {source_key} (SKU: {sku}): {e}")

        logger.info(f"Products: {len(product_map)} total ({inserted} new)")
        return product_map

    def load_dim_order(self, df_ordenes: pd.DataFrame) -> Dict:
        """
        Load order dimension to DWH.

        Each source order creates a new DWH order record (not idempotent - orders are new data).
        Returns mapping: source_key -> DWH order_id
        """
        logger.info(f"Loading {len(df_ordenes)} orders...")
        order_map = {}
        inserted = 0
        skipped = 0

        with self._get_connection() as conn:
            for _, row in df_ordenes.iterrows():
                source_key = str(row['source_key']).strip()
                total_usd = float(row.get('total_usd', 0))

                try:
                    conn.execute("INSERT INTO DimOrder (totalOrderUSD) VALUES (?)", total_usd)
                    conn.commit()
                    new_id = conn.execute("SELECT @@IDENTITY").fetchone()[0]
                    order_map[source_key] = int(new_id)
                    inserted += 1
                except Exception as e:
                    logger.warning(f"Could not insert order {source_key}: {e}")
                    skipped += 1

        logger.info(f"Orders: {inserted} inserted, {skipped} skipped")
        return order_map

    # ========================================================================
    # FACT TABLE LOADING - VALIDATE FOREIGN KEYS
    # ========================================================================

    def load_fact_sales(self, df_fact: pd.DataFrame,
                       product_map: Dict, time_map: Dict, order_map: Dict,
                       channel_map: Dict, customer_map: Dict) -> int:
        """
        Load fact sales table with referential integrity validation.

        Each row requires:
        - Valid product_id (in product_map)
        - Valid customer_id (in customer_map)
        - Valid order_id (in order_map)
        - Valid time_id (in time_map or defaults to 1)
        - Valid channel_id (in channel_map or defaults to 1)

        Rows missing required foreign keys are skipped with logging.

        Returns count of successfully loaded rows.
        """
        logger.info(f"Loading {len(df_fact)} fact sales rows...")
        loaded = 0
        skipped = 0
        missing_product = 0
        missing_customer = 0
        missing_order = 0
        crc_with_dw_rate = 0
        crc_with_default_rate = 0

        with self._get_connection() as conn:
            for idx, row in df_fact.iterrows():
                # Map source keys to DWH dimension IDs
                producto_source_key = str(row.get('producto_source_key', '')).strip()
                cliente_source_key = str(row.get('cliente_source_key', '')).strip()
                orden_source_key = str(row.get('orden_source_key', '')).strip()

                product_id = product_map.get(producto_source_key)
                customer_id = customer_map.get(cliente_source_key)
                order_id = order_map.get(orden_source_key)

                # Map date to time dimension
                fecha = str(row.get('fecha', '')).split()[0] if row.get('fecha') else None
                time_id = time_map.get(fecha, 1) if fecha else 1

                # Map channel
                canal = str(row.get('canal', '')).strip()
                channel_id = channel_map.get(canal, 1)

                # Validate all required foreign keys exist
                if not product_id:
                    missing_product += 1
                    skipped += 1
                    continue

                if not customer_id:
                    missing_customer += 1
                    skipped += 1
                    continue

                if not order_id:
                    missing_order += 1
                    skipped += 1
                    continue

                # Get exchange rate ID if currency conversion happened
                exchange_rate_id = None
                moneda = str(row.get('moneda', '')).strip()
                if moneda == 'CRC':
                    try:
                        # Look up exchange rate for this date
                        fecha_for_rate = str(row.get('fecha', '')).split()[0] if row.get('fecha') else None
                        if fecha_for_rate:
                            rate_result = conn.execute(
                                """SELECT id FROM DimExchangeRate
                                   WHERE fromCurrency = ? AND toCurrency = ? AND date <= ?
                                   ORDER BY date DESC""",
                                'CRC', 'USD', fecha_for_rate
                            ).fetchone()
                            if rate_result:
                                exchange_rate_id = int(rate_result[0])
                                crc_with_dw_rate += 1
                            else:
                                crc_with_default_rate += 1
                    except Exception as e:
                        logger.warning(f"Could not find exchange rate for row {idx}: {e}")

                # Insert fact row
                try:
                    conn.execute(
                        """INSERT INTO FactSales (productId, timeId, orderId, channelId, customerId,
                                                  productCant, productUnitPriceUSD, lineTotalUSD,
                                                  discountPercentage, exchangeRateId, created_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        int(product_id), int(time_id), int(order_id), int(channel_id), int(customer_id),
                        int(row.get('cantidad', 1)),
                        float(row.get('precio_unit_usd', 0)),
                        float(row.get('monto_linea', 0)),
                        float(row.get('discount_percentage', 0)),
                        exchange_rate_id,
                        datetime.now()
                    )
                    conn.commit()
                    loaded += 1
                except Exception as e:
                    logger.warning(f"Row {idx} failed to insert: {e}")
                    skipped += 1

        logger.info(f"FactSales: {loaded} inserted, {skipped} skipped")
        if missing_product > 0:
            logger.warning(f"  - {missing_product} rows missing product references")
        if missing_customer > 0:
            logger.warning(f"  - {missing_customer} rows missing customer references")
        if missing_order > 0:
            logger.warning(f"  - {missing_order} rows missing order references")

        # Report exchange rate usage
        if crc_with_dw_rate > 0 or crc_with_default_rate > 0:
            logger.info(f"\nExchange Rate Summary (CRC->USD):")
            if crc_with_dw_rate > 0:
                logger.info(f"  - {crc_with_dw_rate} rows: Used DWH exchange rates")
            if crc_with_default_rate > 0:
                logger.warning(f"  - {crc_with_default_rate} rows: Used default rate (515.0)")

        return loaded

    # ========================================================================
    # STAGING TABLES - TRACEABILITY
    # ========================================================================

    def load_staging_product_mapping(self, df_mapping: pd.DataFrame) -> None:
        """
        Load product mapping to staging table (Rule 1 traceability).

        Links source system codes to official SKU for cross-source matching.
        Uses idempotent logic - skips if mapping already exists.
        """
        logger.info(f"Loading {len(df_mapping)} product mappings...")
        inserted = 0
        skipped = 0

        with self._get_connection() as conn:
            for _, row in df_mapping.iterrows():
                source_system = str(row.get('source_system', '')).strip()
                source_code = str(row.get('source_code', '')).strip()

                # Check if mapping already exists
                existing = conn.execute(
                    "SELECT map_id FROM staging_map_producto WHERE source_system = ? AND source_code = ?",
                    source_system, source_code
                ).fetchone()

                if existing:
                    skipped += 1
                    continue

                # Insert new mapping
                try:
                    conn.execute(
                        """INSERT INTO staging_map_producto (source_system, source_code, sku_oficial, descripcion, created_at)
                           VALUES (?, ?, ?, ?, ?)""",
                        source_system, source_code,
                        str(row.get('sku_oficial', '')).strip(),
                        str(row.get('descripcion', '')).strip(),
                        datetime.now()
                    )
                    conn.commit()
                    inserted += 1
                except Exception as e:
                    logger.warning(f"Could not insert mapping {source_system}/{source_code}: {e}")

        logger.info(f"Product mappings: {inserted} new, {skipped} existing")

    def load_staging_exchange_rates(self) -> None:
        """
        Verify exchange rates available in DW (Rule 2 traceability).

        Checks if DimExchangeRate table has data for currency conversions.
        """
        logger.info("Checking available exchange rates...")

        try:
            with self._get_connection() as conn:
                count = conn.execute("SELECT COUNT(*) FROM DimExchangeRate").fetchone()[0]

                if count > 0:
                    logger.info(f"Found {count} exchange rates in DWH")
                else:
                    logger.warning("No exchange rates found - conversions used default rates")
        except Exception as e:
            logger.warning(f"Could not verify exchange rates: {e}")

    def load_source_tracking(self, table_name: str, df_data: pd.DataFrame) -> None:
        """
        Record source system traceability (informational).

        Logs which dimension table loaded how many records from which source.
        """
        logger.info(f"Tracking {len(df_data)} records for {table_name}")
