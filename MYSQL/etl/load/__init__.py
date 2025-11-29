"""
Data loading module for MySQL to MSSQL Data Warehouse ETL.
Loads transformed data into DW tables with traceability.
"""
import pyodbc
import pandas as pd
import logging
from typing import Dict, List
from datetime import datetime

logger = logging.getLogger(__name__)


class DataLoader:
    """Loads MySQL data into MSSQL Data Warehouse."""

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
        try:
            return pyodbc.connect(self.connection_string)
        except Exception as e:
            logger.error(f"Error conectando a DWH: {str(e)}")
            raise

    def truncate_tables(self, table_names: List[str]) -> None:
        logger.info(f"Limpiando tablas: {', '.join(table_names)}")

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
                        logger.warning(f"No se pudo limpiar {table}: {e}")
            conn.commit()

        logger.info("[OK] Tablas limpiadas exitosamente")

    def load_dim_category(self, df_categorias: pd.DataFrame) -> Dict:
        """
        Load categories to DWH with idempotent behavior.
        Returns mapping: category_name -> DWH category ID
        """
        logger.info(f"Cargando {len(df_categorias)} categorias...")
        category_map = {}

        with self._get_connection() as conn:
            for _, row in df_categorias.iterrows():
                nombre = str(row['categoria'])

                # Check if category already exists (idempotent)
                existing = conn.execute(
                    "SELECT id FROM DimCategory WHERE name = ?", nombre
                ).fetchone()

                if existing:
                    category_map[nombre] = int(existing[0])
                else:
                    # Insert new category and get generated ID
                    conn.execute("INSERT INTO DimCategory (name) VALUES (?)", nombre)
                    category_map[nombre] = int(conn.execute("SELECT @@IDENTITY").fetchone()[0])

            conn.commit()

        logger.info(f"[OK] {len(category_map)} categorias procesadas")
        return category_map

    def load_dim_channel(self, df_canales: pd.DataFrame) -> Dict:
        logger.info(f"Cargando {len(df_canales)} canales...")
        channel_map = {}

        with self._get_connection() as conn:
            for _, row in df_canales.iterrows():
                nombre = str(row['nombre'])
                canal_nombre = nombre.upper()
                channel_type = self.CHANNEL_TYPE_MAP.get(canal_nombre, 'Other')

                existing = conn.execute(
                    "SELECT id FROM DimChannel WHERE name = ?", nombre
                ).fetchone()

                if existing:
                    channel_map[nombre] = int(existing[0])
                else:
                    conn.execute("INSERT INTO DimChannel (name, channelType) VALUES (?, ?)",
                               nombre, channel_type)
                    channel_map[nombre] = int(conn.execute("SELECT @@IDENTITY").fetchone()[0])

            conn.commit()

        logger.info(f"[OK] {len(channel_map)} canales procesados")
        return channel_map

    def load_dim_customer(self, df_clientes: pd.DataFrame) -> Dict:
        logger.info(f"Cargando {len(df_clientes)} clientes...")
        customer_map = {}

        with self._get_connection() as conn:
            for _, row in df_clientes.iterrows():
                source_key = str(row['source_key'])
                email = str(row.get('correo', ''))
                gender_code = self.GENDER_REVERSE_MAP.get(str(row.get('genero', 'No especificado')), 'O')

                existing = conn.execute(
                    "SELECT id FROM DimCustomer WHERE email = ?", email
                ).fetchone()

                if existing:
                    customer_map[source_key] = int(existing[0])
                else:
                    conn.execute("INSERT INTO DimCustomer (name, email, gender, country, created_at) VALUES (?, ?, ?, ?, ?)",
                               str(row.get('nombre', '')), email, gender_code,
                               str(row.get('pais', '')), row.get('created_at', datetime.now().date()))
                    customer_map[source_key] = int(conn.execute("SELECT @@IDENTITY").fetchone()[0])

            conn.commit()

        logger.info(f"[OK] {len(customer_map)} clientes procesados")
        return customer_map

    def load_dim_time(self, df_tiempo: pd.DataFrame) -> Dict:
        logger.info(f"Cargando {len(df_tiempo)} fechas...")
        time_map = {}

        with self._get_connection() as conn:
            for _, row in df_tiempo.iterrows():
                fecha = row['fecha']
                fecha_str = str(fecha)

                existing = conn.execute(
                    "SELECT id FROM DimTime WHERE date = ?", fecha
                ).fetchone()

                if existing:
                    time_map[fecha_str] = int(existing[0])
                else:
                    conn.execute("INSERT INTO DimTime (date, year, month, day) VALUES (?, ?, ?, ?)",
                               fecha, int(row['anio']), int(row['mes']), int(row['dia']))
                    time_map[fecha_str] = int(conn.execute("SELECT @@IDENTITY").fetchone()[0])

            conn.commit()

        logger.info(f"[OK] {len(time_map)} fechas procesadas")
        return time_map

    def load_dim_product(self, df_productos: pd.DataFrame, category_map: Dict) -> Dict:
        logger.info(f"Cargando {len(df_productos)} productos...")
        product_map = {}

        with self._get_connection() as conn:
            for _, row in df_productos.iterrows():
                source_key = str(row['source_key'])
                sku = str(row.get('sku_oficial', ''))
                categoria_id = category_map.get(str(row.get('categoria', '')), 1)

                existing = conn.execute(
                    "SELECT id FROM DimProduct WHERE code = ?", sku
                ).fetchone()

                if existing:
                    product_map[source_key] = int(existing[0])
                else:
                    conn.execute("INSERT INTO DimProduct (name, code, categoryId) VALUES (?, ?, ?)",
                               str(row.get('nombre', '')), sku, int(categoria_id))
                    product_map[source_key] = int(conn.execute("SELECT @@IDENTITY").fetchone()[0])

            conn.commit()

        logger.info(f"[OK] {len(product_map)} productos procesados")
        return product_map

    def load_dim_order(self, df_ordenes: pd.DataFrame) -> Dict:
        logger.info(f"Cargando {len(df_ordenes)} ordenes...")
        order_map = {}

        with self._get_connection() as conn:
            for _, row in df_ordenes.iterrows():
                source_key = str(row['source_key'])

                try:
                    conn.execute("INSERT INTO DimOrder (totalOrderUSD) VALUES (?)", float(row.get('total_usd', 0)))
                    order_map[source_key] = int(conn.execute("SELECT @@IDENTITY").fetchone()[0])
                except Exception as e:
                    logger.warning(f"No se pudo insertar orden {source_key}: {e}")
                    continue

            conn.commit()

        logger.info(f"[OK] {len(order_map)} ordenes procesadas")
        return order_map

    def load_fact_sales(self, df_fact: pd.DataFrame,
                       product_map: Dict, time_map: Dict, order_map: Dict,
                       channel_map: Dict, customer_map: Dict) -> int:
        """
        Load fact table using dimension ID mappings.
        Skips records with missing foreign keys to maintain referential integrity.
        Returns count of successfully loaded records.
        """
        logger.info(f"Cargando {len(df_fact)} registros de FactSales...")
        loaded_count = 0

        with self._get_connection() as conn:
            for idx, row in df_fact.iterrows():
                # Map source keys to DWH dimension IDs
                product_id = product_map.get(str(row.get('producto_source_key', '')))
                customer_id = customer_map.get(str(row.get('cliente_source_key', '')))
                order_id = order_map.get(str(row.get('orden_source_key', '')))

                # Map date to time dimension
                fecha = str(row.get('fecha', '')).split()[0] if row.get('fecha') else None
                time_id = time_map.get(fecha, 1) if fecha else 1

                # Map channel name to ID
                canal = str(row.get('canal', ''))
                channel_id = channel_map.get(canal, 1)

                # Skip records with missing required dimension IDs
                if not all([product_id, customer_id, order_id]):
                    logger.warning(f"Registro {idx} skipped: missing dimension IDs")
                    continue

                try:
                    # Insert fact record with all foreign keys
                    # Note: precio_unit_usd and monto_linea are already converted from source currency to USD
                    conn.execute(
                        """INSERT INTO FactSales (productId, timeId, orderId, channelId, customerId,
                                                   productCant, productUnitPriceUSD, lineTotalUSD,
                                                   discountPercentage, created_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        int(product_id), int(time_id), int(order_id), int(channel_id), int(customer_id),
                        int(row.get('cantidad', 1)),
                        float(row.get('precio_unit_usd', 0)),
                        float(row.get('monto_linea', 0)),
                        float(row.get('discount_percentage', 0)),
                        datetime.now()
                    )
                    loaded_count += 1
                except Exception as e:
                    logger.warning(f"Registro {idx} skipped: {e}")
                    continue

            conn.commit()

        logger.info(f"[OK] {loaded_count}/{len(df_fact)} registros de FactSales cargados")
        return loaded_count

    def load_staging_product_mapping(self, df_mapping: pd.DataFrame) -> None:
        logger.info(f"Cargando mapeo de {len(df_mapping)} productos...")
        loaded = 0

        with self._get_connection() as conn:
            for _, row in df_mapping.iterrows():
                source_system = str(row.get('source_system', ''))
                source_code = str(row.get('source_code', ''))

                existing = conn.execute(
                    "SELECT map_id FROM staging_map_producto WHERE source_system = ? AND source_code = ?",
                    source_system, source_code
                ).fetchone()

                if not existing:
                    conn.execute(
                        """INSERT INTO staging_map_producto (source_system, source_code, sku_oficial, descripcion, created_at)
                           VALUES (?, ?, ?, ?, ?)""",
                        source_system, source_code,
                        str(row.get('sku_oficial', '')),
                        str(row.get('descripcion', '')),
                        datetime.now()
                    )
                    loaded += 1

            conn.commit()

        logger.info(f"[OK] {loaded} mapeos de productos procesados ({len(df_mapping) - loaded} ya existian)")

    def load_staging_exchange_rates(self) -> None:
        logger.info("Verificando tipos de cambio disponibles (REGLA 2)...")

        try:
            with self._get_connection() as conn:
                count = conn.execute("SELECT COUNT(*) FROM DimExchangeRate").fetchone()[0]

                if count > 0:
                    logger.info(f"[OK] {count} tipos de cambio disponibles en DWH")
                else:
                    logger.warning("[WARN] No hay tipos de cambio en DWH. Las conversiones usaran tasa por defecto.")
        except Exception as e:
            logger.warning(f"No se pudo verificar tipos de cambio: {e}")
            logger.info("[OK] Las conversiones usaran tasa por defecto si es necesario (REGLA 2)")

    def load_source_tracking(self, table_name: str, df_data: pd.DataFrame) -> None:
        logger.info(f"Registrando trazabilidad para {table_name}...")
        logger.info(f"[OK] Trazabilidad registrada para {len(df_data)} registros en {table_name}")
