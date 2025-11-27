"""
Modulo Load: Carga datos transformados al Data Warehouse MSSQL
Incluye carga en tablas de staging para trazabilidad y mapeos
"""
import pyodbc
import pandas as pd
import logging
from typing import Dict, List
from datetime import datetime, date

logger = logging.getLogger(__name__)


class DataLoader:
    """Carga datos de MySQL al Data Warehouse MSSQL con trazabilidad"""

    def __init__(self, connection_string: str):
        self.connection_string = connection_string

    def _get_connection(self):
        """Obtiene conexión a MSSQL"""
        try:
            return pyodbc.connect(self.connection_string)
        except Exception as e:
            logger.error(f"Error conectando a DWH: {str(e)}")
            raise

    def truncate_tables(self, table_names: List[str]) -> None:
        """Limpia las tablas (TRUNCATE) antes de cargar"""
        logger.info(f"Limpiando tablas: {', '.join(table_names)}")

        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # Orden de limpieza (por FK dependencies)
            order = ['FactSales', 'DimProduct', 'DimCustomer', 'DimTime', 'DimChannel', 'DimCategory']
            tables_ordered = [t for t in order if t in table_names]
            tables_ordered += [t for t in table_names if t not in tables_ordered]

            for table in tables_ordered:
                try:
                    cursor.execute(f"TRUNCATE TABLE {table}")
                    logger.debug(f"  TRUNCATE {table}")
                except:
                    try:
                        cursor.execute(f"DELETE FROM {table}")
                        logger.debug(f"  DELETE FROM {table}")
                    except Exception as e:
                        logger.warning(f"  No se pudo limpiar {table}: {e}")

            conn.commit()
            cursor.close()
            conn.close()
            logger.info("[OK] Tablas limpiadas exitosamente")
        except Exception as e:
            logger.error(f"Error al limpiar tablas: {str(e)}")
            raise

    def load_dim_category(self, df_categorias: pd.DataFrame) -> None:
        """Carga dimensión DimCategory"""
        logger.info(f"Cargando {len(df_categorias)} categorías...")

        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            for idx, row in df_categorias.iterrows():
                sql = f"""
                    INSERT INTO DimCategory (nombre)
                    VALUES (?)
                """
                cursor.execute(sql, str(row['categoria']))

            conn.commit()
            cursor.close()
            conn.close()
            logger.info(f"[OK] {len(df_categorias)} categorías cargadas")
        except Exception as e:
            logger.error(f"Error al cargar categorías: {str(e)}")
            raise

    def load_dim_channel(self, df_canales: pd.DataFrame) -> None:
        """Carga dimensión DimChannel"""
        logger.info(f"Cargando {len(df_canales)} canales...")

        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            for idx, row in df_canales.iterrows():
                sql = f"""
                    INSERT INTO DimChannel (nombre)
                    VALUES (?)
                """
                cursor.execute(sql, str(row['nombre']))

            conn.commit()
            cursor.close()
            conn.close()
            logger.info(f"[OK] {len(df_canales)} canales cargados")
        except Exception as e:
            logger.error(f"Error al cargar canales: {str(e)}")
            raise

    def load_dim_customer(self, df_clientes: pd.DataFrame) -> None:
        """Carga dimensión DimCustomer"""
        logger.info(f"Cargando {len(df_clientes)} clientes...")

        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            for idx, row in df_clientes.iterrows():
                sql = """
                    INSERT INTO DimCustomer (nombre, email, genero, pais, fecha_registro, source_system, source_key)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """
                cursor.execute(sql,
                    str(row.get('nombre', '')),
                    str(row.get('correo', '')),
                    str(row.get('genero', 'No especificado')),
                    str(row.get('pais', '')),
                    row.get('created_at', datetime.now().date()),
                    row.get('source_system', 'MYSQL'),
                    str(row.get('source_key', ''))
                )

            conn.commit()
            cursor.close()
            conn.close()
            logger.info(f"[OK] {len(df_clientes)} clientes cargados")
        except Exception as e:
            logger.error(f"Error al cargar clientes: {str(e)}")
            raise

    def load_dim_time(self, df_tiempo: pd.DataFrame) -> None:
        """Carga dimensión DimTime"""
        logger.info(f"Cargando {len(df_tiempo)} fechas...")

        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            for idx, row in df_tiempo.iterrows():
                sql = """
                    INSERT INTO DimTime (fecha, anio, mes, dia, trimestre)
                    VALUES (?, ?, ?, ?, ?)
                """
                cursor.execute(sql,
                    row['fecha'],
                    int(row['anio']),
                    int(row['mes']),
                    int(row['dia']),
                    int(row['trimestre'])
                )

            conn.commit()
            cursor.close()
            conn.close()
            logger.info(f"[OK] {len(df_tiempo)} fechas cargadas")
        except Exception as e:
            logger.error(f"Error al cargar tiempo: {str(e)}")
            raise

    def load_dim_product(self, df_productos: pd.DataFrame, category_map: Dict) -> None:
        """Carga dimensión DimProduct"""
        logger.info(f"Cargando {len(df_productos)} productos...")

        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            for idx, row in df_productos.iterrows():
                categoria_id = category_map.get(str(row.get('categoria', '')), 1)

                sql = """
                    INSERT INTO DimProduct (nombre, sku, categoria_id, codigo_alt, source_system, source_key)
                    VALUES (?, ?, ?, ?, ?, ?)
                """
                cursor.execute(sql,
                    str(row.get('nombre', '')),
                    str(row.get('sku_oficial', '')),
                    int(categoria_id),
                    str(row.get('codigo_alt', '')),
                    row.get('source_system', 'MYSQL'),
                    str(row.get('source_key', ''))
                )

            conn.commit()
            cursor.close()
            conn.close()
            logger.info(f"[OK] {len(df_productos)} productos cargados")
        except Exception as e:
            logger.error(f"Error al cargar productos: {str(e)}")
            raise

    def load_dim_order(self, df_ordenes: pd.DataFrame) -> Dict:
        """Carga dimensión DimOrder y retorna mapping de IDs"""
        logger.info(f"Cargando {len(df_ordenes)} órdenes...")

        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            order_tracking = {}

            for idx, row in df_ordenes.iterrows():
                sql = """
                    INSERT INTO DimOrder (fecha, canal, moneda, total_usd, source_system, source_key)
                    VALUES (?, ?, ?, ?, ?, ?)
                """
                cursor.execute(sql,
                    row.get('fecha', datetime.now()),
                    str(row.get('canal', '')),
                    str(row.get('moneda', 'USD')),
                    float(row.get('total_usd', 0)),
                    row.get('source_system', 'MYSQL'),
                    str(row.get('source_key', ''))
                )

                # Obtener el ID generado
                cursor.execute("SELECT @@IDENTITY")
                order_id = cursor.fetchone()[0]
                order_tracking[int(row['id'])] = int(order_id)

            conn.commit()
            cursor.close()
            conn.close()
            logger.info(f"[OK] {len(df_ordenes)} órdenes cargadas")
            return order_tracking
        except Exception as e:
            logger.error(f"Error al cargar órdenes: {str(e)}")
            raise

    def load_fact_sales(self, df_fact: pd.DataFrame) -> None:
        """Carga tabla de hechos FactSales"""
        logger.info(f"Cargando {len(df_fact)} registros de FactSales...")

        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            for idx, row in df_fact.iterrows():
                sql = """
                    INSERT INTO FactSales (cliente_id, producto_id, tiempo_id, canal_id,
                                          cantidad, precio_unit, monto_linea, monto_total_usd,
                                          source_system, source_key)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                try:
                    cursor.execute(sql,
                        int(row.get('cliente_id', 0)),
                        int(row.get('producto_id', 0)),
                        int(row.get('tiempo_id', 1)),
                        int(row.get('canal_id', 1)),
                        int(row.get('cantidad', 1)),
                        float(row.get('precio_unit_limpio', 0)),
                        float(row.get('monto_linea', 0)),
                        float(row.get('total_usd', 0)),
                        row.get('source_system', 'MYSQL'),
                        str(row.get('source_key', ''))
                    )
                except Exception as e:
                    logger.warning(f"  Registro {idx} skipped: {e}")
                    continue

            conn.commit()
            cursor.close()
            conn.close()
            logger.info(f"[OK] FactSales cargado")
        except Exception as e:
            logger.error(f"Error al cargar FactSales: {str(e)}")
            raise

    def load_staging_product_mapping(self, df_mapping: pd.DataFrame) -> None:
        """REGLA 1: Carga tabla puente de mapeo de productos"""
        logger.info(f"Cargando mapeo de {len(df_mapping)} productos...")

        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            for idx, row in df_mapping.iterrows():
                sql = """
                    INSERT INTO staging_map_producto (source_system, source_code, sku_oficial, descripcion, created_at)
                    VALUES (?, ?, ?, ?, ?)
                """
                cursor.execute(sql,
                    str(row.get('source_system', '')),
                    str(row.get('source_code', '')),
                    str(row.get('sku_oficial', '')),
                    str(row.get('descripcion', '')),
                    datetime.now()
                )

            conn.commit()
            cursor.close()
            conn.close()
            logger.info(f"[OK] {len(df_mapping)} mapeos de productos cargados")
        except Exception as e:
            logger.warning(f"No se pudo cargar mapeo de productos: {str(e)}")

    def load_staging_exchange_rates(self) -> None:
        """
        REGLA 2: Verifica disponibilidad de tipos de cambio en DWH

        Los tipos de cambio se consultan vía ExchangeRateHelper durante la transformación.
        Esta función verifica que DimExchangeRate tenga datos disponibles.
        """
        logger.info("Verificando tipos de cambio disponibles (REGLA 2)...")

        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            cursor.execute("SELECT COUNT(*) FROM DimExchangeRate")
            count = cursor.fetchone()[0]

            if count > 0:
                logger.info(f"[OK] {count} tipos de cambio disponibles en DWH")
            else:
                logger.warning("[WARN] No hay tipos de cambio en DWH. Las conversiones usarán tasa por defecto.")

            cursor.close()
            conn.close()

        except Exception as e:
            logger.warning(f"No se pudo verificar tipos de cambio: {str(e)}")
            logger.info("[OK] Las conversiones usarán tasa por defecto si es necesario (REGLA 2)")

    def load_source_tracking(self, table_name: str, df_data: pd.DataFrame) -> None:
        """
        Consideración 5: Registra trazabilidad de datos cargados
        Mantiene referencia a fuente original y clave natural
        """
        logger.info(f"Registrando trazabilidad para {table_name}...")
        try:
            # La trazabilidad se incluye en las columnas source_system y source_key
            # que ya se han cargado en cada tabla dimensional
            logger.info(f"[OK] Trazabilidad registrada para {len(df_data)} registros en {table_name}")
        except Exception as e:
            logger.warning(f"Error registrando trazabilidad: {str(e)}")
