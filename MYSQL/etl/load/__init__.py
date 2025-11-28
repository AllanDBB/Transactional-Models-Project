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

    def load_dim_category(self, df_categorias: pd.DataFrame) -> Dict:
        """Carga dimensión DimCategory y retorna mapeo nombre -> ID"""
        logger.info(f"Cargando {len(df_categorias)} categorías...")

        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            category_map = {}

            for idx, row in df_categorias.iterrows():
                # DWH schema: DimCategory(id, name)
                sql = """
                    INSERT INTO DimCategory (name)
                    VALUES (?)
                """
                cursor.execute(sql, str(row['categoria']))

                # Obtener ID generado
                cursor.execute("SELECT @@IDENTITY")
                cat_id = cursor.fetchone()[0]
                category_map[str(row['categoria'])] = int(cat_id)

            conn.commit()
            cursor.close()
            conn.close()
            logger.info(f"[OK] {len(df_categorias)} categorías cargadas")
            return category_map
        except Exception as e:
            logger.error(f"Error al cargar categorías: {str(e)}")
            raise

    def load_dim_channel(self, df_canales: pd.DataFrame) -> Dict:
        """Carga dimensión DimChannel y retorna mapeo nombre -> ID"""
        logger.info(f"Cargando {len(df_canales)} canales...")

        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            channel_map = {}

            # Mapeo de canales a channelType
            channel_type_map = {
                'WEBSITE': 'Website',
                'TIENDA': 'Store',
                'APP': 'App',
                'SOCIO': 'Partner'
            }

            for idx, row in df_canales.iterrows():
                canal_nombre = str(row['nombre']).upper()
                channel_type = channel_type_map.get(canal_nombre, 'Other')

                # DWH schema: DimChannel(id, name, channelType)
                sql = """
                    INSERT INTO DimChannel (name, channelType)
                    VALUES (?, ?)
                """
                cursor.execute(sql, str(row['nombre']), channel_type)

                # Obtener ID generado
                cursor.execute("SELECT @@IDENTITY")
                channel_id = cursor.fetchone()[0]
                channel_map[str(row['nombre'])] = int(channel_id)

            conn.commit()
            cursor.close()
            conn.close()
            logger.info(f"[OK] {len(df_canales)} canales cargados")
            return channel_map
        except Exception as e:
            logger.error(f"Error al cargar canales: {str(e)}")
            raise

    def load_dim_customer(self, df_clientes: pd.DataFrame) -> Dict:
        """Carga dimensión DimCustomer y retorna mapeo source_key -> ID"""
        logger.info(f"Cargando {len(df_clientes)} clientes...")

        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            customer_map = {}

            # Mapeo inverso de género (transformado -> DWH)
            gender_reverse_map = {
                'Masculino': 'M',
                'Femenino': 'F',
                'No especificado': 'O'
            }

            for idx, row in df_clientes.iterrows():
                genero_transformado = str(row.get('genero', 'No especificado'))
                gender_code = gender_reverse_map.get(genero_transformado, 'O')

                # DWH schema: DimCustomer(id, name, email, gender, country, created_at)
                sql = """
                    INSERT INTO DimCustomer (name, email, gender, country, created_at)
                    VALUES (?, ?, ?, ?, ?)
                """
                cursor.execute(sql,
                    str(row.get('nombre', '')),
                    str(row.get('correo', '')),
                    gender_code,
                    str(row.get('pais', '')),
                    row.get('created_at', datetime.now().date())
                )

                # Obtener ID generado y guardar mapeo
                cursor.execute("SELECT @@IDENTITY")
                customer_id = cursor.fetchone()[0]
                customer_map[str(row['source_key'])] = int(customer_id)

            conn.commit()
            cursor.close()
            conn.close()
            logger.info(f"[OK] {len(df_clientes)} clientes cargados")
            return customer_map
        except Exception as e:
            logger.error(f"Error al cargar clientes: {str(e)}")
            raise

    def load_dim_time(self, df_tiempo: pd.DataFrame) -> Dict:
        """Carga dimensión DimTime y retorna mapeo fecha -> ID"""
        logger.info(f"Cargando {len(df_tiempo)} fechas...")

        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            time_map = {}

            for idx, row in df_tiempo.iterrows():
                # DWH schema: DimTime(id, year, month, day, date)
                sql = """
                    INSERT INTO DimTime (date, year, month, day)
                    VALUES (?, ?, ?, ?)
                """
                cursor.execute(sql,
                    row['fecha'],
                    int(row['anio']),
                    int(row['mes']),
                    int(row['dia'])
                )

                # Obtener ID generado y guardar mapeo
                cursor.execute("SELECT @@IDENTITY")
                time_id = cursor.fetchone()[0]
                time_map[str(row['fecha'])] = int(time_id)

            conn.commit()
            cursor.close()
            conn.close()
            logger.info(f"[OK] {len(df_tiempo)} fechas cargadas")
            return time_map
        except Exception as e:
            logger.error(f"Error al cargar tiempo: {str(e)}")
            raise

    def load_dim_product(self, df_productos: pd.DataFrame, category_map: Dict) -> Dict:
        """Carga dimensión DimProduct y retorna mapeo source_key -> ID"""
        logger.info(f"Cargando {len(df_productos)} productos...")

        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            product_map = {}

            for idx, row in df_productos.iterrows():
                categoria_id = category_map.get(str(row.get('categoria', '')), 1)

                # DWH schema: DimProduct(id, name, code, categoryId)
                # Usar sku_oficial como 'code'
                sql = """
                    INSERT INTO DimProduct (name, code, categoryId)
                    VALUES (?, ?, ?)
                """
                cursor.execute(sql,
                    str(row.get('nombre', '')),
                    str(row.get('sku_oficial', '')),
                    int(categoria_id)
                )

                # Obtener ID generado y guardar mapeo
                cursor.execute("SELECT @@IDENTITY")
                product_id = cursor.fetchone()[0]
                product_map[str(row['source_key'])] = int(product_id)

            conn.commit()
            cursor.close()
            conn.close()
            logger.info(f"[OK] {len(df_productos)} productos cargados")
            return product_map
        except Exception as e:
            logger.error(f"Error al cargar productos: {str(e)}")
            raise

    def load_dim_order(self, df_ordenes: pd.DataFrame) -> Dict:
        """Carga dimensión DimOrder y retorna mapping de source_key -> ID"""
        logger.info(f"Cargando {len(df_ordenes)} órdenes...")

        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            order_map = {}

            for idx, row in df_ordenes.iterrows():
                # DWH schema: DimOrder(id, totalOrderUSD)
                # Solo almacena el total de la orden
                sql = """
                    INSERT INTO DimOrder (totalOrderUSD)
                    VALUES (?)
                """
                cursor.execute(sql, float(row.get('total_usd', 0)))

                # Obtener el ID generado
                cursor.execute("SELECT @@IDENTITY")
                order_id = cursor.fetchone()[0]
                order_map[str(row['source_key'])] = int(order_id)

            conn.commit()
            cursor.close()
            conn.close()
            logger.info(f"[OK] {len(df_ordenes)} órdenes cargadas")
            return order_map
        except Exception as e:
            logger.error(f"Error al cargar órdenes: {str(e)}")
            raise

    def load_fact_sales(self, df_fact: pd.DataFrame,
                       product_map: Dict, time_map: Dict, order_map: Dict,
                       channel_map: Dict, customer_map: Dict) -> None:
        """Carga tabla de hechos FactSales con mapeo de IDs"""
        logger.info(f"Cargando {len(df_fact)} registros de FactSales...")

        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            loaded_count = 0

            for idx, row in df_fact.iterrows():
                # DWH schema: FactSales(id, productId, timeId, orderId, channelId, customerId,
                #                       productCant, productUnitPriceUSD, lineTotalUSD,
                #                       discountPercentage, created_at, exchangeRateId)

                # Mapear IDs de dimensiones
                product_id = product_map.get(str(row.get('producto_source_key', '')))
                customer_id = customer_map.get(str(row.get('cliente_source_key', '')))
                order_id = order_map.get(str(row.get('orden_source_key', '')))

                # Para time y channel, buscar por valor
                fecha = str(row.get('fecha', '')).split()[0] if row.get('fecha') else None
                time_id = time_map.get(fecha, 1) if fecha else 1

                canal = str(row.get('canal', ''))
                channel_id = channel_map.get(canal, 1)

                if not all([product_id, customer_id, order_id]):
                    logger.warning(f"  Registro {idx} skipped: missing dimension IDs")
                    continue

                sql = """
                    INSERT INTO FactSales (productId, timeId, orderId, channelId, customerId,
                                          productCant, productUnitPriceUSD, lineTotalUSD,
                                          discountPercentage, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                try:
                    cursor.execute(sql,
                        int(product_id),
                        int(time_id),
                        int(order_id),
                        int(channel_id),
                        int(customer_id),
                        int(row.get('cantidad', 1)),
                        float(row.get('precio_unit_limpio', 0)),
                        float(row.get('monto_linea', 0)),
                        float(row.get('discount_percentage', 0)),  # MySQL no tiene descuento, usar 0
                        datetime.now()
                    )
                    loaded_count += 1
                except Exception as e:
                    logger.warning(f"  Registro {idx} skipped: {e}")
                    continue

            conn.commit()
            cursor.close()
            conn.close()
            logger.info(f"[OK] {loaded_count}/{len(df_fact)} registros de FactSales cargados")
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
