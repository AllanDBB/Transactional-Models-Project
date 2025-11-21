"""
Modulo Transform: Aplica reglas de transformacion a los datos de MySQL
Reglas de Integracion (ETL):
1. Homologacion de productos: codigo_alt -> SKU oficial (tabla puente)
2. Normalizacion de moneda: convertir CRC a USD con tabla de tipo de cambio
3. Estandarizacion de genero: M/F/X -> valores unicos
4. Conversion de fechas: VARCHAR a DATE/DATETIME
5. Transformacion de totales: montos string -> decimal (limpiar comas/puntos)
"""
import pandas as pd
import numpy as np
from datetime import datetime
import logging
from typing import Dict, List, Tuple
import re

logger = logging.getLogger(__name__)


class DataTransformer:
    """Transforma y normaliza datos de MySQL segun las 5 reglas de integracion ETL"""

    # REGLA 3: Mapeo de generos (MySQL usa M/F/X)
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

    def __init__(self, exchange_rates: pd.DataFrame = None):
        """
        Args:
            exchange_rates: DataFrame con tipos de cambio (fecha, tasa CRC->USD)
        """
        self.exchange_rates = exchange_rates

    def set_exchange_rates(self, exchange_rates: pd.DataFrame):
        """Configura los tipos de cambio para conversion CRC->USD"""
        self.exchange_rates = exchange_rates

    def transform_clientes(self, df_clientes: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """
        REGLA 3: Estandarizacion de genero (M/F/X -> valores unicos)
        REGLA 4: Conversion de fechas VARCHAR a DATE
        """
        logger.info(f"Transformando {len(df_clientes)} clientes...")

        df = df_clientes.copy()

        df['source_system'] = self.SOURCE_SYSTEM
        df['source_key'] = df['id'].astype(str)

        # REGLA 3: Estandarizar genero M/F/X -> Masculino/Femenino/No especificado
        df['genero'] = df['genero'].fillna('X')
        df['genero'] = df['genero'].map(self.GENDER_MAPPING)

        # Normalizar email
        df['correo'] = df['correo'].str.strip().str.lower()

        # REGLA 4: Convertir fecha VARCHAR 'YYYY-MM-DD' a DATE
        df['created_at'] = pd.to_datetime(df['created_at'], format='%Y-%m-%d', errors='coerce').dt.date

        # Renombrar columnas para DWH
        df = df.rename(columns={
            'id': 'id',
            'nombre': 'name',
            'correo': 'email',
            'genero': 'gender',
            'pais': 'country',
            'created_at': 'created_at'
        })

        tracking = {
            'source_system': self.SOURCE_SYSTEM,
            'tabla_destino': 'DimCustomer',
            'registros_procesados': len(df)
        }

        logger.info(f"[OK] Clientes transformados: {len(df)}")
        logger.info(f"  Generos unicos: {df['gender'].unique()}")
        return df, tracking

    def transform_productos(self, df_productos: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """
        REGLA 1: Homologacion de productos
        MySQL usa codigo_alt (no es SKU oficial)
        Se mantiene para tabla puente de mapeo
        """
        logger.info(f"Transformando {len(df_productos)} productos...")

        df = df_productos.copy()

        df['source_system'] = self.SOURCE_SYSTEM
        df['source_key'] = df['id'].astype(str)

        # REGLA 1: Normalizar codigo_alt
        df['codigo_alt'] = df['codigo_alt'].str.strip().str.upper()

        # Normalizar nombre
        df['nombre'] = df['nombre'].str.strip()

        # Normalizar categoria
        df['categoria'] = df['categoria'].str.strip().str.upper()

        # Renombrar columnas
        df = df.rename(columns={
            'id': 'id',
            'codigo_alt': 'code',  # codigo_alt como code (para mapeo)
            'nombre': 'name',
            'categoria': 'categoryId'
        })

        tracking = {
            'source_system': self.SOURCE_SYSTEM,
            'tabla_destino': 'DimProduct',
            'registros_procesados': len(df)
        }

        logger.info(f"[OK] Productos transformados: {len(df)}")
        logger.info(f"  Codigos unicos: {df['code'].nunique()}")
        return df, tracking

    def _clean_numeric_string(self, value) -> float:
        """
        REGLA 5: Limpia strings con formatos numericos (comas, puntos)
        Ej: '1,200.50' -> 1200.50, '1.200,50' -> 1200.50
        """
        if pd.isna(value):
            return np.nan

        value_str = str(value).strip()

        # Detectar formato: si tiene coma como decimal (europeo)
        # 1.200,50 -> decimal es coma
        # 1,200.50 -> decimal es punto
        if re.match(r'^\d{1,3}(\.\d{3})*(,\d+)?$', value_str):
            # Formato europeo: 1.200,50
            value_str = value_str.replace('.', '').replace(',', '.')
        else:
            # Formato americano: 1,200.50
            value_str = value_str.replace(',', '')

        try:
            return float(value_str)
        except ValueError:
            return np.nan

    def transform_ordenes(self, df_ordenes: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """
        REGLA 2: Normalizacion de moneda (CRC -> USD con tipo de cambio)
        REGLA 4: Conversion de fechas VARCHAR a DATETIME
        REGLA 5: Transformacion de totales string -> decimal
        """
        logger.info(f"Transformando {len(df_ordenes)} ordenes...")

        df = df_ordenes.copy()

        df['source_system'] = self.SOURCE_SYSTEM
        df['source_key'] = df['id'].astype(str)

        # REGLA 4: Convertir fecha VARCHAR 'YYYY-MM-DD HH:MM:SS' a DATETIME
        df['fecha'] = pd.to_datetime(df['fecha'], format='%Y-%m-%d %H:%M:%S', errors='coerce')

        # REGLA 5: Convertir total string -> decimal (limpiar comas/puntos)
        df['total'] = df['total'].apply(self._clean_numeric_string)

        # Normalizar canal
        df['canal'] = df['canal'].str.strip().str.upper()

        # REGLA 2: Conversion de moneda CRC -> USD
        df['moneda'] = df['moneda'].fillna('USD').str.upper()
        df['total_usd'] = df.apply(self._convert_to_usd, axis=1)

        # Validar totales
        df = df.dropna(subset=['total_usd'])
        df = df[df['total_usd'] >= 0]

        # Renombrar columnas
        df = df.rename(columns={
            'id': 'id',
            'cliente_id': 'customerId',
            'fecha': 'date',
            'canal': 'channel',
            'total_usd': 'totalOrderUSD'
        })

        tracking = {
            'source_system': self.SOURCE_SYSTEM,
            'tabla_destino': 'FactSales',
            'registros_procesados': len(df),
            'monedas_encontradas': df_ordenes['moneda'].unique().tolist()
        }

        logger.info(f"[OK] Ordenes transformadas: {len(df)}")
        logger.info(f"  Monedas convertidas a USD")
        return df, tracking

    def _convert_to_usd(self, row) -> float:
        """
        REGLA 2: Convierte monto a USD usando tipo de cambio
        """
        moneda = row.get('moneda', 'USD')
        total = row.get('total', 0)
        fecha = row.get('fecha')

        if pd.isna(total):
            return np.nan

        if moneda == 'USD':
            return float(total)

        if moneda == 'CRC':
            # Buscar tipo de cambio para la fecha
            if self.exchange_rates is not None and not self.exchange_rates.empty:
                fecha_date = pd.to_datetime(fecha).date() if pd.notna(fecha) else None
                if fecha_date:
                    rate_row = self.exchange_rates[
                        self.exchange_rates['fecha'] == fecha_date
                    ]
                    if not rate_row.empty:
                        tasa = rate_row.iloc[0]['tasa']
                        return float(total) / tasa

            # Tasa por defecto si no hay datos
            DEFAULT_CRC_USD_RATE = 515.0
            logger.debug(f"Usando tasa por defecto CRC/USD: {DEFAULT_CRC_USD_RATE}")
            return float(total) / DEFAULT_CRC_USD_RATE

        # Moneda desconocida, asumir USD
        logger.warning(f"Moneda desconocida: {moneda}, asumiendo USD")
        return float(total)

    def transform_orden_detalle(self, df_detalle: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """
        REGLA 5: Transformacion de totales
        - Convierte precios string -> decimal (limpiar comas/puntos)
        - Valida cantidades > 0
        - Calcula linea total
        """
        logger.info(f"Transformando {len(df_detalle)} lineas de detalle...")

        df = df_detalle.copy()

        df['source_system'] = self.SOURCE_SYSTEM
        df['source_key'] = df['id'].astype(str)

        # REGLA 5: Convertir precio string -> decimal
        df['precio_unit'] = df['precio_unit'].apply(self._clean_numeric_string)

        # Convertir cantidad
        df['cantidad'] = pd.to_numeric(df['cantidad'], errors='coerce').astype('Int64')

        # MySQL no tiene descuento en detalle, agregar columna con 0
        df['descuento_pct'] = 0.0

        # Validar datos
        df = df.dropna(subset=['precio_unit', 'cantidad'])
        df = df[(df['cantidad'] > 0) & (df['precio_unit'] >= 0)]

        # Renombrar columnas
        df = df.rename(columns={
            'id': 'id',
            'orden_id': 'orderId',
            'producto_id': 'productId',
            'cantidad': 'productCant',
            'precio_unit': 'productUnitPrice',
            'descuento_pct': 'discountPercentage'
        })

        tracking = {
            'source_system': self.SOURCE_SYSTEM,
            'tabla_destino': 'FactSales',
            'registros_procesados': len(df)
        }

        logger.info(f"[OK] Detalles transformados: {len(df)}")
        return df, tracking

    def extract_categorias(self, df_productos: pd.DataFrame) -> pd.DataFrame:
        """Extrae categorias unicas para DimCategory"""
        logger.info("Extrayendo categorias unicas...")

        categorias = df_productos[['categoryId']].drop_duplicates()
        categorias = categorias.rename(columns={'categoryId': 'name'})

        logger.info(f"[OK] Categorias extraidas: {len(categorias)}")
        return categorias

    def extract_canales(self, df_ordenes: pd.DataFrame) -> pd.DataFrame:
        """Extrae canales unicos para DimChannel"""
        logger.info("Extrayendo canales unicos...")

        canales = df_ordenes[['channel']].drop_duplicates()
        canales = canales.rename(columns={'channel': 'name'})

        logger.info(f"[OK] Canales extraidos: {len(canales)}")
        return canales

    def generate_dimtime(self, df_ordenes: pd.DataFrame) -> pd.DataFrame:
        """
        Genera tabla DimTime
        Para MySQL: incluye conversion de moneda (CRC puede estar presente)
        """
        logger.info("Generando DimTime...")

        fechas = df_ordenes['date'].dt.date.unique()
        fechas = pd.to_datetime(fechas)

        dim_time = pd.DataFrame({
            'date': fechas,
            'year': fechas.year,
            'month': fechas.month,
            'day': fechas.day,
            'exchangeRateToUSD': 1.0  # Se actualiza con BCCR
        })

        dim_time['id'] = range(1, len(dim_time) + 1)

        logger.info(f"[OK] DimTime generada: {len(dim_time)} fechas")
        return dim_time[['id', 'year', 'month', 'day', 'date', 'exchangeRateToUSD']]

    def build_product_mapping(self, df_productos: pd.DataFrame) -> pd.DataFrame:
        """
        REGLA 1: Construye tabla puente de mapeo de productos
        Para MySQL: source_system='MYSQL', source_code=codigo_alt
        El SKU oficial se obtiene de la tabla de mapeo existente o se genera
        """
        logger.info("Construyendo tabla de mapeo de productos...")

        mapping = pd.DataFrame({
            'source_system': self.SOURCE_SYSTEM,
            'source_code': df_productos['code'],
            'sku_oficial': df_productos['code'],  # Se actualizara con mapeo real
            'descripcion': df_productos['name']
        })

        logger.info(f"[OK] Mapeo de {len(mapping)} productos generado")
        return mapping

    def build_fact_sales(self, df_detalles: pd.DataFrame, df_ordenes: pd.DataFrame,
                         df_productos: pd.DataFrame, df_clientes: pd.DataFrame,
                         dw_connection_string: str) -> pd.DataFrame:
        """
        Construye FactSales con FKs a las dimensiones del DWH
        Incluye conversion de moneda para MySQL (REGLA 2)
        """
        import pyodbc

        logger.info(f"Construyendo FactSales con {len(df_detalles)} registros...")

        # JOIN con ordenes para obtener customerId, channel, date, moneda
        df_fact = df_detalles.merge(
            df_ordenes[['id', 'customerId', 'channel', 'date', 'moneda', 'totalOrderUSD']],
            left_on='orderId',
            right_on='id',
            suffixes=('', '_orden')
        )

        # Agregar email de clientes
        df_fact = df_fact.merge(
            df_clientes[['id', 'email']],
            left_on='customerId',
            right_on='id',
            suffixes=('', '_cliente')
        )

        # Agregar code de productos
        df_fact = df_fact.merge(
            df_productos[['id', 'code']],
            left_on='productId',
            right_on='id',
            suffixes=('', '_producto')
        )

        # Conectar al DWH para mapear IDs
        conn = pyodbc.connect(dw_connection_string)
        cursor = conn.cursor()

        # Mapear code -> DimProduct.id
        cursor.execute("SELECT id, code FROM DimProduct")
        product_map = {code: id for id, code in cursor.fetchall()}

        # Mapear email -> DimCustomer.id
        cursor.execute("SELECT id, email FROM DimCustomer")
        customer_map = {email: id for id, email in cursor.fetchall()}

        # Mapear channel -> DimChannel.id
        cursor.execute("SELECT id, name FROM DimChannel")
        channel_map = {name: id for id, name in cursor.fetchall()}

        # Mapear date -> DimTime.id
        cursor.execute("SELECT id, date FROM DimTime")
        time_map = {str(date): id for id, date in cursor.fetchall()}

        cursor.close()
        conn.close()

        # Aplicar mapeos
        df_fact['productId_dwh'] = df_fact['code'].map(product_map)
        df_fact['customerId_dwh'] = df_fact['email'].map(customer_map)
        df_fact['channelId'] = df_fact['channel'].map(channel_map)
        df_fact['timeId'] = df_fact['date'].astype(str).str[:10].map(time_map)

        # REGLA 2: Convertir precio unitario a USD si es CRC
        df_fact['productUnitPriceUSD'] = df_fact.apply(
            lambda row: self._convert_unit_price_to_usd(
                row['productUnitPrice'],
                row.get('moneda', 'USD'),
                row['date']
            ),
            axis=1
        )

        # Calcular linea total USD
        df_fact['lineTotalUSD'] = (
            df_fact['productUnitPriceUSD'] *
            df_fact['productCant'] *
            (1 - df_fact['discountPercentage'] / 100)
        )

        # Crear orderId secuencial
        df_fact = df_fact.sort_values('orderId')
        orden_ids_unicos = df_fact['orderId'].unique()
        orden_id_map = {old_id: new_id for new_id, old_id in enumerate(orden_ids_unicos, start=1)}
        df_fact['orderId_dwh'] = df_fact['orderId'].map(orden_id_map)

        # Preparar DataFrame final
        df_fact_final = pd.DataFrame({
            'productId': df_fact['productId_dwh'],
            'timeId': df_fact['timeId'],
            'orderId': df_fact['orderId_dwh'],
            'channelId': df_fact['channelId'],
            'customerId': df_fact['customerId_dwh'],
            'productCant': df_fact['productCant'],
            'productUnitPriceUSD': df_fact['productUnitPriceUSD'],
            'lineTotalUSD': df_fact['lineTotalUSD'],
            'discountPercentage': df_fact['discountPercentage'],
            'created_at': datetime.now(),
            'exchangeRateId': np.nan
        })

        # Eliminar registros con FKs nulas
        df_fact_final = df_fact_final.dropna(subset=['productId', 'customerId', 'channelId', 'timeId'])

        logger.info(f"[OK] FactSales construido: {len(df_fact_final)} registros validos")
        return df_fact_final

    def _convert_unit_price_to_usd(self, price, moneda, fecha) -> float:
        """Convierte precio unitario a USD"""
        if pd.isna(price):
            return np.nan

        if moneda == 'USD':
            return float(price)

        if moneda == 'CRC':
            if self.exchange_rates is not None and not self.exchange_rates.empty:
                fecha_date = pd.to_datetime(fecha).date() if pd.notna(fecha) else None
                if fecha_date:
                    rate_row = self.exchange_rates[
                        self.exchange_rates['fecha'] == fecha_date
                    ]
                    if not rate_row.empty:
                        tasa = rate_row.iloc[0]['tasa']
                        return float(price) / tasa

            DEFAULT_CRC_USD_RATE = 515.0
            return float(price) / DEFAULT_CRC_USD_RATE

        return float(price)
