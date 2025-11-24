"""
Carga datos transformados en el DWH SQL Server.
"""
import logging
from datetime import datetime, date
from typing import Dict, List

import pandas as pd
import pyodbc

logger = logging.getLogger(__name__)


class DataLoader:
    """Carga dimensiones, hechos y tablas de staging."""

    def __init__(self, connection_string: str):
        self.connection_string = connection_string

    def load_staging_product_mapping(self, df_mapping: pd.DataFrame) -> None:
        logger.info("Cargando tabla puente de productos (%s filas)...", len(df_mapping))
        df = df_mapping.copy()
        df["source_code"] = df["source_code"].astype(str).str.strip()
        df = df[df["source_code"] != ""]
        df = df.drop_duplicates(subset=["source_system", "source_code"])

        with pyodbc.connect(self.connection_string) as conn:
            cursor = conn.cursor()
            existentes = {
                (row[0], row[1])
                for row in cursor.execute("SELECT source_system, source_code FROM staging_map_producto").fetchall()
            }
            for _, row in df.iterrows():
                key = (row["source_system"], row["source_code"])
                if key in existentes:
                    continue
                sql = """
                    INSERT INTO staging_map_producto (source_system, source_code, sku_oficial, descripcion, created_at)
                    VALUES (?, ?, ?, ?, DATEADD(HOUR, -6, GETDATE()))
                """
                cursor.execute(sql, row["source_system"], row["source_code"], row["sku_oficial"], row["descripcion"])
            conn.commit()

    def load_staging_exchange_rates(self, df_rates: pd.DataFrame) -> int:
        logger.info("Cargando tipos de cambio (staging_tipo_cambio)...")
        inserted = 0
        with pyodbc.connect(self.connection_string) as conn:
            cursor = conn.cursor()
            for _, row in df_rates.iterrows():
                try:
                    cursor.execute(
                        """
                        INSERT INTO staging_tipo_cambio (fecha, de_moneda, a_moneda, tasa, fuente)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        row["fecha"],
                        row["de_moneda"],
                        row["a_moneda"],
                        row["tasa"],
                        row.get("fuente", "BCCR"),
                    )
                    inserted += 1
                except pyodbc.IntegrityError:
                    continue
            conn.commit()
        return inserted

    def truncate_tables(self, table_names: List[str]) -> None:
        """Trunca tablas indicadas en orden seguro."""
        logger.info("Limpiando tablas: %s", ", ".join(table_names))
        order = ['FactSales', 'DimOrder', 'DimProduct', 'DimCustomer', 'DimChannel', 'DimCategory', 'DimTime',
                 'staging_source_tracking', 'staging_map_producto', 'staging_tipo_cambio']
        tables = [t for t in order if t in table_names] + [t for t in table_names if t not in order]
        with pyodbc.connect(self.connection_string) as conn:
            cursor = conn.cursor()
            for t in tables:
                try:
                    cursor.execute(f"TRUNCATE TABLE {t}")
                except Exception:
                    cursor.execute(f"DELETE FROM {t}")
            conn.commit()

    def load_dim_category(self, df: pd.DataFrame) -> None:
        df = df.copy()
        if "name" in df.columns:
            df["name"] = df["name"].astype(str).str.strip().str.upper()
        df = df.drop_duplicates(subset=["name"])

        # Evitar choques por llaves A-nicas si quedaron datos previos
        with pyodbc.connect(self.connection_string) as conn:
            cursor = conn.cursor()
            existentes = {row[0] for row in cursor.execute("SELECT name FROM DimCategory").fetchall()}
        df = df[~df["name"].isin(existentes)]

        if not df.empty:
            self._load_dataframe(df, "DimCategory", ["name"])
        else:
            logger.info("DimCategory ya contenA-a todos los registros, no se insertA3 nada")

    def load_dim_channel(self, df: pd.DataFrame) -> None:
        df = df.copy()
        df["channelType"] = df["name"].map(self._map_channel_type)
        df = df.drop_duplicates()

        with pyodbc.connect(self.connection_string) as conn:
            cursor = conn.cursor()
            existentes = {row[0] for row in cursor.execute("SELECT name FROM DimChannel").fetchall()}
        df = df[~df["name"].isin(existentes)]

        if not df.empty:
            self._load_dataframe(df, "DimChannel", ["name", "channelType"])
        else:
            logger.info("DimChannel ya contenA-a todos los registros, no se insertA3 nada")

    def load_dim_product(self, df: pd.DataFrame, category_map: Dict) -> None:
        df_load = df[["name", "code", "categoryId"]].copy()
        df_load["categoryId"] = df_load["categoryId"].map(category_map).fillna(0).astype("Int64")
        df_load = df_load.drop_duplicates(subset=["code"])

        with pyodbc.connect(self.connection_string) as conn:
            cursor = conn.cursor()
            existentes = {row[0] for row in cursor.execute("SELECT code FROM DimProduct").fetchall()}
        df_load = df_load[~df_load["code"].isin(existentes)]

        if not df_load.empty:
            self._load_dataframe(df_load, "DimProduct", ["name", "code", "categoryId"])
        else:
            logger.info("DimProduct ya contenA-a todos los registros, no se insertA3 nada")

    def load_dim_customer(self, df: pd.DataFrame) -> None:
        df = df.copy()
        df["created_at"] = pd.to_datetime(df["created_at"])
        df = df.drop_duplicates(subset=["email"])
        df = df[df["email"].astype(str).str.contains("@", na=False)]

        with pyodbc.connect(self.connection_string) as conn:
            cursor = conn.cursor()
            existentes = {row[0] for row in cursor.execute("SELECT email FROM DimCustomer").fetchall()}
        df = df[~df["email"].isin(existentes)]

        if not df.empty:
            self._load_dataframe(df, "DimCustomer", ["name", "email", "gender", "country", "created_at"])
        else:
            logger.info("DimCustomer ya contenA-a todos los registros, no se insertA3 nada")

    def load_dim_time(self, df: pd.DataFrame) -> None:
        df = df.copy()
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df = df.drop_duplicates(subset=["date"])

        with pyodbc.connect(self.connection_string) as conn:
            cursor = conn.cursor()
            existentes = {row[0] for row in cursor.execute("SELECT date FROM DimTime").fetchall()}
            max_id = cursor.execute("SELECT ISNULL(MAX(id), 0) FROM DimTime").fetchval()
        df = df[~df["date"].isin(existentes)]

        # Reasignar ids consecutivos para nuevas fechas evitando choques de PK
        df = df.sort_values("date")
        df["id"] = range(int(max_id) + 1, int(max_id) + 1 + len(df))

        if not df.empty:
            self._load_dataframe(df, "DimTime", ["id", "year", "month", "day", "date"])
        else:
            logger.info("DimTime ya contenA-a todos los registros, no se insertA3 nada")

    def load_dim_order(self, df: pd.DataFrame) -> None:
        self._load_dataframe(df[["totalOrderUSD"]].copy(), "DimOrder", ["totalOrderUSD"])

    def load_fact_sales(self, df: pd.DataFrame) -> None:
        df = df.copy()
        df["created_at"] = pd.to_datetime(df["created_at"])
        cols = [
            "productId",
            "timeId",
            "orderId",
            "channelId",
            "customerId",
            "productCant",
            "productUnitPriceUSD",
            "lineTotalUSD",
            "discountPercentage",
            "created_at",
            "exchangeRateId",
        ]
        self._load_dataframe(df, "FactSales", cols)

    def load_source_tracking(self, tabla: str, df_source: pd.DataFrame) -> None:
        logger.info("Cargando trazabilidad para %s", tabla)
        with pyodbc.connect(self.connection_string) as conn:
            cursor = conn.cursor()

            df = df_source.copy()
            df = df.drop_duplicates(subset=["source_system", "source_key"])

            dest_map = {}
            if tabla == "DimCustomer":
                cursor.execute("SELECT email, id FROM DimCustomer")
                dest_map = {email: id_val for email, id_val in cursor.fetchall()}
                key_column = "email"
            elif tabla == "DimProduct":
                cursor.execute("SELECT code, id FROM DimProduct")
                dest_map = {code: id_val for code, id_val in cursor.fetchall()}
                key_column = "code"
            else:
                key_column = "id"

            existentes = {
                (row[0], row[1], row[2])
                for row in cursor.execute(
                    "SELECT source_system, source_key, tabla_destino FROM staging_source_tracking"
                ).fetchall()
            }

            for _, row in df.iterrows():
                key = (row.get("source_system"), row.get("source_key"), tabla)
                if key in existentes:
                    continue
                dest_id = dest_map.get(row.get(key_column)) if dest_map else row.get("id")
                if dest_id is None:
                    continue
                cursor.execute(
                    """
                    INSERT INTO staging_source_tracking (source_system, source_key, tabla_destino, id_destino, created_at)
                    VALUES (?, ?, ?, ?, DATEADD(HOUR, -6, GETDATE()))
                    """,
                    row.get("source_system"),
                    row.get("source_key"),
                    tabla,
                    dest_id,
                )
            conn.commit()

    def _load_dataframe(self, df: pd.DataFrame, table: str, columns: List[str], identity: bool = True) -> None:
        logger.info("Cargando %s (%s filas)...", table, len(df))
        with pyodbc.connect(self.connection_string) as conn:
            cursor = conn.cursor()
            if not identity:
                cursor.execute(f"SET IDENTITY_INSERT {table} ON")
            for _, row in df.iterrows():
                values = ", ".join([self._format_value(row[col]) for col in columns])
                cols_str = ", ".join(columns)
                cursor.execute(f"INSERT INTO {table} ({cols_str}) VALUES ({values})")
            if not identity:
                cursor.execute(f"SET IDENTITY_INSERT {table} OFF")
            conn.commit()

    @staticmethod
    def _format_value(value) -> str:
        # Descomponer valores tipo Series/list para obtener un escalar
        if isinstance(value, pd.Series):
            value = value.iloc[0] if not value.empty else None
        elif isinstance(value, (list, tuple, set)):
            value = next(iter(value), None)

        if pd.isna(value):
            return "NULL"
        if isinstance(value, str):
            escaped = value.replace("'", "''")
            return f"'{escaped}'"
        if isinstance(value, bool):
            return "1" if value else "0"
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, (datetime, date, pd.Timestamp)):
            return f"'{pd.to_datetime(value).strftime('%Y-%m-%d %H:%M:%S')}'"
        escaped = str(value).replace("'", "''")
        return f"'{escaped}'"

    @staticmethod
    def _map_channel_type(channel: str) -> str:
        channel_map = {"WEB": "Website", "TIENDA": "Store", "APP": "App", "PARTNER": "Partner"}
        return channel_map.get(channel, "Other")
