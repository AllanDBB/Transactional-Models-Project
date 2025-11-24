"""
Transformador para MongoDB -> DWH aplicando las 5 reglas de integración.
"""
import logging
from datetime import datetime
from typing import Dict, Tuple

import numpy as np
import pandas as pd
import pyodbc

from config import ETLConfig

logger = logging.getLogger(__name__)


class DataTransformer:
    """Transforma y normaliza datos provenientes de MongoDB."""

    SOURCE_SYSTEM = ETLConfig.SOURCE_SYSTEM

    GENDER_MAPPING = {
        "M": "M",
        "F": "F",
        "Masculino": "M",
        "Femenino": "F",
        "Otro": "O",
        "X": "O",
        None: "O",
        "": "O",
    }

    def __init__(self, exchange_helper):
        self.exchange_helper = exchange_helper

    @staticmethod
    def _normalize_datetime(series: pd.Series) -> pd.Series:
        """Convierte objetos Neo4j DateTime u otros en pandas datetime."""
        return pd.to_datetime(
            series.apply(lambda x: x.to_native() if hasattr(x, 'to_native') else x),
            errors='coerce',
        )

    def transform_clientes(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        logger.info("Transformando clientes (regla 3)...")
        df = df.copy()

        id_series = df["_id"] if "_id" in df else df.get("id")
        if id_series is None:
            id_series = pd.Series(range(1, len(df) + 1), index=df.index)
        source_key = id_series.astype(str)

        def pick_series(candidates, default=""):
            for col in candidates:
                if col in df:
                    return df[col]
            return pd.Series([default] * len(df), index=df.index)

        email_series = pick_series(["email", "Email"]).astype(str).str.strip().str.lower()
        name_series = pick_series(["nombre", "Nombre", "name"]).astype(str).str.strip()

        genero_raw = pick_series(["genero", "Genero"], default=None)
        genero_series = genero_raw.map(self.GENDER_MAPPING).fillna("O")

        country_series = pick_series(["pais", "Pais", "country"]).astype(str).str.strip()

        fecha_col = None
        for candidate in ["creado", "created_at", "fecha_registro"]:
            if candidate in df.columns:
                fecha_col = candidate
                break
        if fecha_col:
            fecha_series = pd.to_datetime(df[fecha_col], errors="coerce").dt.date
        else:
            fecha_series = pd.Series([pd.Timestamp.now().date()] * len(df), index=df.index)

        df = pd.DataFrame(
            {
                "id": source_key,
                "name": name_series,
                "email": email_series,
                "gender": genero_series,
                "country": country_series,
                "created_at": fecha_series,
                "source_system": self.SOURCE_SYSTEM,
                "source_key": source_key,
            }
        )

        tracking = {
            "source_system": self.SOURCE_SYSTEM,
            "tabla_destino": "DimCustomer",
            "registros_procesados": len(df),
        }

        return df[
            ["id", "name", "email", "gender", "country", "created_at", "source_system", "source_key"]
        ], tracking

    def transform_productos(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        logger.info("Transformando productos (regla 1)...")
        df = df.copy()

        df["source_system"] = self.SOURCE_SYSTEM
        id_series = df["_id"] if "_id" in df else df.get("id")
        if id_series is None:
            id_series = pd.Series(range(1, len(df) + 1), index=df.index)
        df["source_key"] = id_series.astype(str)
        df["id"] = id_series

        base_codigo = df.get("codigo_mongo", pd.Series([""] * len(df), index=df.index))
        base_codigo = base_codigo.replace(np.nan, "")
        df["codigo_mongo"] = base_codigo.astype(str).str.strip().str.upper()
        df["nombre"] = df.get("nombre", df.get("name", pd.Series([""] * len(df), index=df.index))).astype(str).str.strip()
        df["categoria"] = df.get("categoria", pd.Series([""] * len(df), index=df.index)).astype(str).str.strip().str.upper()

        # SKU oficial: usar equivalencias.sku si existe; si no, codigo_mongo
        def obtener_sku_oficial(row):
            equivalencias = row.get("equivalencias") or {}
            sku_eq = ""
            if isinstance(equivalencias, dict):
                sku_eq = equivalencias.get("sku") or equivalencias.get("codigo_alt") or ""
            sku_eq = str(sku_eq).strip().upper()
            return sku_eq if sku_eq else row["codigo_mongo"]

        df["sku_oficial"] = df.apply(obtener_sku_oficial, axis=1)

        df = df.rename(
            columns={
                "_id": "id",
                "codigo_mongo": "code",
                "nombre": "name",
                "categoria": "categoryId",
            }
        )

        tracking = {
            "source_system": self.SOURCE_SYSTEM,
            "tabla_destino": "DimProduct",
            "registros_procesados": len(df),
        }

        return df[["id", "code", "name", "categoryId", "sku_oficial", "source_system", "source_key"]], tracking

    def transform_ordenes(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        logger.info("Transformando órdenes (reglas 2,4,5)...")
        df = df.copy()

        df["source_system"] = self.SOURCE_SYSTEM
        id_series = df["_id"] if "_id" in df else df.get("id")
        if id_series is None:
            id_series = pd.Series(range(1, len(df) + 1), index=df.index)
        df["source_key"] = id_series.astype(str)

        df["Fecha"] = self._normalize_datetime(df.get("fecha"))
        df["Canal"] = df.get("canal", "").astype(str).str.strip().str.upper()
        df["Moneda"] = df.get("moneda", "CRC").astype(str).str.upper()

        df["Total"] = pd.to_numeric(df.get("total"), errors="coerce")

        # Regla 2: convertir a USD usando ExchangeRateHelper
        def convertir_total(row):
            moneda = row["Moneda"] or "CRC"
            if pd.isna(row["Total"]):
                return np.nan
            if moneda == "USD":
                return float(row["Total"])
            tasa = self.exchange_helper.convertir_monto(row["Total"], moneda, "USD", row["Fecha"].date())
            if tasa is None:
                logger.warning("No se pudo convertir total (sin tasa) para orden %s, usando monto original", row["source_key"])
                return float(row["Total"])
            return float(tasa)

        df["totalOrderUSD"] = df.apply(convertir_total, axis=1)
        df = df.dropna(subset=["totalOrderUSD", "Fecha"])

        df = df.rename(
            columns={
                "_id": "id",
                "cliente_id": "customerId",
                "Fecha": "date",
                "Canal": "channel",
            }
        )

        tracking = {
            "source_system": self.SOURCE_SYSTEM,
            "tabla_destino": "DimOrder",
            "registros_procesados": len(df),
        }

        return df[["id", "customerId", "date", "channel", "totalOrderUSD", "Moneda", "source_system", "source_key"]], tracking

    def transform_orden_detalle(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        logger.info("Transformando detalle (regla 5 + conversión)...")
        df = df.copy()

        df["source_system"] = self.SOURCE_SYSTEM
        df["source_key"] = df["orden_id"].astype(str) + "-" + df["producto_id"].astype(str)

        df["Cantidad"] = pd.to_numeric(df.get("cantidad"), errors="coerce")
        df["PrecioUnit"] = pd.to_numeric(df.get("precio_unit"), errors="coerce")
        df["Moneda"] = df.get("moneda", "CRC").astype(str).str.upper()
        df["Fecha"] = self._normalize_datetime(df.get("fecha"))

        df = df.dropna(subset=["Cantidad", "PrecioUnit", "Fecha"])
        df = df[(df["Cantidad"] > 0) & (df["PrecioUnit"] >= 0)]

        def convertir_precio(row):
            moneda = row["Moneda"] or "CRC"
            if moneda == "USD":
                return float(row["PrecioUnit"])
            monto = self.exchange_helper.convertir_monto(row["PrecioUnit"], moneda, "USD", row["Fecha"].date())
            if monto is None:
                logger.warning("Sin tasa para detalle %s, usando precio original", row["source_key"])
                return float(row["PrecioUnit"])
            return float(monto)

        df["PrecioUnitUSD"] = df.apply(convertir_precio, axis=1)
        df = df.dropna(subset=["PrecioUnitUSD"])

        df["lineTotalUSD"] = df["PrecioUnitUSD"] * df["Cantidad"]

        df = df.rename(
            columns={
                "orden_id": "orderId",
                "producto_id": "productId",
                "Cantidad": "productCant",
                "PrecioUnitUSD": "productUnitPriceUSD",
            }
        )

        tracking = {
            "source_system": self.SOURCE_SYSTEM,
            "tabla_destino": "FactSales",
            "registros_procesados": len(df),
            "total_usd_procesado": df["lineTotalUSD"].sum(),
        }

        return df[
            [
                "orderId",
                "productId",
                "productCant",
                "productUnitPriceUSD",
                "lineTotalUSD",
                "Moneda",
                "Fecha",
                "source_system",
                "source_key",
            ]
        ], tracking

    def extract_categorias(self, df_productos: pd.DataFrame) -> pd.DataFrame:
        categorias = df_productos[["categoryId"]].drop_duplicates()
        return categorias.rename(columns={"categoryId": "name"})

    def extract_canales(self, df_ordenes: pd.DataFrame) -> pd.DataFrame:
        canales = df_ordenes[["channel"]].drop_duplicates()
        return canales.rename(columns={"channel": "name"})

    def generate_dimtime(self, df_ordenes: pd.DataFrame) -> pd.DataFrame:
        fechas = pd.to_datetime(df_ordenes["date"]).dt.date.unique()
        fechas = pd.to_datetime(fechas)
        dim_time = pd.DataFrame(
            {
                "date": fechas,
                "year": fechas.year,
                "month": fechas.month,
                "day": fechas.day,
            }
        )
        dim_time["id"] = range(1, len(dim_time) + 1)
        return dim_time[["id", "year", "month", "day", "date"]]

    def build_product_mapping(self, df_productos: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "source_system": self.SOURCE_SYSTEM,
                "source_code": df_productos["code"],
                "sku_oficial": df_productos["sku_oficial"],
                "descripcion": df_productos["name"],
            }
        )

    def build_fact_sales(
        self,
        df_detalles: pd.DataFrame,
        df_ordenes: pd.DataFrame,
        df_productos: pd.DataFrame,
        df_clientes: pd.DataFrame,
        dw_connection_string: str,
    ) -> pd.DataFrame:
        logger.info("Construyendo FactSales...")
        df_fact = df_detalles.merge(
            df_ordenes[["id", "customerId", "channel", "date"]],
            left_on="orderId",
            right_on="id",
            suffixes=("", "_orden"),
        )

        df_fact = df_fact.merge(
            df_clientes[["id", "email"]], left_on="customerId", right_on="id", suffixes=("", "_cliente")
        )
        df_fact = df_fact.merge(
            df_productos[["id", "code"]],
            left_on="productId",
            right_on="id",
            suffixes=("", "_producto"),
        )

        conn = pyodbc.connect(dw_connection_string)
        cursor = conn.cursor()
        cursor.execute("SELECT id, code FROM DimProduct")
        product_map = {code: id for id, code in cursor.fetchall()}
        cursor.execute("SELECT id, email FROM DimCustomer")
        customer_map = {email: id for id, email in cursor.fetchall()}
        cursor.execute("SELECT id, channelType FROM DimChannel")
        channel_map = {channelType: id for id, channelType in cursor.fetchall()}
        cursor.execute("SELECT id, date FROM DimTime")
        time_map = {str(date): id for id, date in cursor.fetchall()}
        cursor.close()
        conn.close()

        channel_type_map = {
            "WEB": "Website",
            "TIENDA": "Store",
            "APP": "App",
            "PARTNER": "Partner",
        }
        df_fact["channelType"] = df_fact["channel"].map(lambda x: channel_type_map.get(x, "Other"))
        df_fact["productId_dwh"] = df_fact["code"].map(product_map)
        df_fact["customerId_dwh"] = df_fact["email"].map(customer_map)
        df_fact["channelId"] = df_fact["channelType"].map(channel_map)
        df_fact["date_key"] = pd.to_datetime(df_fact["date"]).dt.date.astype(str)
        df_fact["timeId"] = df_fact["date_key"].map(time_map)

        df_fact = df_fact.sort_values("orderId")
        orden_ids_unicos = df_fact["orderId"].unique()
        orden_id_map = {old_id: new_id for new_id, old_id in enumerate(orden_ids_unicos, start=1)}
        df_fact["orderId_dwh"] = df_fact["orderId"].map(orden_id_map)

        df_fact_final = pd.DataFrame(
            {
                "productId": df_fact["productId_dwh"],
                "timeId": df_fact["timeId"],
                "orderId": df_fact["orderId_dwh"],
                "channelId": df_fact["channelId"],
                "customerId": df_fact["customerId_dwh"],
                "productCant": df_fact["productCant"],
                "productUnitPriceUSD": df_fact["productUnitPriceUSD"],
                "lineTotalUSD": df_fact["lineTotalUSD"],
                "discountPercentage": np.nan,
                "created_at": datetime.now(),
                "exchangeRateId": np.nan,
            }
        )

        df_fact_final = df_fact_final.dropna(subset=["productId", "customerId", "channelId", "timeId"])
        logger.info("FactSales listo con %s registros", len(df_fact_final))
        return df_fact_final
