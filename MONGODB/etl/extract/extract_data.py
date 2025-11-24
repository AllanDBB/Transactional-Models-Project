"""
Extractor para MongoDB -> DWH.
Obtiene clientes, productos, órdenes y líneas de detalle desde la base transaccional.
"""
import logging
from typing import Tuple, List, Dict, Any

import pandas as pd
from pymongo import MongoClient

from config import MongoConfig

logger = logging.getLogger(__name__)


class DataExtractor:
    """Encapsula la extracción de datos desde MongoDB."""

    def __init__(self):
        self.uri = MongoConfig.uri()
        self.database = MongoConfig.DATABASE

    def _get_client(self) -> MongoClient:
        return MongoClient(self.uri, serverSelectionTimeoutMS=5000)

    def extract(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Extrae clientes, productos, órdenes y detalle a DataFrames."""
        logger.info("Conectando a MongoDB...")
        client = self._get_client()
        db = client[self.database]

        clientes = list(db.clientes.find())
        productos = list(db.productos.find())
        ordenes = list(db.ordenes.find())
        detalle = self._flatten_detalle(ordenes)

        logger.info(
            "Extraído: clientes=%s productos=%s ordenes=%s detalle=%s",
            len(clientes),
            len(productos),
            len(ordenes),
            len(detalle),
        )

        return (
            pd.DataFrame(clientes),
            pd.DataFrame(productos),
            pd.DataFrame(ordenes),
            pd.DataFrame(detalle),
        )

    @staticmethod
    def _flatten_detalle(ordenes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Convierte items anidados de las órdenes en filas de detalle."""
        detalle = []
        for orden in ordenes:
            order_id = orden.get("_id")
            moneda = orden.get("moneda", "CRC")
            fecha = orden.get("fecha")
            items = orden.get("items", [])
            for item in items:
                detalle.append(
                    {
                        "orden_id": order_id,
                        "producto_id": item.get("producto_id") or item.get("_id"),
                        "cantidad": item.get("cantidad"),
                        "precio_unit": item.get("precio_unit"),
                        "moneda": moneda,
                        "fecha": fecha,
                    }
                )
        return detalle
