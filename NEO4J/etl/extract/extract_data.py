"""
Extractor de datos desde Neo4j (clientes, productos, órdenes y líneas).
"""
import logging
from typing import Tuple

import pandas as pd
from neo4j import GraphDatabase

from config import NeoConfig

logger = logging.getLogger(__name__)


class DataExtractor:
    """Obtiene datos transaccionales desde Neo4j."""

    def __init__(self):
        self.driver = GraphDatabase.driver(
            NeoConfig.URI, auth=(NeoConfig.USER, NeoConfig.PASSWORD)
        )
        self.database = NeoConfig.DATABASE

    def _run_query(self, query: str):
        with self.driver.session(database=self.database) as session:
            result = session.run(query)
            return [record.data() for record in result]

    def extract(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        logger.info("Extrayendo datos desde Neo4j...")
        clientes = self._run_query(
            """
            MATCH (c:Cliente)
            RETURN c.id AS id, c.nombre AS nombre, c.email AS email,
                   c.genero AS genero, c.pais AS pais
            """
        )

        productos = self._run_query(
            """
            MATCH (p:Producto)-[:PERTENECE_A]->(cat:Categoria)
            RETURN p.id AS id, p.nombre AS nombre, p.sku AS sku,
                   p.codigo_alt AS codigo_alt, p.codigo_mongo AS codigo_mongo,
                   cat.nombre AS categoria
            """
        )

        ordenes = self._run_query(
            """
            MATCH (c:Cliente)-[:REALIZO]->(o:Orden)
            RETURN o.id AS id, c.id AS cliente_id, o.fecha AS fecha,
                   o.canal AS canal, o.moneda AS moneda, o.total AS total
            """
        )

        detalle = self._run_query(
            """
            MATCH (o:Orden)-[d:CONTIENTE]->(p:Producto)
            RETURN o.id AS orden_id, p.id AS producto_id,
                   d.cantidad AS cantidad, d.precio_unit AS precio_unit,
                   o.moneda AS moneda, o.fecha AS fecha
            """
        )

        logger.info(
            "Extraído Neo4j: clientes=%s productos=%s ordenes=%s detalle=%s",
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

    def close(self):
        self.driver.close()
