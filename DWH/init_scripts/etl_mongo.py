import logging
import os
from datetime import datetime

from db_utils import clear_table, executemany_chunks
from dotenv import load_dotenv
from pymongo import MongoClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
LOG = logging.getLogger("etl_mongo")
load_dotenv()


def parse_date(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value)).date()
    except Exception:
        try:
            return value.date()
        except Exception:
            return None


def get_client():
    uri = os.getenv("MONGODB_URI")
    if not uri:
        raise RuntimeError("MONGODB_URI no definido")
    return MongoClient(uri)


def load_orders():
    clear_table("staging.mongo_orders")
    with get_client() as client:
        db = client.get_default_database()
        orders = db.get_collection("ordens")
        rows = []
        for doc in orders.find({}):
            total = doc.get("total")
            rows.append(
                (
                    "MongoDB",
                    str(doc.get("_id")) or doc.get("orden_id"),
                    str(doc.get("client_id")) or doc.get("cliente_id"),
                    parse_date(doc.get("fecha")),
                    total,
                    doc.get("moneda") or "CRC",
                    None,
                )
            )
        executemany_chunks(
            "staging.mongo_orders",
            ["source_system", "source_key", "customer_key", "order_date", "total_amount", "currency", "payload_json"],
            rows,
        )


def load_order_items():
    clear_table("staging.mongo_order_items")
    with get_client() as client:
        db = client.get_default_database()
        orders = {str(o.get("_id")) or o.get("orden_id"): o for o in db.get_collection("ordens").find({})}
        coll_items = db.get_collection("orden_items")
        rows = []

        if coll_items.count_documents({}) > 0:
            for doc in coll_items.find({}):
                order_key = doc.get("orden_id") or doc.get("order_id")
                rows.append(
                    (
                        "MongoDB",
                        f"{order_key}-{doc.get('producto_id')}",
                        order_key,
                        doc.get("producto_id"),
                        None,
                        doc.get("cantidad"),
                        doc.get("precio_unit"),
                        (orders.get(order_key, {}).get("moneda") if order_key in orders else None) or doc.get("moneda") or "CRC",
                        parse_date(orders.get(order_key, {}).get("fecha")) if order_key in orders else None,
                        None,
                    )
                )
        else:
            # Fallback: items embebidos en ordens
            for order_key, doc in orders.items():
                items = doc.get("items", [])
                for idx, item in enumerate(items):
                    rows.append(
                        (
                            "MongoDB",
                            f"{order_key}-{idx}",
                            order_key,
                            item.get("producto_id") or item.get("sku"),
                            item.get("descripcion"),
                            item.get("cantidad"),
                            item.get("precio_unit"),
                            doc.get("moneda") or "CRC",
                            parse_date(doc.get("fecha")),
                            None,
                        )
                    )

        executemany_chunks(
            "staging.mongo_order_items",
            [
                "source_system",
                "source_key",
                "order_key",
                "product_key",
                "product_desc",
                "quantity",
                "unit_price",
                "currency",
                "order_date",
                "payload_json",
            ],
            rows,
        )


def load_customers():
    clear_table("staging.mongo_customers")
    with get_client() as client:
        db = client.get_default_database()
        customers = db.get_collection("clientes")
        rows = []
        for doc in customers.find({}):
            rows.append(
                (
                    "MongoDB",
                    str(doc.get("_id")) or doc.get("cliente_id"),
                    doc.get("nombre"),
                    doc.get("email"),
                    doc.get("genero"),  # 'Masculino', 'Femenino', 'Otro'
                    None,
                )
            )
        executemany_chunks(
            "staging.mongo_customers",
            ["source_system", "source_key", "name", "email", "genero", "payload_json"],
            rows,
        )


def main():
    load_orders()
    load_customers()
    load_order_items()


if __name__ == "__main__":
    main()
