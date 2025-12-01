import logging
import os
from datetime import datetime

import pymysql
from db_utils import clear_table, executemany_chunks
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
LOG = logging.getLogger("etl_mysql")
load_dotenv()


def parse_date(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value)).date()
    except Exception:
        return None


def get_conn():
    host = os.getenv("MYSQL_HOST", "host.docker.internal")
    port = int(os.getenv("MYSQL_PORT", "3306"))
    user = os.getenv("MYSQL_USER", "root")
    pwd = os.getenv("MYSQL_PASSWORD", "root123")
    db = os.getenv("MYSQL_DATABASE", "sales_mysql")
    return pymysql.connect(host=host, port=port, user=user, password=pwd, database=db, cursorclass=pymysql.cursors.DictCursor)


def load_products():
    clear_table("staging.mysql_products")
    rows = []
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, codigo_alt, nombre, categoria FROM Producto")
            for r in cur.fetchall():
                rows.append(
                    (
                        "MySQL",
                        r["codigo_alt"],
                        r["codigo_alt"],
                        r["codigo_alt"],
                        r["nombre"],
                        r["categoria"],
                        None,  # MySQL no tiene precio en Producto
                        None,
                    )
                )
    executemany_chunks(
        "staging.mysql_products",
        ["source_system", "source_key", "sku", "codigo_alt", "nombre", "categoria", "precio", "payload_json"],
        rows,
        chunk_size=5000,
    )


def load_customers():
    clear_table("staging.mysql_customers")
    rows = []
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, nombre, correo, genero, pais, created_at FROM Cliente")
            for r in cur.fetchall():
                rows.append(
                    (
                        "MySQL",
                        str(r["id"]),
                        r["nombre"],
                        r["correo"],
                        r["genero"],
                        r["pais"],
                        parse_date(r["created_at"]),
                        None,
                    )
                )
    executemany_chunks(
        "staging.mysql_customers",
        ["source_system", "source_key", "nombre", "correo", "genero", "pais", "created_at_src", "payload_json"],
        rows,
        chunk_size=5000,
    )


def load_sales():
    clear_table("staging.mysql_sales")
    rows = []
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT d.id AS detalle_id,
                       o.id AS orden_id,
                       p.codigo_alt AS sku,
                       o.cliente_id AS customer_key,
                       o.canal,
                       d.cantidad,
                       d.precio_unit,
                       o.moneda,
                       o.fecha
                FROM OrdenDetalle d
                JOIN Orden o ON d.orden_id = o.id
                JOIN Producto p ON d.producto_id = p.id
                """
            )
            for r in cur.fetchall():
                rows.append(
                    (
                        "MySQL",
                        f"{r['orden_id']}-{r['sku']}",
                        r["sku"],
                        r["customer_key"],
                        r["orden_id"],
                        r["canal"],
                        r["cantidad"],
                        r["precio_unit"],
                        r["moneda"],
                        parse_date(r["fecha"]),
                        None,
                    )
                )
    executemany_chunks(
        "staging.mysql_sales",
        [
            "source_system",
            "source_key",
            "sku",
            "customer_key",
            "order_key",
            "channel",
            "quantity",
            "unit_price",
            "currency",
            "order_date",
            "payload_json",
        ],
        rows,
        chunk_size=8000,
    )


def main():
    load_customers()
    load_products()
    load_sales()


if __name__ == "__main__":
    main()
