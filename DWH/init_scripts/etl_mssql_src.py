import logging
import os
from datetime import datetime

import pymssql
from db_utils import clear_table, executemany_chunks
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
LOG = logging.getLogger("etl_mssql_src")
load_dotenv()


def parse_date(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value)).date()
    except Exception:
        return None


def get_conn():
    host = os.getenv("MSSQL_SRC_HOST", "host.docker.internal")
    port = int(os.getenv("MSSQL_SRC_PORT", "1435"))
    user = os.getenv("MSSQL_SRC_USER", "sa")
    pwd = os.getenv("MSSQL_SRC_PASSWORD", "BasesDatos2!")
    db = os.getenv("MSSQL_SRC_DB", "SalesDB_MSSQL")
    return pymssql.connect(server=host, port=port, user=user, password=pwd, database=db, timeout=30)


def load_products():
    clear_table("staging.mssql_products")
    rows = []
    with get_conn() as conn:
        cur = conn.cursor(as_dict=True)
        cur.execute("SELECT SKU AS source_key, Nombre, Categoria, 0.0 AS price FROM sales_ms.Producto")
        for r in cur.fetchall():
            rows.append(
                (
                    "MSSQL_SRC",
                    r["source_key"],
                    r["source_key"],  # code = SKU también
                    r["Nombre"],
                    r["Categoria"],
                    r.get("price"),
                    None,
                )
            )
    executemany_chunks(
        "staging.mssql_products",
        ["source_system", "source_key", "code", "name", "category", "price", "payload_json"],
        rows,
        chunk_size=5000,
    )


def load_customers():
    clear_table("staging.mssql_customers")
    rows = []
    with get_conn() as conn:
        cur = conn.cursor(as_dict=True)
        cur.execute(
            """
            SELECT ClienteId,
                   Nombre,
                   Email,
                   Genero,
                   Pais,
                   FechaRegistro
            FROM sales_ms.Cliente
            """
        )
        for r in cur.fetchall():
            rows.append(
                (
                    "MSSQL_SRC",
                    str(r["ClienteId"]),
                    r["Nombre"],
                    r["Email"],
                    r["Genero"],
                    r["Pais"],
                    parse_date(r["FechaRegistro"]),
                    None,
                )
            )
    executemany_chunks(
        "staging.mssql_customers",
        ["source_system", "source_key", "name", "email", "gender", "country", "created_at_src", "payload_json"],
        rows,
        chunk_size=5000,
    )


def load_sales():
    clear_table("staging.mssql_sales")
    rows = []
    with get_conn() as conn:
        cur = conn.cursor(as_dict=True)
        cur.execute(
            """
            SELECT d.OrdenDetalleId,
                   o.OrdenId,
                   p.SKU AS product_key,
                   o.ClienteId AS customer_key,
                   o.Canal AS channel,
                   d.Cantidad AS quantity,
                   d.PrecioUnit AS unit_price,
                   o.Moneda AS currency,
                   o.Fecha AS order_date
            FROM sales_ms.OrdenDetalle d
            JOIN sales_ms.Orden o ON d.OrdenId = o.OrdenId
            JOIN sales_ms.Producto p ON d.ProductoId = p.ProductoId
            """
        )
        for r in cur.fetchall():
            rows.append(
                (
                    "MSSQL_SRC",
                    f"{r['OrdenId']}-{r['product_key']}",
                    r["product_key"],
                    r["customer_key"],
                    r["OrdenId"],
                    r["channel"],
                    r["quantity"],
                    r["unit_price"],
                    r["currency"],
                    parse_date(r["order_date"]),
                    None,
                )
            )
    executemany_chunks(
        "staging.mssql_sales",
        [
            "source_system",
            "source_key",
            "product_key",
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
