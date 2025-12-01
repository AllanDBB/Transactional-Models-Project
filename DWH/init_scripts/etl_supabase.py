import logging
import os
from db_utils import clear_table, executemany_chunks
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
LOG = logging.getLogger("etl_supabase")
load_dotenv()


def parse_dt(val):
    if not val:
        return None
    try:
        return datetime.fromisoformat(str(val))
    except Exception:
        return None


def get_conn():
    host = os.getenv("SUPABASE_PG_HOST", "host.docker.internal")
    port = int(os.getenv("SUPABASE_PG_PORT", "5432"))
    user = os.getenv("SUPABASE_PG_USER", "postgres")
    pwd = os.getenv("SUPABASE_PG_PASSWORD", "postgres123")
    db = os.getenv("SUPABASE_PG_DB", "transactional_db")
    return psycopg2.connect(host=host, port=port, user=user, password=pwd, dbname=db)


def load_users():
    clear_table("staging.supabase_users")
    rows = []
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT id, email, username AS name, '' AS country, created_at FROM users")
        for r in cur.fetchall():
            rows.append(
                (
                    "SUPABASE",
                    str(r["id"]),
                    r["email"],
                    r["name"],
                    r["country"],
                    parse_dt(r["created_at"]),
                    None,
                )
            )
    executemany_chunks(
        "staging.supabase_users",
        ["source_system", "source_key", "email", "name", "country", "created_at_src", "payload_json"],
        rows,
        chunk_size=5000,
    )


def load_orders():
    clear_table("staging.supabase_orders")
    rows = []
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            SELECT id,
                   user_id,
                   total_amount,
                   status,
                   payment_method,
                   created_at,
                   updated_at
            FROM orders
            """
        )
        for r in cur.fetchall():
            rows.append(
                (
                    "SUPABASE",
                    str(r["id"]),
                    str(r["user_id"]),
                    r["total_amount"],
                    r["status"],
                    r["payment_method"],
                    parse_dt(r["created_at"]),
                    parse_dt(r["updated_at"]),
                    None,
                )
            )
    executemany_chunks(
        "staging.supabase_orders",
        [
            "source_system",
            "source_key",
            "user_key",
            "total_amount",
            "status",
            "payment_method",
            "created_at_src",
            "updated_at_src",
            "payload_json",
        ],
        rows,
        chunk_size=5000,
    )


def load_order_items():
    clear_table("staging.supabase_order_items")
    rows = []
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            SELECT id,
                   order_id,
                   product_id,
                   quantity,
                   unit_price,
                   subtotal
            FROM order_items
            """
        )
        for r in cur.fetchall():
            rows.append(
                (
                    "SUPABASE",
                    str(r["id"]),
                    str(r["order_id"]),
                    str(r["product_id"]),
                    r["quantity"],
                    r["unit_price"],
                    r["subtotal"],
                    None,
                )
            )
    executemany_chunks(
        "staging.supabase_order_items",
        [
            "source_system",
            "source_key",
            "order_key",
            "product_key",
            "quantity",
            "unit_price",
            "subtotal",
            "payload_json",
        ],
        rows,
        chunk_size=5000,
    )


def load_products():
    clear_table("staging.supabase_products")
    rows = []
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            SELECT id,
                   name,
                   description,
                   category,
                   price,
                   stock,
                   supplier_id,
                   active,
                   created_at
            FROM products
            """
        )
        for r in cur.fetchall():
            rows.append(
                (
                    "SUPABASE",
                    str(r["id"]),
                    r["name"],
                    r["description"],
                    r["category"],
                    r["price"],
                    r["stock"],
                    str(r["supplier_id"]) if r["supplier_id"] else None,
                    r["active"],
                    parse_dt(r["created_at"]),
                    None,
                )
            )
    executemany_chunks(
        "staging.supabase_products",
        [
            "source_system",
            "source_key",
            "name",
            "description",
            "category",
            "price",
            "stock",
            "supplier_id",
            "active",
            "created_at_src",
            "payload_json",
        ],
        rows,
        chunk_size=5000,
    )


def main():
    load_users()
    load_products()
    load_orders()
    load_order_items()


if __name__ == "__main__":
    main()
