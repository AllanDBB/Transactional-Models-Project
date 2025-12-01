import logging
import os
from db_utils import clear_table, executemany_chunks
from dotenv import load_dotenv
from supabase import create_client, Client
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
LOG = logging.getLogger("etl_supabase")
load_dotenv()


def parse_dt(val):
    if not val:
        return None
    try:
        return datetime.fromisoformat(str(val).replace('T', ' ').replace('Z', ''))
    except Exception:
        return None


def get_supabase() -> Client:
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    if not url or not key:
        raise RuntimeError("SUPABASE_URL y SUPABASE_KEY son requeridos")
    return create_client(url, key)


def load_clientes():
    """Cargar clientes desde Supabase (tabla cliente en español)"""
    clear_table("staging.supabase_users")
    supabase = get_supabase()
    rows = []
    
    # Paginación para evitar límites
    page_size = 1000
    offset = 0
    
    while True:
        response = supabase.table("cliente").select("*").range(offset, offset + page_size - 1).execute()
        if not response.data:
            break
        
        for r in response.data:
            rows.append(
                (
                    "SUPABASE",
                    str(r["cliente_id"]),
                    r["nombre"],
                    r["email"],
                    r.get("genero", ""),
                    r.get("pais", ""),
                    parse_dt(r.get("fecha_registro")),
                    None,
                )
            )
        
        offset += page_size
        if len(response.data) < page_size:
            break
    
    executemany_chunks(
        "staging.supabase_users",
        ["source_system", "source_key", "name", "email", "gender", "country", "created_at_src", "payload_json"],
        rows,
        chunk_size=5000,
    )


def load_ordenes():
    """Cargar órdenes desde Supabase (tabla orden en español)"""
    clear_table("staging.supabase_orders")
    supabase = get_supabase()
    rows = []
    
    page_size = 1000
    offset = 0
    
    while True:
        response = supabase.table("orden").select("*").range(offset, offset + page_size - 1).execute()
        if not response.data:
            break
        
        for r in response.data:
            rows.append(
                (
                    "SUPABASE",
                    str(r["orden_id"]),
                    str(r["cliente_id"]),
                    r.get("total", 0),
                    "COMPLETED",  # status por defecto
                    r.get("canal", "WEB"),  # payment_method = canal
                    parse_dt(r.get("fecha")),  # created_at_src
                    None,  # updated_at_src
                    None,  # payload_json
                )
            )
        
        offset += page_size
        if len(response.data) < page_size:
            break
    
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
    """Cargar items de órdenes desde Supabase (tabla orden_detalle en español)"""
    clear_table("staging.supabase_order_items")
    supabase = get_supabase()
    rows = []
    
    page_size = 1000
    offset = 0
    
    while True:
        response = supabase.table("orden_detalle").select("*").range(offset, offset + page_size - 1).execute()
        if not response.data:
            break
        
        for r in response.data:
            rows.append(
                (
                    "SUPABASE",
                    str(r["orden_detalle_id"]),
                    str(r["orden_id"]),
                    str(r["producto_id"]),
                    r["cantidad"],
                    r["precio_unit"],
                    r["cantidad"] * r["precio_unit"],
                    None,
                )
            )
        
        offset += page_size
        if len(response.data) < page_size:
            break
    
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


def load_productos():
    """Cargar productos desde Supabase (tabla producto en español)"""
    clear_table("staging.supabase_products")
    supabase = get_supabase()
    rows = []
    
    page_size = 1000
    offset = 0
    
    while True:
        response = supabase.table("producto").select("*").range(offset, offset + page_size - 1).execute()
        if not response.data:
            break
        
        for r in response.data:
            rows.append(
                (
                    "SUPABASE",
                    str(r["producto_id"]),
                    r["nombre"],
                    None,  # description
                    r.get("categoria", ""),
                    r.get("precio", 0),
                    None,  # stock
                    None,  # supplier_id
                    None,  # active
                    None,  # created_at_src
                    None,  # payload_json
                )
            )
        
        offset += page_size
        if len(response.data) < page_size:
            break
    
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
    LOG.info("=== Iniciando ETL Supabase ===")
    load_clientes()
    LOG.info("Clientes cargados")
    load_productos()
    LOG.info("Productos cargados")
    load_ordenes()
    LOG.info("Órdenes cargadas")
    load_order_items()
    LOG.info("Items cargados")
    LOG.info("=== ETL Supabase completado ===")


if __name__ == "__main__":
    main()


if __name__ == "__main__":
    main()
