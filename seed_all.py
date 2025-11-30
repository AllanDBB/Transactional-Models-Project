"""
Generador de dataset canónico para todas las fuentes (MSSQL como fuente oficial de IDs).

Produce archivos JSON/CSV en shared/generated/ con:
- clientes.json
- productos.json
- ordenes.json
- orden_items.json

Además genera splits por fuente (MSSQL, MySQL, MongoDB, Supabase, Neo4j) para que cada motor
consuma su subconjunto sin perder los IDs oficiales (de MSSQL).

Opcionalmente, con --push cargará los splits a cada motor usando credenciales de entorno:
- MSSQL: MSSQL_HOST (def: localhost), MSSQL_PORT (1435), MSSQL_USER (sa), MSSQL_PASSWORD (BasesDatos2!), MSSQL_DB (SalesDB_MSSQL).
- MySQL: MYSQL_HOST (localhost), MYSQL_PORT (3306), MYSQL_USER (root), MYSQL_PASSWORD (root123), MYSQL_DB (sales_mysql).
- MongoDB: MONGODB_URI (de MONGODB/server/.env).
- Supabase: SUPABASE_URL, SUPABASE_SERVICE_ROLE (ideal) o VITE_SUPABASE_ANON_KEY (puede fallar por RLS).
- Neo4j: NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD.
"""
from __future__ import annotations

import json
import random
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import List, Dict
import argparse
import os
import uuid

def load_env_file(path: Path) -> dict:
    env = {}
    if not path.exists():
        return env
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip()
    return env

# Intenta completar variables desde .env de cada subproyecto si no están ya en el entorno
def hydrate_env_defaults():
    # Mongo
    mongo_env = load_env_file(Path("MONGODB/server/.env"))
    os.environ.setdefault("MONGODB_URI", mongo_env.get("MONGODB_URI", ""))
    # Supabase
    supa_env = load_env_file(Path("SUPABASE/website/.env"))
    os.environ.setdefault("SUPABASE_URL", supa_env.get("VITE_SUPABASE_URL", ""))
    os.environ.setdefault("SUPABASE_KEY", supa_env.get("SUPABASE_SERVICE_ROLE", supa_env.get("VITE_SUPABASE_ANON_KEY", "")))
    # Neo4j
    neo_env = load_env_file(Path("NEO4J/server/.env"))
    os.environ.setdefault("NEO4J_URI", neo_env.get("NEO4J_URI", ""))
    os.environ.setdefault("NEO4J_USER", neo_env.get("NEO4J_USER", ""))
    os.environ.setdefault("NEO4J_PASSWORD", neo_env.get("NEO4J_PASSWORD", ""))
    # MySQL defaults from .env.example
    mysql_env = load_env_file(Path("MYSQL/.env"))
    if mysql_env:
        os.environ.setdefault("MYSQL_HOST", mysql_env.get("MYSQL_HOST", "localhost"))
        os.environ.setdefault("MYSQL_PORT", mysql_env.get("MYSQL_PORT", "3306"))
        os.environ.setdefault("MYSQL_USER", mysql_env.get("MYSQL_USER", "root"))
        os.environ.setdefault("MYSQL_PASSWORD", mysql_env.get("MYSQL_PASSWORD", "root123"))
        os.environ.setdefault("MYSQL_DB", mysql_env.get("MYSQL_DATABASE", "sales_mysql"))

# ----------------------------
# Parámetros de tamaño (mínimos de la guía)
# ----------------------------
NUM_CLIENTES = 3200  # >= 3000
NUM_PRODUCTOS = 550  # >= 500
NUM_ORDENES = 26000  # >= 25000
ITEMS_POR_ORDEN = (1, 5)  # min, max

RANDOM_SEED = 42

# Rango de fechas de venta (2024-01-01 a 2025-12-31)
VENTA_INICIO = date(2024, 1, 1)
VENTA_FIN = date(2025, 12, 31)

CANAL_OPCIONES = ["WEB", "APP", "PARTNER", "TIENDA", "TELEFONO"]
MONEDAS = ["USD", "CRC"]

# ----------------------------
# Convenciones de IDs oficiales (MSSQL)
# ----------------------------
# Clientes: CL00001 ...
# Productos (SKU oficial): SKU00001 ... (oficial); alt codes y códigos Mongo/otros se derivan
# Órdenes: ORD000001 ...

# Distribución de SKUs según conversación:
# - Solapados (todos): 00001-00100
# - Brian: 00201-00300
# - Santi: 00301-00400
# - Allan: 00401-00500
# Nota: 00101-00200 quedó sin asignar explícito; los incluimos en "solapados + base".


@dataclass
class Cliente:
    cliente_id: str
    nombre: str
    email: str
    genero: str
    pais: str
    fecha_registro: str


@dataclass
class Producto:
    sku: str          # ID oficial MSSQL
    nombre: str
    categoria: str
    precio_usd: float
    precio_crc: int
    codigo_alt: str   # ej. A0001
    codigo_mongo: str # ej. M00001
    sku_owner: str    # quien lo generó (solapado/brian/santi/allan)


@dataclass
class Orden:
    orden_id: str      # ID oficial MSSQL
    cliente_id: str
    fecha: str
    canal: str
    moneda: str
    total: float


@dataclass
class OrdenItem:
    orden_id: str
    producto_id: str   # SKU oficial
    cantidad: int
    precio_unit: float


def random_date(start: date, end: date) -> datetime:
    delta_days = (end - start).days
    day_offset = random.randint(0, delta_days)
    # hora aleatoria
    hour = random.randint(0, 23)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    return datetime.combine(start + timedelta(days=day_offset), datetime.min.time()) + timedelta(
        hours=hour, minutes=minute, seconds=second
    )


def build_clientes() -> List[Cliente]:
    nombres = ["Alex", "Brenda", "Carlos", "Daniela", "Eduardo", "Fernanda", "Gabriel", "Hanna", "Iker", "Julia"]
    apellidos = ["Lopez", "Martinez", "Gomez", "Ramirez", "Rodriguez", "Sanchez", "Vargas", "Castro", "Mejia", "Jimenez"]
    generos = ["Masculino", "Femenino", "Otro"]
    paises = ["CR", "MX", "US", "ES", "CO", "AR"]

    clientes: List[Cliente] = []
    for i in range(1, NUM_CLIENTES + 1):
        nombre = f"{random.choice(nombres)} {random.choice(apellidos)}"
        correo = f"user{i:04d}@example.com"
        genero = random.choice(generos)
        pais = random.choice(paises)
        fecha_reg = random_date(date(2023, 6, 1), date(2025, 1, 15)).isoformat()
        clientes.append(
            Cliente(
                cliente_id=f"CL{i:05d}",
                nombre=nombre,
                email=correo,
                genero=genero,
                pais=pais,
                fecha_registro=fecha_reg,
            )
        )
    return clientes


def build_productos() -> List[Producto]:
    categorias = ["ALIMENTOS", "TECNOLOGIA", "LIBROS", "HOGAR", "DEPORTE", "BELLEZA", "JUGUETES", "MODA"]

    productos: List[Producto] = []
    # Rango solapado + segmentos
    def owner_for_sku(n: int) -> str:
        if 1 <= n <= 100:
            return "solapado"
        if 101 <= n <= 200:
            return "sin_asignar"
        if 201 <= n <= 300:
            return "brian"
        if 301 <= n <= 400:
            return "santi"
        if 401 <= n <= 500:
            return "allan"
        return "extra"

    for i in range(1, NUM_PRODUCTOS + 1):
        categoria = random.choice(categorias)
        base_price_usd = round(random.uniform(5, 500), 2)
        crc = int(base_price_usd * 540)  # tasa aprox.
        sku_num = i
        productos.append(
            Producto(
                sku=f"SKU{sku_num:05d}",
                nombre=f"Producto {i}",
                categoria=categoria,
                precio_usd=base_price_usd,
                precio_crc=crc,
                codigo_alt=f"A{sku_num:04d}",
                codigo_mongo=f"M{sku_num:05d}",
                sku_owner=owner_for_sku(sku_num),
            )
        )
    return productos


def build_ordenes_y_items(clientes: List[Cliente], productos: List[Producto]) -> tuple[List[Orden], List[OrdenItem]]:
    ordenes: List[Orden] = []
    items: List[OrdenItem] = []

    for i in range(1, NUM_ORDENES + 1):
        cliente = random.choice(clientes)
        fecha_dt = random_date(VENTA_INICIO, VENTA_FIN)
        canal = random.choice(CANAL_OPCIONES)
        moneda = random.choice(MONEDAS)

        num_items = random.randint(ITEMS_POR_ORDEN[0], ITEMS_POR_ORDEN[1])
        used_products = random.sample(productos, num_items)

        total = 0.0
        orden_id = f"ORD{i:06d}"
        for prod in used_products:
            cantidad = random.randint(1, 5)
            # si es CRC, usamos precio_crc; si es USD, precio_usd
            precio = prod.precio_crc if moneda == "CRC" else prod.precio_usd
            total += cantidad * precio
            items.append(
                OrdenItem(
                    orden_id=orden_id,
                    producto_id=prod.sku,
                    cantidad=cantidad,
                    precio_unit=precio,
                )
            )

        ordenes.append(
            Orden(
                orden_id=orden_id,
                cliente_id=cliente.cliente_id,
                fecha=fecha_dt.isoformat(),
                canal=canal,
                moneda=moneda,
                total=round(total, 2) if moneda == "USD" else int(total),
            )
        )
    return ordenes, items


def write_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=True, indent=2)


def write_csv(path: Path, rows: List[Dict[str, str]]) -> None:
    import csv

    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


# ----------------------------
# Pushers (opcionales)
# ----------------------------

def push_mssql(base_dir: Path):
    try:
        import pyodbc  # type: ignore
    except ImportError:
        print("[MSSQL] pyodbc no instalado; omitiendo push.")
        return

    host = os.getenv("MSSQL_HOST", "localhost")
    port = int(os.getenv("MSSQL_PORT", "1435"))
    user = os.getenv("MSSQL_USER", "sa")
    pwd = os.getenv("MSSQL_PASSWORD", "BasesDatos2!")
    db = os.getenv("MSSQL_DB", "SalesDB_MSSQL")
    driver = os.getenv("MSSQL_ODBC_DRIVER", "ODBC Driver 17 for SQL Server")
    conn_str = f"DRIVER={{{driver}}};SERVER={host},{port};DATABASE={db};UID={user};PWD={pwd};TrustServerCertificate=yes"
    print(f"[MSSQL] Conectando a {host}:{port}/{db}")
    cn = pyodbc.connect(conn_str)
    cur = cn.cursor()
    # Limpiar si existe SP
    try:
        cur.execute("IF OBJECT_ID('sales_ms.sp_limpiar_bd','P') IS NOT NULL EXEC sales_ms.sp_limpiar_bd;")
        cn.commit()
    except Exception as e:
        print(f"[MSSQL] No se pudo limpiar: {e}, intentando wipe manual.")
        cur.execute("ALTER TABLE sales_ms.OrdenDetalle NOCHECK CONSTRAINT ALL;")
        cur.execute("ALTER TABLE sales_ms.Orden NOCHECK CONSTRAINT ALL;")
        cur.execute("DELETE FROM sales_ms.OrdenDetalle; DELETE FROM sales_ms.Orden; DELETE FROM sales_ms.Producto; DELETE FROM sales_ms.Cliente;")
        cn.commit()
        cur.execute("ALTER TABLE sales_ms.OrdenDetalle CHECK CONSTRAINT ALL;")
        cur.execute("ALTER TABLE sales_ms.Orden CHECK CONSTRAINT ALL;")
        cn.commit()

    clientes = json.loads((base_dir / "clientes.json").read_text(encoding="utf-8"))
    productos = json.loads((base_dir / "productos.json").read_text(encoding="utf-8"))
    ordenes = json.loads((base_dir / "ordenes.json").read_text(encoding="utf-8"))
    items = json.loads((base_dir / "orden_items.json").read_text(encoding="utf-8"))

    # Insert clientes
    for chunk in chunked(clientes, 500):
        cur.executemany(
            """
            MERGE sales_ms.Cliente AS tgt
            USING (SELECT ? AS Email) AS src
            ON tgt.Email = src.Email
            WHEN NOT MATCHED THEN
                INSERT (Nombre, Email, Genero, Pais, FechaRegistro) VALUES (?, ?, ?, ?, ?);
            """,
            [
                (
                    f"{c['cliente_id'].lower()}@seed.local",
                    c["nombre"],
                    f"{c['cliente_id'].lower()}@seed.local",
                    c["genero"] if c["genero"] in ("Masculino", "Femenino") else "Masculino",
                    c["pais"],
                    c["fecha_registro"],
                )
                for c in chunk
            ],
        )
    cn.commit()
    # Map cliente_id oficial -> identity
    cur.execute("SELECT ClienteId, Email FROM sales_ms.Cliente")
    cliente_map = {}
    for cid, email in cur.fetchall():
        try:
            code = email.split("@")[0].upper().replace("SEED.LOCAL", "")
            cliente_map[code] = cid
        except Exception:
            continue

    # Insert productos
    for chunk in chunked(productos, 500):
        cur.executemany(
            "INSERT INTO sales_ms.Producto (SKU, Nombre, Categoria) VALUES (?,?,?)",
            [(p["sku"], p["nombre"], p["categoria"]) for p in chunk],
        )
    cn.commit()
    cur.execute("SELECT ProductoId, SKU FROM sales_ms.Producto")
    prod_map = {sku: pid for pid, sku in cur.fetchall()}

    # Insert ordenes
    for chunk in chunked(ordenes, 400):
        cur.executemany(
            "INSERT INTO sales_ms.Orden (ClienteId, Fecha, Canal, Moneda, Total) VALUES (?,?,?,?,?)",
            [
                (
                    cliente_map.get(o["cliente_id"], None),
                    o["fecha"],
                    o["canal"] if o["canal"] in ("WEB", "TIENDA", "APP") else "WEB",
                    o["moneda"],
                    o["total"],
                )
                for o in chunk
                if cliente_map.get(o["cliente_id"])
            ],
        )
    cn.commit()
    cur.execute("SELECT OrdenId, Fecha, ClienteId FROM sales_ms.Orden")
    # identity no mapea fácil con ORD000001; se reconstruye por orden de inserción
    # hacemos mapping aproximado por ClienteId+Fecha
    orden_map = {}
    for row in cur.fetchall():
        key = (row.ClienteId, row.Fecha.isoformat() if hasattr(row.Fecha, "isoformat") else str(row.Fecha))
        orden_map[key] = row.OrdenId

    # Insert items
    inserted = 0
    for it in items:
        prod_id = prod_map.get(it["producto_id"])
        # encontrar orden_id local
        # Busca orden con cliente y fecha iguales
        # Para simplificar: usamos primer match
        ord_obj = next((o for o in ordenes if o["orden_id"] == it["orden_id"]), None)
        if not ord_obj:
            continue
        ord_local = orden_map.get(
            (
                cliente_map.get(ord_obj["cliente_id"]),
                ord_obj["fecha"],
            )
        )
        if not prod_id or not ord_local:
            continue
        cur.execute(
            "INSERT INTO sales_ms.OrdenDetalle (OrdenId, ProductoId, Cantidad, PrecioUnit, DescuentoPct) VALUES (?,?,?,?,NULL)",
            (ord_local, prod_id, it["cantidad"], it["precio_unit"]),
        )
        inserted += 1
        if inserted % 500 == 0:
            cn.commit()
    cn.commit()
    print(f"[MSSQL] Cargados clientes:{len(clientes)}, productos:{len(productos)}, ordenes:{len(ordenes)}, items:{inserted}")
    cn.close()


def push_mysql(base_dir: Path):
    try:
        import mysql.connector  # type: ignore
    except ImportError:
        print("[MySQL] mysql-connector-python no instalado; omitiendo push.")
        return

    host = os.getenv("MYSQL_HOST", "localhost")
    port = int(os.getenv("MYSQL_PORT", "3306"))
    user = os.getenv("MYSQL_USER", "root")
    pwd = os.getenv("MYSQL_PASSWORD", "root123")
    db = os.getenv("MYSQL_DB", "sales_mysql")
    print(f"[MySQL] Conectando a {host}:{port}/{db}")
    cn = mysql.connector.connect(host=host, port=port, user=user, password=pwd, database=db)
    cur = cn.cursor()
    # truncate
    cur.execute("SET FOREIGN_KEY_CHECKS=0")
    for table in ["OrdenDetalle", "Orden", "Producto", "Cliente"]:
        cur.execute(f"TRUNCATE TABLE {table}")
    cur.execute("SET FOREIGN_KEY_CHECKS=1")

    clientes = json.loads((base_dir / "clientes.json").read_text(encoding="utf-8"))
    productos = json.loads((base_dir / "productos.json").read_text(encoding="utf-8"))
    ordenes = json.loads((base_dir / "ordenes.json").read_text(encoding="utf-8"))
    items = json.loads((base_dir / "orden_items.json").read_text(encoding="utf-8"))

    # clientes
    for chunk in chunked(clientes, 500):
        cur.executemany(
            "INSERT INTO Cliente (nombre, correo, genero, pais, created_at) VALUES (%s,%s,%s,%s,%s)",
            [
                (
                    c["nombre"],
                    f"{c['cliente_id'].lower()}@seed.local",
                    ("M" if str(c["genero"]).lower().startswith("m") else "F" if str(c["genero"]).lower().startswith("f") else "X"),
                    c["pais"],
                    c["fecha_registro"][:10],
                )
                for c in chunk
            ],
        )
    cn.commit()
    cur.execute("SELECT id, correo FROM Cliente")
    cliente_map = {correo.split("@")[0].upper(): cid for cid, correo in cur.fetchall()}

    # productos
    for chunk in chunked(productos, 500):
        cur.executemany(
            "INSERT INTO Producto (codigo_alt, nombre, categoria) VALUES (%s,%s,%s)",
            [(p["sku"], p["nombre"], p["categoria"]) for p in chunk],
        )
    cn.commit()
    cur.execute("SELECT id, codigo_alt FROM Producto")
    prod_map = {code: pid for pid, code in cur.fetchall()}

    # ordenes
    for chunk in chunked(ordenes, 400):
        cur.executemany(
            "INSERT INTO Orden (cliente_id, fecha, canal, moneda, total) VALUES (%s,%s,%s,%s,%s)",
            [
                (
                    cliente_map.get(o["cliente_id"]),
                    o["fecha"][:19].replace("T", " "),
                    o["canal"],
                    o["moneda"],
                    str(o["total"]),
                )
                for o in chunk
                if cliente_map.get(o["cliente_id"])
            ],
        )
    cn.commit()
    # reconstruir mapping por orden de inserción y cliente+fecha
    cur.execute("SELECT id, cliente_id, fecha FROM Orden")
    orden_map = {(cid, f): oid for oid, cid, f in cur.fetchall()}

    inserted = 0
    for it in items:
        ord_obj = next((o for o in ordenes if o["orden_id"] == it["orden_id"]), None)
        if not ord_obj:
            continue
        ord_local = orden_map.get((cliente_map.get(ord_obj["cliente_id"]), ord_obj["fecha"][:19].replace("T", " ")))
        prod_local = prod_map.get(it["producto_id"])
        if not ord_local or not prod_local:
            continue
        cur.execute(
            "INSERT INTO OrdenDetalle (orden_id, producto_id, cantidad, precio_unit) VALUES (%s,%s,%s,%s)",
            (ord_local, prod_local, it["cantidad"], str(it["precio_unit"])),
        )
        inserted += 1
        if inserted % 500 == 0:
            cn.commit()
    cn.commit()
    print(f"[MySQL] Cargados clientes:{len(clientes)}, productos:{len(productos)}, ordenes:{len(ordenes)}, items:{inserted}")
    cur.close()
    cn.close()


def push_mongodb(base_dir: Path):
    try:
        from pymongo import MongoClient  # type: ignore
    except ImportError:
        print("[MongoDB] pymongo no instalado; omitiendo push.")
        return
    uri = os.getenv("MONGODB_URI")
    if not uri:
        print("[MongoDB] MONGODB_URI no definido; omitiendo push.")
        return
    client = MongoClient(uri)
    default_db = client.get_default_database()
    db = default_db if default_db is not None else client["SalesMongoDB"]
    db.clientes.delete_many({})
    db.productos.delete_many({})
    db.ordenes.delete_many({})
    clientes = json.loads((base_dir / "clientes.json").read_text(encoding="utf-8"))
    productos = json.loads((base_dir / "productos.json").read_text(encoding="utf-8"))
    ordenes = json.loads((base_dir / "ordenes.json").read_text(encoding="utf-8"))
    items = json.loads((base_dir / "orden_items.json").read_text(encoding="utf-8"))
    db.clientes.insert_many(
        [
            {
                "_id": c["cliente_id"],
                "nombre": c["nombre"],
                "email": f"{c['cliente_id'].lower()}@seed.local",
                "genero": c["genero"],
                "pais": c["pais"],
                "fecha_registro": c["fecha_registro"],
            }
            for c in clientes
        ]
    )
    try:
        db.productos.create_index("codigo_mongo", unique=True, sparse=True)
    except Exception as e:
        # Si ya existe el índice, ignorar conflicto
        if "IndexKeySpecsConflict" not in str(e):
            raise
    db.productos.insert_many(
        [
            {
                "_id": p["sku"],
                "nombre": p["nombre"],
                "categoria": p["categoria"],
                "equivalencias": {"sku": p["sku"], "codigo_alt": p["codigo_alt"], "codigo_mongo": p["codigo_mongo"]},
                "codigo_mongo": p.get("codigo_mongo"),
            }
            for p in productos
        ],
        ordered=False,
    )
    # Ordenes con items embebidos
    orden_items_map = {}
    for it in items:
        orden_items_map.setdefault(it["orden_id"], []).append(
            {"producto_id": it["producto_id"], "cantidad": it["cantidad"], "precio_unit": it["precio_unit"]}
        )
    db.ordenes.insert_many(
        [
            {
                "_id": o["orden_id"],
                "cliente_id": o["cliente_id"],
                "fecha": o["fecha"],
                "canal": o["canal"],
                "moneda": o["moneda"],
                "total": o["total"],
                "items": orden_items_map.get(o["orden_id"], []),
            }
            for o in ordenes
        ]
    )
    print(f"[MongoDB] Cargados clientes:{len(clientes)}, productos:{len(productos)}, ordenes:{len(ordenes)}")
    client.close()


def push_supabase(base_dir: Path):
    import requests

    url = os.getenv("SUPABASE_URL") or os.getenv("VITE_SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE") or os.getenv("SUPABASE_KEY") or os.getenv("VITE_SUPABASE_ANON_KEY")
    if not url or not key:
        print("[Supabase] SUPABASE_URL/SERVICE_ROLE no definidos; omitiendo push.")
        return
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }
    def post(table, rows, on_conflict=None):
        if not rows:
            return
        h = dict(headers)
        if on_conflict:
            h["Prefer"] = "resolution=merge-duplicates"
        params = {"on_conflict": on_conflict} if on_conflict else None
        resp = requests.post(f"{url}/rest/v1/{table}", headers=h, params=params, json=rows)
        if resp.status_code >= 300:
            print(f"[Supabase] Error insert {table}: {resp.status_code} {resp.text}")
        else:
            print(f"[Supabase] Insert {table}: {len(rows)} rows")
    clientes = json.loads((base_dir / "clientes.json").read_text(encoding="utf-8"))
    productos = json.loads((base_dir / "productos.json").read_text(encoding="utf-8"))
    ordenes = json.loads((base_dir / "ordenes.json").read_text(encoding="utf-8"))
    items = json.loads((base_dir / "orden_items.json").read_text(encoding="utf-8"))
    # Limpieza rápida
    for tbl in ["orden_detalle", "orden", "producto", "cliente"]:
        requests.delete(f"{url}/rest/v1/{tbl}", headers=headers, params={"select": "*"})
    def id_to_uuid(s: str) -> str:
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, s))

    # Limpieza rápida
    for tbl in ["orden_detalle", "orden", "producto", "cliente"]:
        requests.delete(f"{url}/rest/v1/{tbl}", headers=headers, params={"select": "*"})

    cliente_map = {c["cliente_id"]: id_to_uuid(c["cliente_id"]) for c in clientes}
    producto_map = {p["sku"]: id_to_uuid(p["sku"]) for p in productos}
    orden_map = {o["orden_id"]: id_to_uuid(o["orden_id"]) for o in ordenes}

    def map_genero(g: str) -> str:
        g = (g or "").lower()
        if g.startswith("m"):
            return "M"
        if g.startswith("f"):
            return "F"
        return "M"

    allowed_canal = {"WEB", "APP", "PARTNER"}

    post(
        "cliente",
        [
            {
                "cliente_id": cliente_map[c["cliente_id"]],
                "nombre": c["nombre"],
                "email": f"{c['cliente_id'].lower()}@seed.local",
                "genero": map_genero(c["genero"]),
                "pais": c["pais"],
                "fecha_registro": c["fecha_registro"],
            }
            for c in clientes
        ],
        on_conflict="cliente_id",
    )
    post(
        "producto",
        [
            {
                "producto_id": producto_map[p["sku"]],
                "nombre": p["nombre"],
                "categoria": p["categoria"],
                "sku": p["sku"],
            }
            for p in productos
        ],
        on_conflict="producto_id",
    )
    post(
        "orden",
        [
            {
                "orden_id": orden_map[o["orden_id"]],
                "cliente_id": cliente_map.get(o["cliente_id"]),
                "fecha": o["fecha"],
                "canal": o["canal"] if o["canal"] in allowed_canal else "WEB",
                "moneda": o["moneda"],
                "total": o["total"],
            }
            for o in ordenes
        ],
        on_conflict="orden_id",
    )
    post(
        "orden_detalle",
        [
            {
                "orden_id": orden_map.get(it["orden_id"]),
                "producto_id": producto_map.get(it["producto_id"]),
                "cantidad": it["cantidad"],
                "precio_unit": it["precio_unit"],
            }
            for it in items
        ],
    )


def push_neo4j(base_dir: Path):
    try:
        from neo4j import GraphDatabase  # type: ignore
    except ImportError:
        print("[Neo4j] neo4j driver no instalado; omitiendo push.")
        return
    uri = os.getenv("NEO4J_URI")
    user = os.getenv("NEO4J_USER")
    pwd = os.getenv("NEO4J_PASSWORD")
    if not uri or not user or not pwd:
        print("[Neo4j] Credenciales no definidas; omitiendo push.")
        return
    driver = GraphDatabase.driver(uri, auth=(user, pwd))
    clientes = json.loads((base_dir / "clientes.json").read_text(encoding="utf-8"))
    productos = json.loads((base_dir / "productos.json").read_text(encoding="utf-8"))
    ordenes = json.loads((base_dir / "ordenes.json").read_text(encoding="utf-8"))
    items = json.loads((base_dir / "orden_items.json").read_text(encoding="utf-8"))
    with driver.session() as sess:
        sess.run("MATCH (n) DETACH DELETE n")
        sess.run("CREATE CONSTRAINT IF NOT EXISTS FOR (c:Cliente) REQUIRE c.id IS UNIQUE")
        sess.run("CREATE CONSTRAINT IF NOT EXISTS FOR (p:Producto) REQUIRE p.id IS UNIQUE")
        sess.run("CREATE CONSTRAINT IF NOT EXISTS FOR (o:Orden) REQUIRE o.id IS UNIQUE")
        # Clientes
        sess.run(
            """
            UNWIND $rows AS r
            MERGE (c:Cliente {id:r.cliente_id})
            SET c.nombre = r.nombre, c.pais = r.pais, c.genero = r.genero
            """,
            rows=clientes,
        )
        # Productos
        sess.run(
            """
            UNWIND $rows AS r
            MERGE (p:Producto {id:r.sku})
            SET p.nombre = r.nombre, p.categoria = r.categoria, p.codigo_alt = r.codigo_alt
            """,
            rows=productos,
        )
        # Ordenes + items
        # Primero ordenes
        sess.run(
            """
            UNWIND $rows AS r
            MERGE (o:Orden {id:r.orden_id})
            SET o.fecha = r.fecha, o.canal = r.canal, o.moneda = r.moneda, o.total = r.total
            WITH o, r
            MATCH (c:Cliente {id:r.cliente_id})
            MERGE (c)-[:REALIZO]->(o)
            """,
            rows=ordenes,
        )
        # Items
        sess.run(
            """
            UNWIND $rows AS r
            MATCH (o:Orden {id:r.orden_id})
            MATCH (p:Producto {id:r.producto_id})
            MERGE (o)-[:CONTIENTE {cantidad:r.cantidad, precio_unit:r.precio_unit}]->(p)
            """,
            rows=items,
        )
    driver.close()
    print(f"[Neo4j] Cargados clientes:{len(clientes)}, productos:{len(productos)}, ordenes:{len(ordenes)}, items:{len(items)}")


def main() -> None:
    hydrate_env_defaults()
    parser = argparse.ArgumentParser(description="Generar y opcionalmente subir dataset canónico.")
    parser.add_argument("--push", action="store_true", help="Subir datos a las bases.")
    parser.add_argument(
        "--targets",
        type=lambda s: [x.strip() for x in s.split(",")],
        default=["mssql", "mysql", "mongodb", "supabase", "neo4j"],
        help="Lista de targets a subir (mssql,mysql,mongodb,supabase,neo4j)",
    )
    args = parser.parse_args()

    random.seed(RANDOM_SEED)

    clientes = build_clientes()
    productos = build_productos()
    ordenes, items = build_ordenes_y_items(clientes, productos)

    out_dir = Path("shared/generated")
    write_json(out_dir / "clientes.json", [asdict(c) for c in clientes])
    write_json(out_dir / "productos.json", [asdict(p) for p in productos])
    write_json(out_dir / "ordenes.json", [asdict(o) for o in ordenes])
    write_json(out_dir / "orden_items.json", [asdict(it) for it in items])

    # CSV opcional
    write_csv(out_dir / "clientes.csv", [asdict(c) for c in clientes])
    write_csv(out_dir / "productos.csv", [asdict(p) for p in productos])
    write_csv(out_dir / "ordenes.csv", [asdict(o) for o in ordenes])
    write_csv(out_dir / "orden_items.csv", [asdict(it) for it in items])

    # Splits por fuente (porcentajes para distribuir carga; todos usan IDs oficiales)
    splits = {
        "mssql": 1.0,    # MSSQL guarda el dataset completo
        "mysql": 0.35,
        "mongodb": 0.35,
        "supabase": 0.35,
        "neo4j": 0.35,
    }
    # Clientes y órdenes se muestrean con solape parcial
    for target, ratio in splits.items():
        tdir = out_dir / target
        if ratio >= 1.0:
            cli_t = clientes
            ord_t = ordenes
            items_t = items
        else:
            cli_count = max(1, int(len(clientes) * ratio))
            ord_count = max(1, int(len(ordenes) * ratio))
            cli_t = random.sample(clientes, cli_count)
            cli_ids = {c.cliente_id for c in cli_t}
            ord_t = [o for o in ordenes if o.cliente_id in cli_ids]
            if len(ord_t) > ord_count:
                ord_t = random.sample(ord_t, ord_count)
            ord_ids = {o.orden_id for o in ord_t}
            items_t = [it for it in items if it.orden_id in ord_ids]

        write_json(tdir / "clientes.json", [asdict(c) for c in cli_t])
        write_json(tdir / "ordenes.json", [asdict(o) for o in ord_t])
        write_json(tdir / "orden_items.json", [asdict(it) for it in items_t])
        write_json(tdir / "productos.json", [asdict(p) for p in productos])  # catálogo completo compartido

    print(
        f"Generado en {out_dir} -> clientes:{len(clientes)}, productos:{len(productos)}, "
        f"ordenes:{len(ordenes)}, items:{len(items)}"
    )

    if args.push:
        targets = set(args.targets)
        base_dir = out_dir
        if "mssql" in targets:
            push_mssql(base_dir / "mssql")
        if "mysql" in targets:
            push_mysql(base_dir / "mysql")
        if "mongodb" in targets:
            push_mongodb(base_dir / "mongodb")
        if "supabase" in targets:
            push_supabase(base_dir / "supabase")
        if "neo4j" in targets:
            push_neo4j(base_dir / "neo4j")


if __name__ == "__main__":
    main()
