import os
from datetime import datetime, date, timedelta

from conexion import *


LOG_FILE_PATH = "SUPABASE/etl/etl_processed_dates.log"

def map_channel_type(canal: str) -> str:
    if canal == 'WEB':
        return 'Website'
    if canal == 'APP':
        return 'App'
    if canal == 'PARTNER':
        return 'Partner'
    return 'Other'


def generate_sku_for_product(cursor, product_row: dict) -> str:
    # Buscar el último servicio ya creado (S + 4 dígitos), ordenado desc
    cursor.execute(
        """
        SELECT TOP 1 code
        FROM DimProduct
        WHERE code LIKE 'S____'
        ORDER BY code DESC;
        """
    )
    row = cursor.fetchone()

    last_number = 0
    if row and row[0]:
        last_code = str(row[0])

        # Esperamos algo como "S0007"
        if last_code[0].upper() == "S" and len(last_code) >= 2:
            try:
                last_number = int(last_code[1:])  # convierte "0007" → 7
            except ValueError:
                # Si por alguna razón el código no es numérico después de la S, empezamos desde 0
                last_number = 0

    new_number = last_number + 1
    if new_number > 9999:
        raise RuntimeError("Se alcanzó el máximo de códigos de servicio (S9999).")

    # Formato: S + 4 dígitos, con ceros a la izquierda (S0001, S0002, ...)
    new_code = f"S{new_number:04d}"
    return new_code



# ===============================
#  LOG DE FECHAS PROCESADAS
# ===============================

def load_processed_dates() -> set[date]:
    processed = set()
    if not os.path.exists(LOG_FILE_PATH):
        return processed

    with open(LOG_FILE_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                d = date.fromisoformat(line)
                processed.add(d)
            except ValueError:
                # línea rara → la ignoramos
                pass
    return processed


def append_processed_dates(new_dates: set[date]) -> None:
    """
    Agrega al log las nuevas fechas procesadas.
    No pasa nada si hay repetidas; al leer usamos set().
    """
    if not new_dates:
        return

    with open(LOG_FILE_PATH, "a", encoding="utf-8") as f:
        for d in sorted(new_dates):
            f.write(d.isoformat() + "\n")


# ===============================
#  LOOKUPS / INSERTS EN DIMENSIONES
# ===============================

def get_or_create_customer(cursor, nombre, email, genero, pais, fecha_registro) -> int:
    # 1 buscar por emial
    cursor.execute(
        "SELECT id FROM DimCustomer WHERE email = ?",
        (email,)
    )
    row = cursor.fetchone()
    if row:
        return int(row[0])

    # 2 si no existe, inserta
    gender_norm = genero

    #convertir fecharegistro a datetime para poder meterlo en createdat
    if isinstance(fecha_registro, datetime):
        created_at = fecha_registro
    else:
        created_at = datetime.combine(fecha_registro, datetime.min.time())

    cursor.execute(
        """
        INSERT INTO DimCustomer (name, email, gender, country, created_at)
        OUTPUT INSERTED.id
        VALUES (?, ?, ?, ?, ?);
        """,
        (nombre, email, gender_norm, pais, created_at)
    )
    new_id_row = cursor.fetchone()
    if not new_id_row or new_id_row[0] is None:
        raise RuntimeError("No se pudo obtener el ID insertado para DimCustomer")

    return int(new_id_row[0])



def get_or_create_category(cursor, categoria: str) -> int:
    cursor.execute(
        "SELECT id FROM DimCategory WHERE name = ?",
        (categoria,)
    )
    row = cursor.fetchone()
    if row:
        return int(row[0])

    cursor.execute(
        """
        INSERT INTO DimCategory (name)
        OUTPUT INSERTED.id
        VALUES (?);
        """,
        (categoria,)
    )
    new_id_row = cursor.fetchone()
    if not new_id_row or new_id_row[0] is None:
        raise RuntimeError("No se pudo obtener el ID insertado para DimCategory")

    return int(new_id_row[0])



def get_or_create_product(cursor, product_row: dict, category_id: int) -> int:
    sku = product_row.get("sku")
    nombre = product_row["nombre"]

    if sku:
        # Buscar por SKU
        cursor.execute(
            "SELECT id FROM DimProduct WHERE code = ?",
            (sku,)
        )
        row = cursor.fetchone()
        if row:
            return int(row[0])

        # si no existe, insertar nuevo con ese sku
        cursor.execute(
            """
            INSERT INTO DimProduct (name, code, categoryId)
            OUTPUT INSERTED.id
            VALUES (?, ?, ?);
            """,
            (nombre, sku, category_id)
        )
        new_id_row = cursor.fetchone()
        if not new_id_row or new_id_row[0] is None:
            raise RuntimeError("No se pudo obtener el ID insertado para DimProduct (con sku)")
        return int(new_id_row[0])

    # si el sku es null, buscar por nombre y categoria
    cursor.execute(
        """
        SELECT id FROM DimProduct
        WHERE name = ? AND categoryId = ?;
        """,
        (nombre, category_id)
    )
    row = cursor.fetchone()
    if row:
        return int(row[0])

    # No existe: generar un SKU (TODO) e insertar
    generated_sku = generate_sku_for_product(cursor, product_row)

    cursor.execute(
        """
        INSERT INTO DimProduct (name, code, categoryId)
        OUTPUT INSERTED.id
        VALUES (?, ?, ?);
        """,
        (nombre, generated_sku, category_id)
    )
    new_id_row = cursor.fetchone()
    if not new_id_row or new_id_row[0] is None:
        raise RuntimeError("No se pudo obtener el ID insertado para DimProduct (sin sku)")
    return int(new_id_row[0])

def get_or_create_time(cursor, fecha: date) -> int:
    cursor.execute(
        "SELECT id FROM DimTime WHERE date = ?",
        (fecha,)
    )
    row = cursor.fetchone()
    if row:
        return int(row[0])

    year = fecha.year
    month = fecha.month
    day = fecha.day

    cursor.execute(
        """
        INSERT INTO DimTime (year, month, day, date)
        OUTPUT INSERTED.id
        VALUES (?, ?, ?, ?);
        """,
        (year, month, day, fecha)
    )
    new_row = cursor.fetchone()

    return int(new_row[0])



def get_or_create_channel(cursor, canal_supabase: str) -> int:
    channel_type = map_channel_type(canal_supabase)

    cursor.execute(
        "SELECT id FROM DimChannel WHERE channelType = ?",
        (channel_type,)
    )
    row = cursor.fetchone()
    if row:
        return int(row[0])

    cursor.execute(
        """
        INSERT INTO DimChannel (channelType)
        OUTPUT INSERTED.id
        VALUES (?);
        """,
        (channel_type,)
    )
    new_id_row = cursor.fetchone()
    if not new_id_row or new_id_row[0] is None:
        raise RuntimeError("No se pudo obtener el ID insertado para DimChannel")
    return int(new_id_row[0])



def get_exchange_rate_for_date(cursor, fecha: date):
    cursor.execute(
        """
        SELECT id, rate
        FROM DimExchangeRate
        WHERE fromCurrency = 'CRC'
          AND toCurrency = 'USD'
          AND date = ?;
        """,
        (fecha,)
    )
    row = cursor.fetchone()
    if not row:
        raise RuntimeError(f"No hay tipo de cambio en DimExchangeRate para la fecha {fecha}")
    return int(row[0]), float(row[1])


# ===============================
#  ETL PRINCIPAL
# ===============================

def run_etl_supabase_to_dw(effective_dates: set[date]):
    """
    effective_dates: conjunto de fechas (date) que SÍ se deben procesar en este ETL.
    Cualquier registro cuya fecha no esté en este set será ignorado.
    """
    if not effective_dates:
        print("No hay fechas efectivas para procesar (effective_dates vacío).")
        return

    supabase = get_supabase_client()
    conn = get_dw_connection()
    cursor = conn.cursor()

    # Diccionarios de mapeo (UUID Supabase -> ID DW)
    cliente_uuid_to_id = {}
    producto_uuid_to_id = {}
    orden_uuid_to_id = {}
    orden_uuid_to_time_id = {}
    orden_uuid_to_channel_id = {}
    orden_uuid_to_customer_id = {}
    orden_uuid_to_exchange_rate_id = {}
    orden_uuid_to_currency = {}
    orden_uuid_to_rate = {}
    orden_uuid_to_created_at = {}

    try:
        # =========================
        # 1. CLIENTES → DimCustomer
        # =========================
        print("Extrayendo clientes de Supabase...")
        clientes_resp = supabase.table("cliente").select("*").execute()
        clientes = clientes_resp.data or []
        print(f"Clientes Supabase (total): {len(clientes)}")

        for cli in clientes:
            cliente_id = cli["cliente_id"]
            nombre = cli["nombre"]
            email = cli["email"]
            genero = cli["genero"]
            pais = cli["pais"]
            fecha_registro = cli["fecha_registro"]  # Supabase suele devolver string 'YYYY-MM-DD'

            if isinstance(fecha_registro, str):
                fecha_registro = datetime.fromisoformat(fecha_registro)

            fecha_cli = fecha_registro.date()

            # Si la fecha de registro NO está en el rango efectivo → lo ignoramos
            if fecha_cli not in effective_dates:
                continue

            dim_id = get_or_create_customer(cursor, nombre, email, genero, pais, fecha_cli)
            cliente_uuid_to_id[cliente_id] = dim_id

        # =========================
        # 2. PRODUCTOS → DimCategory + DimProduct
        # =========================
        print("Extrayendo productos de Supabase...")
        productos_resp = supabase.table("producto").select("*").execute()
        productos = productos_resp.data or []
        print(f"Productos Supabase (total): {len(productos)}")

        for prod in productos:
            producto_uuid = prod["producto_id"]
            categoria = prod["categoria"]

            # NUEVO: filtro por fecha_registro del producto
            fecha_reg_raw = prod.get("fecha_registro")
            if fecha_reg_raw is None:
                # Si no tiene fecha_registro, por ahora lo ignoramos
                continue

            if isinstance(fecha_reg_raw, str):
                fecha_registro = datetime.fromisoformat(fecha_reg_raw)
            else:
                fecha_registro = fecha_reg_raw

            fecha_prod = fecha_registro.date()

            if fecha_prod not in effective_dates:
                continue

            category_id = get_or_create_category(cursor, categoria)
            product_id = get_or_create_product(cursor, prod, category_id)

            producto_uuid_to_id[producto_uuid] = product_id

        # =========================
        # 3. ÓRDENES → DimOrder (+ DimTime, DimChannel, DimExchangeRate)
        # =========================
        print("Extrayendo órdenes de Supabase...")
        ordenes_resp = supabase.table("orden").select("*").execute()
        ordenes = ordenes_resp.data or []
        print(f"Órdenes Supabase (total): {len(ordenes)}")

        for ord_row in ordenes:
            orden_uuid = ord_row["orden_id"]
            cliente_uuid = ord_row["cliente_id"]
            fecha_raw = ord_row["fecha"]      # normalmente string ISO
            canal = ord_row["canal"]          # 'WEB','APP','PARTNER'
            moneda = ord_row["moneda"]        # 'USD' o 'CRC'
            total = float(ord_row["total"])

            # Parse fecha a datetime
            if isinstance(fecha_raw, str):
                fecha_dt = datetime.fromisoformat(fecha_raw.replace("Z", "+00:00"))
            else:
                fecha_dt = fecha_raw
            fecha_only = fecha_dt.date()

            # Si la fecha de la orden no está en el rango efectivo → ignorar esta orden
            if fecha_only not in effective_dates:
                continue

            # FK a DimCustomer:
            # primero intentamos usar el mapping cargado en la fase de clientes
            if cliente_uuid in cliente_uuid_to_id:
                customer_id = cliente_uuid_to_id[cliente_uuid]
            else:
                # Cliente fue creado fuera de rango, pero igual lo necesitamos.
                # Lo traemos directo de Supabase y lo insertamos/ubicamos en DimCustomer.
                cli_resp = supabase.table("cliente").select("*").eq("cliente_id", cliente_uuid).execute()
                cli_data = (cli_resp.data or [])
                if not cli_data:
                    raise RuntimeError(f"No se encontró cliente {cliente_uuid} para la orden {orden_uuid}")

                cli = cli_data[0]
                nombre = cli["nombre"]
                email = cli["email"]
                genero = cli["genero"]
                pais = cli["pais"]
                fecha_registro = cli["fecha_registro"]
                if isinstance(fecha_registro, str):
                    fecha_registro = datetime.fromisoformat(fecha_registro)

                customer_id = get_or_create_customer(
                    cursor,
                    nombre,
                    email,
                    genero,
                    pais,
                    fecha_registro.date()
                )
                cliente_uuid_to_id[cliente_uuid] = customer_id

            # DimTime
            time_id = get_or_create_time(cursor, fecha_only)

            # DimChannel
            channel_id = get_or_create_channel(cursor, canal)

            # DimExchangeRate
            exchange_rate_id, rate_value = get_exchange_rate_for_date(cursor, fecha_only)

            # Normalizar total a USD
            if moneda == "USD":
                total_usd = total
            elif moneda == "CRC":
                total_usd = total / rate_value
            else:
                raise RuntimeError(f"Moneda desconocida en orden {orden_uuid}: {moneda}")

            cursor.execute(
                """
                INSERT INTO DimOrder (totalOrderUSD)
                OUTPUT INSERTED.id
                VALUES (?);
                """,
                (total_usd,)
            )
            order_id_row = cursor.fetchone()
            if not order_id_row or order_id_row[0] is None:
                raise RuntimeError(f"No se pudo obtener el ID insertado para DimOrder (orden {orden_uuid})")
            order_id = int(order_id_row[0])

            # Guardar mappings
            orden_uuid_to_id[orden_uuid] = order_id
            orden_uuid_to_time_id[orden_uuid] = time_id
            orden_uuid_to_channel_id[orden_uuid] = channel_id
            orden_uuid_to_customer_id[orden_uuid] = customer_id
            orden_uuid_to_exchange_rate_id[orden_uuid] = exchange_rate_id
            orden_uuid_to_currency[orden_uuid] = moneda
            orden_uuid_to_rate[orden_uuid] = rate_value
            orden_uuid_to_created_at[orden_uuid] = fecha_dt

        # =========================
        # 4. DETALLES → FactSales
        # =========================
        print("Extrayendo detalles de órdenes de Supabase...")
        detalles_resp = supabase.table("orden_detalle").select("*").execute()
        detalles = detalles_resp.data or []
        print(f"Detalles Supabase (total): {len(detalles)}")

        for det in detalles:
            orden_uuid = det["orden_id"]
            producto_uuid = det["producto_id"]

            # Si la orden no fue procesada (por estar fuera de rango), saltamos el detalle
            if orden_uuid not in orden_uuid_to_id:
                continue

            cantidad = int(det["cantidad"])
            precio_unit = float(det["precio_unit"])

            # FKs desde mapeos en memoria
            # Si el producto no está en el mapping (porque se creó fuera de rango),
            # podemos traerlo on-demand desde Supabase también.
            if producto_uuid in producto_uuid_to_id:
                product_id = producto_uuid_to_id[producto_uuid]
            else:
                prod_resp = supabase.table("producto").select("*").eq("producto_id", producto_uuid).execute()
                prod_data = prod_resp.data or []
                if not prod_data:
                    raise RuntimeError(f"No se encontró producto {producto_uuid} para el detalle de orden {orden_uuid}")
                prod_row = prod_data[0]
                categoria = prod_row["categoria"]
                category_id = get_or_create_category(cursor, categoria)
                product_id = get_or_create_product(cursor, prod_row, category_id)
                producto_uuid_to_id[producto_uuid] = product_id

            order_id = orden_uuid_to_id[orden_uuid]
            time_id = orden_uuid_to_time_id[orden_uuid]
            channel_id = orden_uuid_to_channel_id[orden_uuid]
            customer_id = orden_uuid_to_customer_id[orden_uuid]
            exchange_rate_id = orden_uuid_to_exchange_rate_id[orden_uuid]
            currency = orden_uuid_to_currency[orden_uuid]
            rate_value = orden_uuid_to_rate[orden_uuid]
            created_at = orden_uuid_to_created_at[orden_uuid]

            # Calcular precios en USD
            if currency == "USD":
                unit_price_usd = precio_unit
            elif currency == "CRC":
                unit_price_usd = precio_unit / rate_value
            else:
                raise RuntimeError(f"Moneda desconocida en detalle de orden {orden_uuid}: {currency}")

            line_total_usd = cantidad * unit_price_usd
            discount_percentage = 0.0  # supabase no maneja descuentos

            cursor.execute(
                """
                INSERT INTO FactSales (
                    productId,
                    timeId,
                    orderId,
                    channelId,
                    customerId,
                    productCant,
                    productUnitPriceUSD,
                    lineTotalUSD,
                    discountPercentage,
                    created_at,
                    exchangeRateId
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    product_id,
                    time_id,
                    order_id,
                    channel_id,
                    customer_id,
                    cantidad,
                    unit_price_usd,
                    line_total_usd,
                    discount_percentage,
                    created_at,
                    exchange_rate_id
                )
            )


        # Si todo salió bien:
        conn.commit()
        print("ETL Supabase → DW completado correctamente.")

    except Exception as e:
        conn.rollback()
        print(f"[ERROR] Se hizo rollback del ETL por error: {e}")
        raise

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    # Pedir rango de fechas al usuario
    def ask_date(prompt: str) -> date:
        while True:
            s = input(prompt).strip()
            try:
                # Formato ISO: YYYY-MM-DD
                d = datetime.strptime(s, "%Y-%m-%d").date()
                return d
            except ValueError:
                print("Fecha inválida. Usa formato YYYY-MM-DD, por ejemplo 2025-04-13.")

    print("=== ETL Supabase → DW con rango de fechas ===")
    start_date = ask_date("Fecha inicio (YYYY-MM-DD): ")
    end_date = ask_date("Fecha fin   (YYYY-MM-DD): ")

    today = date.today()
    if end_date >= today:
        print("La fecha de fin no puede ser hoy ni una fecha futura.")
        raise SystemExit(1)

    if start_date > end_date:
        print("La fecha de inicio no puede ser mayor que la fecha de fin.")
        raise SystemExit(1)

    # Construir lista de fechas del rango
    all_dates = []
    current = start_date
    while current <= end_date:
        all_dates.append(current)
        current = current + timedelta(days=1)

    # Cargar fechas ya procesadas desde el log
    processed_dates = load_processed_dates()
    print(f"Fechas ya procesadas (log): {[d.isoformat() for d in sorted(processed_dates)]}")

    # Quitar del rango las fechas ya procesadas
    effective_dates = {d for d in all_dates if d not in processed_dates}

    if not effective_dates:
        print("Todas las fechas del rango ya fueron procesadas. No hay nada que hacer.")
        raise SystemExit(0)

    print(f"Fechas a procesar en esta corrida: {[d.isoformat() for d in sorted(effective_dates)]}")

    try:
        run_etl_supabase_to_dw(effective_dates)
    except Exception as e:
        print("ETL falló, NO se actualiza el log de fechas.")
        raise
    else:
        # Solo si el ETL terminó bien, actualizamos el log
        append_processed_dates(effective_dates)
        print("Log actualizado. Fechas marcadas como procesadas:")
        print(", ".join(d.isoformat() for d in sorted(effective_dates)))

