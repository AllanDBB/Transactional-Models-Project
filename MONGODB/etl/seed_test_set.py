"""
Carga un dataset pequeño de prueba ("testSet") en MongoDB.
Genera ~50 órdenes con clientes/productos básicos en CRC.
No borra datos existentes; usa IDs y códigos deterministas para evitar duplicados.
"""
import random
from datetime import datetime, timedelta

from pymongo import MongoClient, UpdateOne

from config import MongoConfig


def build_sample_data(n_orders: int = 50):
    random.seed(42)

    # 10 clientes
    clientes = []
    for i in range(10):
        clientes.append(
            {
                "_id": f"cli-{i+1}",
                "nombre": f"Cliente Test {i+1}",
                "email": f"cliente{i+1}@test.com",
                "genero": random.choice(["Masculino", "Femenino", "Otro"]),
                "pais": random.choice(["CR", "US", "MX"]),
                "creado": datetime.now() - timedelta(days=random.randint(0, 60)),
            }
        )

    # 10 productos
    categorias = ["ALIMENTOS", "TECNOLOGIA", "LIBROS", "HOGAR", "DEPORTE"]
    productos = []
    for i in range(10):
        productos.append(
            {
                "_id": f"prod-{i+1}",
                "codigo_mongo": f"MN-{1000+i}",
                "nombre": f"Producto Test {i+1}",
                "categoria": random.choice(categorias),
                "equivalencias": {
                    "sku": f"SKU-{1000+i}",
                    "codigo_alt": f"ALT-{1000+i}",
                },
            }
        )

    # Órdenes + items
    ordenes = []
    for i in range(n_orders):
        cliente = random.choice(clientes)
        fecha = datetime.now() - timedelta(days=random.randint(0, 30), hours=random.randint(0, 23))
        canal = random.choice(["WEB", "TIENDA", "APP"])
        moneda = random.choice(["CRC", "USD"])
        items = []
        total_crc = 0
        total_usd = 0.0
        for _ in range(2):
            prod = random.choice(productos)
            cantidad = random.randint(1, 3)
            precio = random.randint(5000, 25000)  # CRC entero
            items.append(
                {"producto_id": prod["_id"], "cantidad": cantidad, "precio_unit": precio}
            )
            total_crc += precio * cantidad
        if moneda == "USD":
            # simula CRC->USD a ~530
            total_usd = round(total_crc / 530, 2)

        ordenes.append(
            {
                "_id": f"ord-{i+1}",
                "cliente_id": cliente["_id"],
                "fecha": fecha,
                "canal": canal,
                "moneda": moneda,
                "total": total_usd if moneda == "USD" else total_crc,
                "items": items,
            }
        )

    return clientes, productos, ordenes


def upsert_collection(collection, documents, key="_id"):
    ops = [UpdateOne({key: doc[key]}, {"$set": doc}, upsert=True) for doc in documents]
    if ops:
        result = collection.bulk_write(ops, ordered=False)
        return result.upserted_count + result.modified_count
    return 0


def main():
    client = MongoClient(MongoConfig.uri())
    db = client[MongoConfig.DATABASE]

    clientes, productos, ordenes = build_sample_data()

    inserted_cli = upsert_collection(db.clientes, clientes)
    inserted_prod = upsert_collection(db.productos, productos)
    inserted_ord = upsert_collection(db.ordenes, ordenes)

    print(f"Clientes upserted: {inserted_cli}")
    print(f"Productos upserted: {inserted_prod}")
    print(f"Ordenes upserted: {inserted_ord}")


if __name__ == "__main__":
    main()
