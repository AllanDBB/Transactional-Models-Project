"""
Carga un dataset pequeño de prueba ("testSet") en Neo4j.
Genera ~50 órdenes con clientes/productos/categorías y relaciones CONTIENTE.
No borra datos existentes; usa MERGE con ids deterministas.
"""
import random
from datetime import datetime, timedelta

from neo4j import GraphDatabase

from config import NeoConfig


def main():
    random.seed(42)
    driver = GraphDatabase.driver(NeoConfig.URI, auth=(NeoConfig.USER, NeoConfig.PASSWORD))

    categorias = ["ALIMENTOS", "TECNOLOGIA", "LIBROS", "HOGAR", "DEPORTE"]

    clientes = []
    for i in range(10):
        clientes.append(
            {
                "id": f"cli-{i+1}",
                "nombre": f"Cliente Test {i+1}",
                "email": f"cliente{i+1}@test.com",
                "genero": random.choice(["Masculino", "Femenino", "Otro"]),
                "pais": random.choice(["CR", "US", "MX"]),
            }
        )

    productos = []
    for i in range(10):
        productos.append(
            {
                "id": f"prod-{i+1}",
                "nombre": f"Producto Test {i+1}",
                "sku": f"SKU-{1000+i}",
                "codigo_alt": f"ALT-{1000+i}",
                "codigo_mongo": f"MN-{1000+i}",
                "categoria": random.choice(categorias),
            }
        )

    ordenes = []
    for i in range(50):
        cliente = random.choice(clientes)
        fecha = datetime.now() - timedelta(days=random.randint(0, 30), hours=random.randint(0, 23))
        canal = random.choice(["WEB", "TIENDA", "APP", "PARTNER"])
        moneda = random.choice(["CRC", "USD"])
        items = []
        total_crc = 0
        for _ in range(2):
            prod = random.choice(productos)
            cantidad = random.randint(1, 3)
            precio = random.randint(5000, 25000)
            items.append({"producto_id": prod["id"], "cantidad": cantidad, "precio_unit": precio})
            total_crc += precio * cantidad
        total = round(total_crc / 530, 2) if moneda == "USD" else total_crc

        ordenes.append(
            {
                "id": f"ord-{i+1}",
                "cliente_id": cliente["id"],
                "fecha": fecha.isoformat(),
                "canal": canal,
                "moneda": moneda,
                "total": total,
                "items": items,
            }
        )

    q_categorias = """
    UNWIND $categorias AS cat
      MERGE (c:Categoria {id: cat, nombre: cat})
    """

    q_clientes = """
    UNWIND $clientes AS cli
      MERGE (c:Cliente {id: cli.id})
      SET c.nombre = cli.nombre, c.email = cli.email, c.genero = cli.genero, c.pais = cli.pais
    """

    q_productos = """
    UNWIND $productos AS p
      MERGE (prod:Producto {id: p.id})
      SET prod.nombre = p.nombre, prod.sku = p.sku, prod.codigo_alt = p.codigo_alt, prod.codigo_mongo = p.codigo_mongo
      WITH prod, p
      MATCH (cat:Categoria {nombre: p.categoria})
      MERGE (prod)-[:PERTENECE_A]->(cat)
    """

    q_ordenes = """
    UNWIND $ordenes AS o
      MATCH (cli:Cliente {id: o.cliente_id})
      MERGE (ord:Orden {id: o.id})
      SET ord.fecha = datetime(o.fecha), ord.canal = o.canal, ord.moneda = o.moneda, ord.total = o.total
      MERGE (cli)-[:REALIZO]->(ord)
      WITH ord, o
      UNWIND o.items AS it
        MATCH (prod:Producto {id: it.producto_id})
        MERGE (ord)-[r:CONTIENTE]->(prod)
        SET r.cantidad = it.cantidad, r.precio_unit = it.precio_unit
    """

    with driver.session(database=NeoConfig.DATABASE) as session:
        session.run(q_categorias, categorias=categorias)
        session.run(q_clientes, clientes=clientes)
        session.run(q_productos, productos=productos)
        session.run(q_ordenes, ordenes=ordenes)

    driver.close()
    print("Dataset testSet cargado en Neo4j (50 órdenes, 10 clientes, 10 productos).")


if __name__ == "__main__":
    main()
