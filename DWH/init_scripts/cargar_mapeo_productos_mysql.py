#!/usr/bin/env python3
"""
Carga mapeo de productos (SKU <-> codigo_alt) desde JSON al staging del DWH.
Usa staging.map_producto y evita reconectar por cada insert.
"""

import json
import logging
import os
import sys
from pathlib import Path

import pymssql
from dotenv import load_dotenv

# Cargar variables de entorno desde DWH/ o raiz
dwh_env = Path(__file__).parent.parent / ".env"
root_env = Path(__file__).parent.parent.parent / ".env"

if dwh_env.exists():
    load_dotenv(dwh_env)
elif root_env.exists():
    load_dotenv(root_env)
else:
    load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class ProductMappingLoader:
    """Carga mappings de productos (SKU <-> codigo_alt) al DWH"""

    def __init__(self):
        # Variables SQL Server
        self.server = os.getenv("serverenv", "localhost,1434")
        self.database = os.getenv("databaseenv", "MSSQL_DW")
        self.username = os.getenv("usernameenv", "sa")
        self.password = os.getenv("passwordenv", "BasesDatos2!")

        print(f"Conectando a SQL Server en {self.server}, base de datos {self.database}")
        print(f"Usuario SQL Server: {self.username}")

        # Rutas de archivos (Docker / local)
        docker_path = Path("/app/shared/generated/mysql/json/productos.json")
        local_path = Path(__file__).parent.parent.parent / "shared" / "generated" / "mysql" / "json" / "productos.json"
        self.mysql_productos_json = docker_path if docker_path.exists() else local_path

    def load_productos_json(self):
        """Lee el archivo JSON de productos MySQL"""
        try:
            with open(self.mysql_productos_json, "r", encoding="utf-8") as f:
                productos = json.load(f)
            logger.info(f"[OK] Cargados {len(productos)} productos desde JSON")
            return productos
        except FileNotFoundError:
            logger.error(f"[ERROR] Archivo no encontrado: {self.mysql_productos_json}")
            return []
        except json.JSONDecodeError as e:
            logger.error(f"[ERROR] Error al parsear JSON: {e}")
            return []

    def connect_to_database(self):
        """Conecta a SQL Server DWH usando pymssql"""
        try:
            server_parts = self.server.replace(",", ":").split(":")
            server_host = server_parts[0]
            server_port = int(server_parts[1]) if len(server_parts) > 1 else 1433

            if server_host == "localhost":
                server_host = "sqlserver-dw"
                server_port = 1433

            connection = pymssql.connect(
                server=server_host,
                port=server_port,
                user=self.username,
                password=self.password,
                database=self.database,
                timeout=10,
                as_dict=False,
            )

            logger.debug("[OK] Conexion a base de datos exitosa")
            return connection
        except pymssql.DatabaseError as e:
            logger.error(f"[ERROR] Error de base de datos: {e}")
            return None
        except Exception as e:
            logger.error(f"[ERROR] Error conectando a BD: {e}")
            return None

    def load_product_mappings(self):
        """Carga los mappings de productos desde JSON al DWH"""
        print("\n" + "=" * 80)
        print("CARGANDO MAPEO DE PRODUCTOS (SKU <-> codigo_alt)")
        print("=" * 80)

        productos = self.load_productos_json()
        if not productos:
            logger.error("[ERROR] No se pudieron cargar los productos")
            return

        connection = self.connect_to_database()
        if not connection:
            return

        try:
            cursor = connection.cursor()

            # Limpiar mappings previos de MySQL
            cursor.execute("DELETE FROM staging.map_producto WHERE source_system = %s", ("MySQL",))

            # Preparar batch
            rows = []
            for producto in productos:
                source_code = producto.get("codigo_alt")
                sku_oficial = producto.get("sku")
                nombre = producto.get("nombre", "")

                if not source_code or not sku_oficial:
                    logger.warning(f"[ADVERTENCIA] Producto sin codigo_alt o sku: {producto}")
                    continue

                descripcion = nombre if nombre else "Sin descripcion"
                rows.append(("MySQL", source_code, sku_oficial, descripcion))

            if not rows:
                logger.warning("[ADVERTENCIA] No hay filas validas para insertar")
                return

            cursor.executemany(
                """
INSERT INTO staging.map_producto (source_system, source_code, sku_oficial, descripcion, activo)
VALUES (%s, %s, %s, %s, 1)
""",
                rows,
            )
            connection.commit()

            print(f"\n{'=' * 80}")
            print("[RESULTADO] CARGA COMPLETADA")
            print(f"   Productos insertados: {len(rows)}")
            print(f"{'=' * 80}\n")
            logger.info(f"[OK] Insertados {len(rows)} mappings")

        except Exception as e:
            logger.error(f"[ERROR] Error al cargar mappings: {e}")
            connection.rollback()
        finally:
            connection.close()


def main():
    loader = ProductMappingLoader()

    if len(sys.argv) > 1 and sys.argv[1] == "load":
        loader.load_product_mappings()
    else:
        print("Comandos disponibles:")
        print("  python cargar_mapeo_productos_mysql.py load")


if __name__ == "__main__":
    main()
