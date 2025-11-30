#!/usr/bin/env python3
"""
Script para cargar mapeo de productos (SKU <-> codigo_alt) desde JSON
Lee los productos de shared\generated\mysql\json\productos.json
e inserta los mappings en staging_map_producto del DWH
"""

import json
import pymssql
import os
import sys
import logging
from pathlib import Path
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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
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

        # Rutas de archivos
        # En Docker: /app/shared/, en local: ../../../shared/
        docker_path = Path("/app/shared/generated/mysql/json/productos.json")
        local_path = Path(__file__).parent.parent.parent / "shared" / "generated" / "mysql" / "json" / "productos.json"

        self.mysql_productos_json = docker_path if docker_path.exists() else local_path

    def load_productos_json(self):
        """Lee el archivo JSON de productos MySQL"""
        try:
            with open(self.mysql_productos_json, 'r', encoding='utf-8') as f:
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
            logger.debug(f"Conectando a SQL: {self.server}/{self.database}")

            # Parsear servidor y puerto
            server_parts = self.server.replace(",", ":").split(":")
            server_host = server_parts[0]
            server_port = int(server_parts[1]) if len(server_parts) > 1 else 1433

            # En Docker, localhost -> sqlserver-dw y puerto interno es 1433
            if server_host == "localhost":
                server_host = "sqlserver-dw"
                server_port = 1433

            logger.debug(f"Intentando conectar a {server_host}:{server_port}/{self.database}")

            connection = pymssql.connect(
                server=server_host,
                port=server_port,
                user=self.username,
                password=self.password,
                database=self.database,
                timeout=10,
                as_dict=False
            )

            logger.debug("[OK] Conexion a base de datos exitosa")
            return connection
        except pymssql.DatabaseError as e:
            logger.error(f"[ERROR] Error de base de datos: {e}")
            return None
        except Exception as e:
            logger.error(f"[ERROR] Error conectando a BD: {e}")
            return None

    def clear_mysql_mappings(self):
        """Elimina todos los mappings anteriores de MySQL"""
        connection = self.connect_to_database()
        if not connection:
            return False

        try:
            cursor = connection.cursor()
            cursor.execute("DELETE FROM staging_map_producto WHERE source_system = 'MySQL'")
            connection.commit()
            deleted_count = cursor.rowcount
            logger.info(f"[OK] Eliminados {deleted_count} mappings anteriores de MySQL")
            return True
        except Exception as e:
            logger.error(f"[ERROR] Error eliminando datos: {e}")
            connection.rollback()
            return False
        finally:
            connection.close()

    def insert_product_mapping(self, source_code, sku_oficial, descripcion):
        """Inserta un mapeo de producto en staging_map_producto"""
        connection = self.connect_to_database()
        if not connection:
            return False

        try:
            cursor = connection.cursor()
            cursor.execute("""
                INSERT INTO staging_map_producto (source_system, source_code, sku_oficial, descripcion, activo)
                VALUES (%s, %s, %s, %s, 1)
            """, ('MySQL', source_code, sku_oficial, descripcion))
            connection.commit()
            logger.debug(f"Insertado: {source_code} -> {sku_oficial}")
            return True

        except Exception as e:
            logger.error(f"[ERROR] Error en BD: {e}")
            connection.rollback()
            return False
        finally:
            connection.close()

    def load_product_mappings(self):
        """Carga los mappings de productos desde JSON al DWH"""

        print("\n" + "=" * 80)
        print("CARGANDO MAPEO DE PRODUCTOS (SKU <-> codigo_alt)")
        print("=" * 80)

        # Eliminar mappings anteriores de MySQL
        if not self.clear_mysql_mappings():
            logger.error("[ERROR] No se pudieron limpiar los datos anteriores")
            return

        # Cargar productos desde JSON
        productos = self.load_productos_json()
        if not productos:
            logger.error("[ERROR] No se pudieron cargar los productos")
            return

        # Procesar cada producto
        total_insertados = 0
        total_errores = 0

        for producto in productos:
            source_code = producto.get('codigo_alt')
            sku_oficial = producto.get('sku')
            nombre = producto.get('nombre', '')

            if not source_code or not sku_oficial:
                logger.warning(f"[ADVERTENCIA] Producto sin codigo_alt o sku: {producto}")
                total_errores += 1
                continue

            # Crear descripcion combinada
            descripcion = f"{nombre}" if nombre else "Sin descripcion"

            # Insertar
            if self.insert_product_mapping(source_code, sku_oficial, descripcion):
                total_insertados += 1
            else:
                total_errores += 1

        print(f"\n{'=' * 80}")
        print("[RESULTADO] CARGA COMPLETADA")
        print(f"   Productos insertados: {total_insertados}")
        print(f"   Errores: {total_errores}")
        print(f"   Total procesados: {total_insertados + total_errores}")
        print(f"{'=' * 80}\n")

        logger.info("Poblacion de mappings completada")


def main():
    loader = ProductMappingLoader()

    if len(sys.argv) > 1:
        command = sys.argv[1]

        if command == "load":
            loader.load_product_mappings()
        else:
            print("Comandos disponibles:")
            print("  python cargar_mapeo_productos_mysql.py load")
    else:
        loader.load_product_mappings()


if __name__ == "__main__":
    main()
