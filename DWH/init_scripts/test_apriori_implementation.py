"""
Script de prueba para validar la implementación de Apriori
Verifica que todas las tablas y SPs existan y funcionen correctamente
"""
import logging
import os
import sys

import pymssql
from dotenv import load_dotenv

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()


def connect_to_database():
    """Conectar a SQL Server DWH"""
    try:
        server = os.getenv("serverenv", "localhost")
        database = os.getenv("databaseenv", "MSSQL_DW")
        username = os.getenv("usernameenv")
        password = os.getenv("passwordenv")
        
        server_parts = server.replace(",", ":").split(":")
        server_host = server_parts[0]
        server_port = int(server_parts[1]) if len(server_parts) > 1 else 1433
        
        if server_host == "localhost":
            server_host = "sqlserver-dw"
            server_port = 1433
        
        connection = pymssql.connect(
            server=server_host,
            port=server_port,
            user=username,
            password=password,
            database=database,
            timeout=10
        )
        
        return connection
    
    except Exception as e:
        logger.error(f"Error conectando a base de datos: {e}")
        return None


def test_table_exists():
    """Verificar que la tabla ProductAssociationRules existe"""
    logger.info("\n[TEST 1] Verificando existencia de tabla dwh.ProductAssociationRules...")
    
    connection = connect_to_database()
    if not connection:
        logger.error("✗ No se pudo conectar a la base de datos")
        return False
    
    try:
        cursor = connection.cursor()
        cursor.execute("""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = 'dwh' 
              AND TABLE_NAME = 'ProductAssociationRules'
        """)
        
        count = cursor.fetchone()[0]
        
        if count == 1:
            logger.info("✓ Tabla dwh.ProductAssociationRules existe")
            return True
        else:
            logger.error("✗ Tabla dwh.ProductAssociationRules NO existe")
            return False
    
    except Exception as e:
        logger.error(f"✗ Error verificando tabla: {e}")
        return False
    finally:
        connection.close()


def test_stored_procedures():
    """Verificar que los stored procedures existen"""
    logger.info("\n[TEST 2] Verificando stored procedures...")
    
    connection = connect_to_database()
    if not connection:
        return False
    
    procedures = [
        'sp_get_product_recommendations',
        'sp_get_cart_recommendations',
        'sp_get_top_association_rules',
        'sp_get_apriori_stats'
    ]
    
    all_exist = True
    
    try:
        cursor = connection.cursor()
        
        for proc in procedures:
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.ROUTINES 
                WHERE ROUTINE_TYPE = 'PROCEDURE' 
                  AND ROUTINE_NAME = '{proc}'
            """)
            
            count = cursor.fetchone()[0]
            
            if count == 1:
                logger.info(f"  ✓ {proc} existe")
            else:
                logger.error(f"  ✗ {proc} NO existe")
                all_exist = False
        
        return all_exist
    
    except Exception as e:
        logger.error(f"✗ Error verificando stored procedures: {e}")
        return False
    finally:
        connection.close()


def test_fact_sales_data():
    """Verificar que hay datos en FactSales para analizar"""
    logger.info("\n[TEST 3] Verificando datos en FactSales...")
    
    connection = connect_to_database()
    if not connection:
        return False
    
    try:
        cursor = connection.cursor()
        
        # Contar transacciones
        cursor.execute("""
            SELECT COUNT(DISTINCT orderId) AS total_orders
            FROM dwh.FactSales
        """)
        total_orders = cursor.fetchone()[0]
        
        # Contar productos únicos
        cursor.execute("""
            SELECT COUNT(DISTINCT productId) AS total_products
            FROM dwh.FactSales
        """)
        total_products = cursor.fetchone()[0]
        
        logger.info(f"  Total de órdenes: {total_orders}")
        logger.info(f"  Total de productos únicos: {total_products}")
        
        if total_orders >= 100 and total_products >= 10:
            logger.info("✓ Hay suficientes datos para análisis Apriori")
            return True
        else:
            logger.warning(f"⚠ Pocos datos: se recomienda tener al menos 100 órdenes y 10 productos")
            return total_orders > 0 and total_products > 0
    
    except Exception as e:
        logger.error(f"✗ Error verificando datos: {e}")
        return False
    finally:
        connection.close()


def test_apriori_stats():
    """Ejecutar sp_get_apriori_stats"""
    logger.info("\n[TEST 4] Ejecutando sp_get_apriori_stats...")
    
    connection = connect_to_database()
    if not connection:
        return False
    
    try:
        cursor = connection.cursor()
        cursor.execute("EXEC dbo.sp_get_apriori_stats")
        
        result = cursor.fetchone()
        
        if result:
            total_rules, active_rules, last_update, avg_support, avg_confidence, avg_lift, max_lift, min_support = result
            
            logger.info(f"  Total de reglas: {total_rules}")
            logger.info(f"  Reglas activas: {active_rules}")
            logger.info(f"  Última actualización: {last_update}")
            
            if active_rules and active_rules > 0:
                logger.info(f"  Support promedio: {avg_support:.4f}")
                logger.info(f"  Confidence promedio: {avg_confidence:.4f}")
                logger.info(f"  Lift promedio: {avg_lift:.4f}")
                logger.info(f"  Lift máximo: {max_lift:.4f}")
                logger.info("✓ sp_get_apriori_stats funciona correctamente")
                return True
            else:
                logger.warning("⚠ No hay reglas activas aún. Ejecuta: python apriori_analysis.py run")
                return True
        else:
            logger.error("✗ sp_get_apriori_stats no devolvió resultados")
            return False
    
    except Exception as e:
        logger.error(f"✗ Error ejecutando sp_get_apriori_stats: {e}")
        return False
    finally:
        connection.close()


def test_sample_recommendation():
    """Probar recomendación para un producto de ejemplo"""
    logger.info("\n[TEST 5] Probando sp_get_product_recommendations...")
    
    connection = connect_to_database()
    if not connection:
        return False
    
    try:
        cursor = connection.cursor()
        
        # Obtener un producto aleatorio
        cursor.execute("SELECT TOP 1 id FROM dwh.DimProduct ORDER BY NEWID()")
        result = cursor.fetchone()
        
        if not result:
            logger.warning("⚠ No hay productos en DimProduct")
            return False
        
        product_id = result[0]
        
        # Obtener recomendaciones
        cursor.execute(f"EXEC dbo.sp_get_product_recommendations @ProductId={product_id}, @TopN=5")
        
        recommendations = cursor.fetchall()
        
        logger.info(f"  Buscando recomendaciones para ProductID={product_id}...")
        
        if recommendations:
            logger.info(f"  ✓ Se encontraron {len(recommendations)} recomendaciones:")
            for rec in recommendations[:3]:  # Mostrar solo las primeras 3
                rule_id, conseq_ids, conseq_names, support, confidence, lift, fecha = rec
                logger.info(f"    → {conseq_names[:50]} (Lift={lift:.2f}, Conf={confidence:.2f})")
            return True
        else:
            logger.warning("  ⚠ No se encontraron recomendaciones para este producto")
            logger.info("  Esto es normal si no se ha ejecutado el análisis Apriori aún")
            return True
    
    except Exception as e:
        logger.error(f"✗ Error ejecutando recomendaciones: {e}")
        return False
    finally:
        connection.close()


def main():
    """Ejecutar todos los tests"""
    logger.info("=" * 80)
    logger.info("VALIDACIÓN DE IMPLEMENTACIÓN DE APRIORI")
    logger.info("=" * 80)
    
    tests = [
        ("Tabla ProductAssociationRules", test_table_exists),
        ("Stored Procedures", test_stored_procedures),
        ("Datos en FactSales", test_fact_sales_data),
        ("sp_get_apriori_stats", test_apriori_stats),
        ("Recomendaciones de ejemplo", test_sample_recommendation)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            logger.error(f"✗ Error en test '{test_name}': {e}")
            results.append((test_name, False))
    
    # Resumen
    logger.info("\n" + "=" * 80)
    logger.info("RESUMEN DE TESTS")
    logger.info("=" * 80)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✓ PASS" if result else "✗ FAIL"
        logger.info(f"  {status} - {test_name}")
    
    logger.info("=" * 80)
    logger.info(f"RESULTADO: {passed}/{total} tests pasaron")
    
    if passed == total:
        logger.info("✓ TODOS LOS TESTS PASARON")
        logger.info("\nPara ejecutar el análisis Apriori:")
        logger.info("  docker exec dwh-scheduler python apriori_analysis.py run")
    else:
        logger.warning(f"⚠ {total - passed} tests fallaron")
        logger.info("\nVerifica la configuración del DWH y que los scripts SQL se hayan ejecutado")
    
    logger.info("=" * 80)
    
    return passed == total


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
