from db_utils import get_connection
import logging
from datetime import datetime, timedelta

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def transform_staging_to_dwh():
    """Transform Layer: staging → dwh"""
    
    logger.info("="*60)
    logger.info("TRANSFORM LAYER: staging → dwh")
    logger.info("="*60)
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            
            # VALIDACIÓN: Verificar que hay datos en staging
            logger.info("\n🔍 Validando datos en staging...")
            cur.execute("""
                SELECT 
                    (SELECT COUNT(*) FROM staging.mssql_customers) +
                    (SELECT COUNT(*) FROM staging.mysql_customers) +
                    (SELECT COUNT(*) FROM staging.mongo_customers) +
                    (SELECT COUNT(*) FROM staging.supabase_users) as total_customers,
                    (SELECT COUNT(*) FROM staging.mssql_products) +
                    (SELECT COUNT(*) FROM staging.mysql_products) +
                    (SELECT COUNT(*) FROM staging.supabase_products) as total_products,
                    (SELECT COUNT(*) FROM staging.mssql_sales) +
                    (SELECT COUNT(*) FROM staging.mysql_sales) +
                    (SELECT COUNT(*) FROM staging.mongo_order_items) +
                    (SELECT COUNT(*) FROM staging.neo4j_order_items) +
                    (SELECT COUNT(*) FROM staging.supabase_order_items) as total_sales
            """)
            customers_count, products_count, sales_count = cur.fetchone()
            
            logger.info(f"   • Clientes en staging:  {customers_count:>6,}")
            logger.info(f"   • Productos en staging: {products_count:>6,}")
            logger.info(f"   • Ventas en staging:    {sales_count:>6,}")
            
            if customers_count == 0 or products_count == 0:
                logger.error("\n❌ ERROR: No hay datos en staging.")
                logger.error("   Debes ejecutar primero los ETLs de Extract Layer:")
                logger.error("   1. docker exec dwh-scheduler python etl_mssql_src.py")
                logger.error("   2. docker exec dwh-scheduler python etl_mysql.py")
                logger.error("   3. docker exec dwh-scheduler python etl_mongo.py")
                logger.error("   4. docker exec dwh-scheduler python etl_neo4j.py")
                logger.error("   5. docker exec dwh-scheduler python etl_supabase.py")
                return
            
            logger.info("✓ Staging contiene datos, continuando...\n")
            
            # 1. Limpiar dwh.* (excepto DimOrder que se limpia después)
            logger.info("\n🗑️  Limpiando dwh.*...")
            cur.execute("DELETE FROM dwh.FactSales")
            cur.execute("DELETE FROM dwh.DimTime")
            cur.execute("DELETE FROM dwh.DimProduct")
            cur.execute("DELETE FROM dwh.DimCustomer")
            cur.execute("DELETE FROM dwh.DimChannel")
            conn.commit()
            logger.info("✓ Limpiado")
            
            # 2. DimCustomer (consolidar de todas las fuentes, dedup por email)
            logger.info("\n📊 Transformando staging → dwh.DimCustomer...")
            cur.execute("""
                INSERT INTO dwh.DimCustomer (name, email, gender, country, created_at)
                SELECT 
                    name,
                    email,
                    CASE 
                        WHEN UPPER(LEFT(gender, 1)) = 'M' THEN 'M'
                        WHEN UPPER(LEFT(gender, 1)) = 'F' THEN 'F'
                        ELSE 'O'
                    END as gender,
                    country,
                    created_at_src
                FROM (
                    SELECT name, email, gender, country, created_at_src,
                           ROW_NUMBER() OVER (PARTITION BY email ORDER BY staging_id) as rn
                    FROM staging.mssql_customers
                    UNION ALL
                    SELECT nombre as name, correo as email, genero as gender, pais as country, created_at_src,
                           ROW_NUMBER() OVER (PARTITION BY correo ORDER BY staging_id) as rn
                    FROM staging.mysql_customers
                    UNION ALL
                    SELECT name, email, genero as gender, NULL as country, NULL as created_at_src,
                           ROW_NUMBER() OVER (PARTITION BY email ORDER BY staging_id) as rn
                    FROM staging.mongo_customers
                    UNION ALL
                    SELECT name, email, gender, country, created_at_src,
                           ROW_NUMBER() OVER (PARTITION BY email ORDER BY staging_id) as rn
                    FROM staging.supabase_users
                ) unified
                WHERE rn = 1 AND email IS NOT NULL
            """)
            count = cur.rowcount
            conn.commit()
            logger.info(f"✓ {count:,} clientes únicos")
            
            # 3. DimProduct (consolidar productos de todas las fuentes)
            logger.info("\n📦 Transformando staging → dwh.DimProduct...")
            cur.execute("""
                INSERT INTO dwh.DimProduct (name, code, categoryId)
                SELECT DISTINCT
                    name,
                    code,
                    NULL as categoryId
                FROM (
                    SELECT name, code FROM staging.mssql_products
                    UNION ALL
                    SELECT nombre as name, COALESCE(sku, codigo_alt) as code FROM staging.mysql_products
                    UNION ALL
                    SELECT name, source_key as code FROM staging.supabase_products
                    UNION ALL
                    SELECT 
                        COALESCE(product_desc, 'MongoDB Product') as name,
                        product_key as code
                    FROM staging.mongo_order_items
                    WHERE product_key IS NOT NULL
                    UNION ALL
                    SELECT 
                        JSON_VALUE(props_json, '$.nombre') as name,
                        node_key as code
                    FROM staging.neo4j_nodes
                    WHERE node_label = 'Producto' AND JSON_VALUE(props_json, '$.nombre') IS NOT NULL
                ) products
                WHERE name IS NOT NULL AND code IS NOT NULL
            """)
            count = cur.rowcount
            conn.commit()
            logger.info(f"✓ {count:,} productos")
            
            # 4. DimTime (generar para 2024-2025)
            logger.info("\n📅 Generando dwh.DimTime...")
            start_date = datetime(2024, 1, 1)
            end_date = datetime(2025, 12, 31)
            dates = []
            current = start_date
            while current <= end_date:
                dates.append((
                    current.year,
                    current.month,
                    current.day,
                    current.strftime('%Y-%m-%d')
                ))
                current += timedelta(days=1)
            
            cur.executemany("""
                INSERT INTO dwh.DimTime (year, month, day, date)
                VALUES (%s, %s, %s, %s)
            """, dates)
            count = cur.rowcount
            conn.commit()
            logger.info(f"✓ {count:,} fechas")
            
            # 5. DimChannel - con IDs fijos 1-5
            logger.info("\n📡 Poblando dwh.DimChannel...")
            cur.execute("SET IDENTITY_INSERT dwh.DimChannel ON")
            cur.execute("""
                INSERT INTO dwh.DimChannel (id, name)
                VALUES (1, 'WEB'), (2, 'TIENDA'), (3, 'APP'), (4, 'PARTNER'), (5, 'TELEFONO')
            """)
            cur.execute("SET IDENTITY_INSERT dwh.DimChannel OFF")
            conn.commit()
            logger.info("✓ 5 canales (IDs 1-5)")
            
            # 6. DimOrder - limpiar y crear un registro dummy con ID=1
            logger.info("\n📝 dwh.DimOrder...")
            cur.execute("DELETE FROM dwh.DimOrder")
            cur.execute("SET IDENTITY_INSERT dwh.DimOrder ON")
            cur.execute("""
                INSERT INTO dwh.DimOrder (id, totalOrderUSD)
                VALUES (1, 0.0)
            """)
            cur.execute("SET IDENTITY_INSERT dwh.DimOrder OFF")
            conn.commit()
            logger.info("✓ 1 orden (dummy con ID=1)")
            
            # 7. FactSales (consolidar ventas de todas las fuentes)
            logger.info("\n💰 Transformando staging → dwh.FactSales...")
            logger.info("   (Esto puede tardar 1-2 minutos...)")
            
            # 7.1 MSSQL sales
            logger.info("   • Cargando MSSQL sales...")
            cur.execute("""
                INSERT INTO dwh.FactSales (
                    productId, timeId, customerId, channelId, orderId,
                    productCant, productUnitPriceUSD, lineTotalUSD, 
                    discountPercentage, exchangeRateId, created_at
                )
                SELECT 
                    p.id as productId,
                    t.id as timeId,
                    c.id as customerId,
                    CASE 
                        WHEN s.channel = 'TIENDA' THEN 2
                        WHEN s.channel = 'WEB' THEN 1
                        WHEN s.channel = 'APP' THEN 3
                        ELSE 1
                    END as channelId,
                    1 as orderId,
                    s.quantity,
                    s.unit_price,
                    s.quantity * s.unit_price as lineTotalUSD,
                    0.0,
                    NULL,
                    GETDATE()
                FROM staging.mssql_sales s
                INNER JOIN staging.mssql_products sp ON sp.source_key = s.product_key AND sp.source_system = 'MSSQL_SRC'
                INNER JOIN staging.mssql_customers sc ON sc.source_key = s.customer_key AND sc.source_system = 'MSSQL_SRC'
                INNER JOIN dwh.DimProduct p ON p.code = sp.code
                INNER JOIN dwh.DimCustomer c ON c.email = sc.email
                INNER JOIN dwh.DimTime t ON t.date = s.order_date
                WHERE s.quantity > 0 AND s.unit_price > 0
            """)
            count_mssql = cur.rowcount
            conn.commit()
            logger.info(f"   ✓ {count_mssql:,} ventas de MSSQL")
            
            # 7.2 MySQL sales
            logger.info("   • Cargando MySQL sales...")
            cur.execute("""
                INSERT INTO dwh.FactSales (
                    productId, timeId, customerId, channelId, orderId,
                    productCant, productUnitPriceUSD, lineTotalUSD, 
                    discountPercentage, exchangeRateId, created_at
                )
                SELECT 
                    p.id,
                    t.id,
                    c.id,
                    CASE 
                        WHEN s.channel = 'WEB' THEN 1
                        WHEN s.channel = 'TIENDA' THEN 2
                        WHEN s.channel = 'APP' THEN 3
                        ELSE 1
                    END,
                    1,
                    s.quantity,
                    s.unit_price,
                    s.quantity * s.unit_price,
                    0.0,
                    NULL,
                    GETDATE()
                FROM staging.mysql_sales s
                INNER JOIN staging.mysql_customers mc ON mc.source_key = s.customer_key AND mc.source_system = 'MySQL'
                INNER JOIN dwh.DimProduct p ON p.code = s.sku
                INNER JOIN dwh.DimCustomer c ON c.email = mc.correo
                INNER JOIN dwh.DimTime t ON t.date = s.order_date
                WHERE s.quantity > 0 AND s.unit_price > 0
            """)
            count_mysql = cur.rowcount
            conn.commit()
            logger.info(f"   ✓ {count_mysql:,} ventas de MySQL")
            
            # 7.3 MongoDB order_items
            logger.info("   • Cargando MongoDB order_items...")
            cur.execute("""
                INSERT INTO dwh.FactSales (
                    productId, timeId, customerId, channelId, orderId,
                    productCant, productUnitPriceUSD, lineTotalUSD, 
                    discountPercentage, exchangeRateId, created_at
                )
                SELECT 
                    p.id,
                    t.id,
                    c.id,
                    1 as channelId,
                    1,
                    oi.quantity,
                    oi.unit_price,
                    oi.quantity * oi.unit_price,
                    0.0,
                    NULL,
                    GETDATE()
                FROM staging.mongo_order_items oi
                INNER JOIN staging.mongo_orders mo ON mo.source_key = oi.order_key AND mo.source_system = 'MongoDB'
                INNER JOIN staging.mongo_customers mc ON mc.source_key = mo.customer_key AND mc.source_system = 'MongoDB'
                INNER JOIN dwh.DimCustomer c ON c.email = mc.email
                INNER JOIN dwh.DimProduct p ON p.code = oi.product_key
                INNER JOIN dwh.DimTime t ON t.date = oi.order_date
                WHERE oi.quantity > 0 AND oi.unit_price > 0 AND oi.product_key IS NOT NULL
            """)
            count_mongo = cur.rowcount
            conn.commit()
            logger.info(f"   ✓ {count_mongo:,} ventas de MongoDB")
            
            # 7.4 Neo4j order_items
            logger.info("   • Cargando Neo4j order_items...")
            cur.execute("""
                INSERT INTO dwh.FactSales (
                    productId, timeId, customerId, channelId, orderId,
                    productCant, productUnitPriceUSD, lineTotalUSD, 
                    discountPercentage, exchangeRateId, created_at
                )
                SELECT 
                    p.id,
                    t.id,
                    c.id,
                    1,
                    1,
                    oi.quantity,
                    oi.unit_price,
                    oi.quantity * oi.unit_price,
                    0.0,
                    NULL,
                    GETDATE()
                FROM staging.neo4j_order_items oi
                INNER JOIN staging.neo4j_nodes nc ON nc.node_key = oi.customer_key AND nc.node_label = 'Cliente'
                INNER JOIN dwh.DimCustomer c ON c.email = JSON_VALUE(nc.props_json, '$.email')
                INNER JOIN dwh.DimProduct p ON p.code = oi.product_key
                INNER JOIN dwh.DimTime t ON t.date = oi.order_date
                WHERE oi.quantity > 0 AND oi.unit_price > 0 AND oi.product_key IS NOT NULL
            """)
            count_neo4j = cur.rowcount
            conn.commit()
            logger.info(f"   ✓ {count_neo4j:,} ventas de Neo4j")
            
            # 7.5 Supabase order_items
            logger.info("   • Cargando Supabase order_items...")
            cur.execute("""
                INSERT INTO dwh.FactSales (
                    productId, timeId, customerId, channelId, orderId,
                    productCant, productUnitPriceUSD, lineTotalUSD, 
                    discountPercentage, exchangeRateId, created_at
                )
                SELECT 
                    p.id,
                    t.id,
                    c.id,
                    1,
                    1,
                    oi.quantity,
                    oi.unit_price,
                    oi.subtotal,
                    0.0,
                    NULL,
                    GETDATE()
                FROM staging.supabase_order_items oi
                INNER JOIN staging.supabase_orders so ON so.source_key = oi.order_key AND so.source_system = 'SUPABASE'
                INNER JOIN staging.supabase_users su ON su.source_key = so.user_key AND su.source_system = 'SUPABASE'
                INNER JOIN dwh.DimCustomer c ON c.email = su.email
                INNER JOIN staging.supabase_products sp ON sp.source_key = oi.product_key AND sp.source_system = 'SUPABASE'
                INNER JOIN dwh.DimProduct p ON p.code = sp.source_key
                INNER JOIN dwh.DimTime t ON t.date = CAST(so.created_at_src AS DATE)
                WHERE oi.quantity > 0 AND oi.unit_price > 0
            """)
            count_supabase = cur.rowcount
            conn.commit()
            logger.info(f"   ✓ {count_supabase:,} ventas de Supabase")
            
            total_ventas = count_mssql + count_mysql + count_mongo + count_neo4j + count_supabase
            logger.info(f"   📊 Total: {total_ventas:,} transacciones cargadas")
            
            # Verificación
            logger.info("\n" + "="*60)
            logger.info("VERIFICACIÓN")
            logger.info("="*60)
            
            cur.execute("SELECT COUNT(*) FROM dwh.DimCustomer")
            logger.info(f"   • Clientes:  {cur.fetchone()[0]:>8,}")
            
            cur.execute("SELECT COUNT(*) FROM dwh.DimProduct")
            logger.info(f"   • Productos: {cur.fetchone()[0]:>8,}")
            
            cur.execute("SELECT COUNT(*), SUM(lineTotalUSD) FROM dwh.FactSales")
            ventas, monto = cur.fetchone()
            logger.info(f"   • Ventas:    {ventas:>8,}")
            if monto:
                logger.info(f"   • Monto:     ${monto:>13,.2f} USD")
            else:
                logger.info(f"   • Monto:     $          0.00 USD")
            
            logger.info("\n✅ TRANSFORM COMPLETADO")

if __name__ == "__main__":
    inicio = datetime.now()
    try:
        transform_staging_to_dwh()
        duracion = (datetime.now() - inicio).total_seconds()
        logger.info(f"\n⏱️  {duracion:.1f}s")
    except Exception as e:
        logger.error(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        raise
