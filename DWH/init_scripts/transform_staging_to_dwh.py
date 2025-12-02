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
                    (SELECT COUNT(*) FROM staging.mongo_products) +
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
            
            # Solo validar que haya ALGO de datos (al menos clientes o ventas)
            if customers_count == 0 and sales_count == 0:
                logger.error("\n❌ ERROR: No hay datos en staging.")
                logger.error("   Debes ejecutar al menos UN ETL de Extract Layer:")
                logger.error("   - docker exec dwh-scheduler python etl_mssql_src.py")
                logger.error("   - docker exec dwh-scheduler python etl_mysql.py")
                logger.error("   - docker exec dwh-scheduler python etl_mongo.py")
                logger.error("   - docker exec dwh-scheduler python etl_neo4j.py")
                logger.error("   - docker exec dwh-scheduler python etl_supabase.py")
                return
            
            logger.info("✓ Staging contiene datos, continuando...")
            logger.info("  (Las fuentes sin datos serán omitidas automáticamente)\n")
            
            # 1. Limpiar dwh.* (en orden correcto por FKs)
            logger.info("\n🗑️  Limpiando dwh.*...")
            cur.execute("DELETE FROM dwh.FactSales")
            cur.execute("DELETE FROM dwh.DimOrder")
            cur.execute("DELETE FROM dwh.DimTime")
            cur.execute("DELETE FROM dwh.DimProduct")
            cur.execute("DELETE FROM dwh.DimCategory")
            cur.execute("DELETE FROM dwh.DimCustomer")
            cur.execute("DELETE FROM dwh.DimChannel")
            conn.commit()
            logger.info("✓ Limpiado")
            
            # 2. DimExchangeRate - promover desde staging.tipo_cambio
            logger.info("\n💱 Promoviendo staging.tipo_cambio → dwh.DimExchangeRate...")
            cur.execute("SELECT COUNT(*) FROM staging.tipo_cambio")
            tipo_cambio_count = cur.fetchone()[0]
            logger.info(f"   • Tipos de cambio en staging: {tipo_cambio_count:,}")
            
            if tipo_cambio_count > 0:
                cur.execute("EXEC dbo.sp_promote_exchange_rate")
                conn.commit()
                cur.execute("SELECT COUNT(*) FROM dwh.DimExchangeRate")
                exchange_rate_count = cur.fetchone()[0]
                logger.info(f"✓ {exchange_rate_count:,} tipos de cambio")
            else:
                logger.warning("⚠️  No hay datos en staging.tipo_cambio, omitiendo...")
            
            # 3. DimCustomer (consolidar de todas las fuentes, dedup por email)
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
                           ROW_NUMBER() OVER (PARTITION BY email ORDER BY created_at_src DESC, name) as rn
                    FROM (
                        SELECT name, email, gender, country, created_at_src
                        FROM staging.mssql_customers
                        UNION ALL
                        SELECT nombre as name, correo as email, genero as gender, pais as country, created_at_src
                        FROM staging.mysql_customers
                        UNION ALL
                        SELECT name, email, genero as gender, NULL as country, NULL as created_at_src
                        FROM staging.mongo_customers
                        UNION ALL
                        SELECT name, email, gender, country, created_at_src
                        FROM staging.supabase_users
                        UNION ALL
                        SELECT 
                            JSON_VALUE(props_json, '$.nombre') as name,
                            JSON_VALUE(props_json, '$.email') as email,
                            JSON_VALUE(props_json, '$.genero') as gender,
                            JSON_VALUE(props_json, '$.pais') as country,
                            NULL as created_at_src
                        FROM staging.neo4j_nodes
                        WHERE node_label = 'Cliente' AND JSON_VALUE(props_json, '$.email') IS NOT NULL
                    ) all_sources
                ) unified
                WHERE rn = 1 AND email IS NOT NULL
            """)
            count = cur.rowcount
            conn.commit()
            logger.info(f"✓ {count:,} clientes únicos")
            
            # 4. DimCategory (consolidar categorías de todos los productos)
            logger.info("\n🏷️  Transformando staging → dwh.DimCategory...")
            cur.execute("""
                INSERT INTO dwh.DimCategory (name)
                SELECT DISTINCT category
                FROM (
                    SELECT category FROM staging.mssql_products WHERE category IS NOT NULL
                    UNION
                    SELECT categoria FROM staging.mysql_products WHERE categoria IS NOT NULL
                    UNION
                    SELECT categoria FROM staging.mongo_products WHERE categoria IS NOT NULL
                    UNION
                    SELECT category FROM staging.supabase_products WHERE category IS NOT NULL
                    UNION
                    SELECT JSON_VALUE(props_json, '$.nombre') 
                    FROM staging.neo4j_nodes 
                    WHERE node_label = 'Categoria' 
                      AND JSON_VALUE(props_json, '$.nombre') IS NOT NULL
                ) categories
                WHERE category IS NOT NULL AND LEN(LTRIM(RTRIM(category))) > 0
                ORDER BY category
            """)
            count = cur.rowcount
            conn.commit()
            logger.info(f"✓ {count:,} categorías")
            
            # 5. DimProduct (consolidar productos de todas las fuentes CON categoryId)
            # NUEVA LÓGICA: Deduplicar por nombre y generar SKU unificado
            logger.info("\n📦 Transformando staging → dwh.DimProduct (con deduplicación)...")
            
            # Paso 1: Insertar productos únicos con SKU unificado
            cur.execute("""
                WITH AllProducts AS (
                    -- MSSQL products
                    SELECT name, code, category, 'MSSQL' as source_system FROM staging.mssql_products
                    UNION ALL
                    -- MySQL products
                    SELECT nombre as name, COALESCE(sku, codigo_alt) as code, categoria as category, 'MySQL' as source_system
                    FROM staging.mysql_products
                    UNION ALL
                    -- Supabase products
                    SELECT name, source_key as code, category, 'Supabase' as source_system FROM staging.supabase_products
                    UNION ALL
                    -- MongoDB products
                    SELECT 
                        nombre as name,
                        codigo_mongo as code,
                        categoria as category,
                        'MongoDB' as source_system
                    FROM staging.mongo_products
                    WHERE nombre IS NOT NULL AND codigo_mongo IS NOT NULL
                    UNION ALL
                    -- Neo4j products
                    SELECT 
                        JSON_VALUE(n.props_json, '$.nombre') as name,
                        n.node_key as code,
                        e.to_key as category,
                        'Neo4j' as source_system
                    FROM staging.neo4j_nodes n
                    LEFT JOIN staging.neo4j_edges e 
                        ON e.edge_type = 'PERTENECE_A' 
                        AND e.from_label = 'Producto' 
                        AND e.from_key = n.node_key
                    WHERE n.node_label = 'Producto' AND JSON_VALUE(n.props_json, '$.nombre') IS NOT NULL
                ),
                UniqueProducts AS (
                    SELECT 
                        name,
                        category,
                        MIN(code) as original_code,
                        ROW_NUMBER() OVER (ORDER BY MIN(name)) as row_num
                    FROM AllProducts
                    WHERE name IS NOT NULL AND code IS NOT NULL
                    GROUP BY name, category
                )
                INSERT INTO dwh.DimProduct (name, code, categoryId)
                SELECT 
                    up.name,
                    'SKU' + RIGHT('00000' + CAST(up.row_num as VARCHAR), 5) as unified_code,
                    c.id as categoryId
                FROM UniqueProducts up
                LEFT JOIN dwh.DimCategory c ON c.name = up.category
            """)
            count = cur.rowcount
            conn.commit()
            logger.info(f"✓ {count:,} productos únicos insertados")
            
            # Paso 2: Poblar staging.map_producto con todos los mapeos
            logger.info("📋 Poblando staging.map_producto con mapeos...")
            cur.execute("""
                WITH AllProducts AS (
                    SELECT name, code, 'MSSQL' as source_system FROM staging.mssql_products
                    UNION ALL
                    SELECT nombre, COALESCE(sku, codigo_alt), 'MySQL' FROM staging.mysql_products
                    UNION ALL
                    SELECT name, source_key, 'Supabase' FROM staging.supabase_products
                    UNION ALL
                    SELECT nombre, codigo_mongo, 'MongoDB' FROM staging.mongo_products
                    WHERE nombre IS NOT NULL AND codigo_mongo IS NOT NULL
                    UNION ALL
                    SELECT 
                        JSON_VALUE(n.props_json, '$.nombre'),
                        n.node_key,
                        'Neo4j'
                    FROM staging.neo4j_nodes n
                    WHERE n.node_label = 'Producto' AND JSON_VALUE(n.props_json, '$.nombre') IS NOT NULL
                ),
                UniqueMapping AS (
                    SELECT 
                        ap.source_system,
                        ap.code,
                        ap.name,
                        dp.code as sku_oficial,
                        ROW_NUMBER() OVER (PARTITION BY ap.source_system, ap.code ORDER BY dp.code) as rn
                    FROM AllProducts ap
                    INNER JOIN dwh.DimProduct dp ON dp.name = ap.name
                    WHERE ap.code IS NOT NULL AND ap.name IS NOT NULL
                )
                MERGE INTO staging.map_producto AS target
                USING (
                    SELECT 
                        source_system,
                        code as source_code,
                        sku_oficial,
                        name as descripcion,
                        1 as activo
                    FROM UniqueMapping
                    WHERE rn = 1
                ) AS source
                ON target.source_system = source.source_system 
                   AND target.source_code = source.source_code
                WHEN NOT MATCHED THEN
                    INSERT (source_system, source_code, sku_oficial, descripcion, activo)
                    VALUES (source.source_system, source.source_code, source.sku_oficial, source.descripcion, source.activo);
            """)
            map_count = cur.rowcount
            conn.commit()
            logger.info(f"✓ {map_count:,} mapeos creados en staging.map_producto")
            
            # 6. DimTime (generar para 2024-2025)
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
            
            # 7. DimChannel - con IDs fijos 1-5
            logger.info("\n📡 Poblando dwh.DimChannel...")
            cur.execute("SET IDENTITY_INSERT dwh.DimChannel ON")
            cur.execute("""
                INSERT INTO dwh.DimChannel (id, name)
                VALUES (1, 'WEB'), (2, 'TIENDA'), (3, 'APP'), (4, 'PARTNER'), (5, 'TELEFONO')
            """)
            cur.execute("SET IDENTITY_INSERT dwh.DimChannel OFF")
            conn.commit()
            logger.info("✓ 5 canales (IDs 1-5)")
            
            # 8. DimOrder - crear órdenes reales desde staging
            logger.info("\n📝 Transformando staging → dwh.DimOrder...")
            
            # Crear órdenes desde cada fuente
            cur.execute("""
                INSERT INTO dwh.DimOrder (totalOrderUSD)
                SELECT DISTINCT
                    COALESCE(total_amount, 0.0) as totalOrderUSD
                FROM (
                    -- MSSQL orders (agrupar por order_key)
                    SELECT order_key, SUM(quantity * unit_price) as total_amount
                    FROM staging.mssql_sales
                    GROUP BY order_key
                    
                    UNION ALL
                    
                    -- MySQL orders (agrupar por order_key)
                    SELECT order_key, SUM(quantity * unit_price) as total_amount
                    FROM staging.mysql_sales
                    GROUP BY order_key
                    
                    UNION ALL
                    
                    -- MongoDB orders
                    SELECT source_key as order_key, total_amount
                    FROM staging.mongo_orders
                    
                    UNION ALL
                    
                    -- Supabase orders
                    SELECT source_key as order_key, total_amount
                    FROM staging.supabase_orders
                    
                    UNION ALL
                    
                    -- Neo4j orders (agrupar por order_key)
                    SELECT order_key, SUM(quantity * unit_price) as total_amount
                    FROM staging.neo4j_order_items
                    GROUP BY order_key
                ) orders
                WHERE total_amount > 0
            """)
            count = cur.rowcount
            conn.commit()
            logger.info(f"✓ {count:,} órdenes creadas")
            
            # 9. FactSales (consolidar ventas de todas las fuentes)
            logger.info("\n💰 Transformando staging → dwh.FactSales...")
            logger.info("   (Esto puede tardar 1-2 minutos...)")
            
            # 9.1 MSSQL sales
            logger.info("   • Cargando MSSQL sales...")
            cur.execute("""
                WITH mssql_orders AS (
                    SELECT order_key, SUM(quantity * unit_price) as total
                    FROM staging.mssql_sales
                    GROUP BY order_key
                )
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
                    COALESCE(o.id, 1) as orderId,
                    s.quantity,
                    -- Si viene en CRC, convertir a USD dividiendo por el rate
                    CASE 
                        WHEN s.currency = 'CRC' AND ex.rate IS NOT NULL THEN s.unit_price / ex.rate
                        ELSE s.unit_price
                    END as productUnitPriceUSD,
                    -- Calcular total en USD
                    CASE 
                        WHEN s.currency = 'CRC' AND ex.rate IS NOT NULL THEN (s.quantity * s.unit_price) / ex.rate
                        ELSE s.quantity * s.unit_price
                    END as lineTotalUSD,
                    0.0 as discountPercentage,
                    ex.id as exchangeRateId,
                    GETDATE() as created_at
                FROM staging.mssql_sales s
                INNER JOIN staging.mssql_products sp ON sp.source_key = s.product_key AND sp.source_system = 'MSSQL_SRC'
                INNER JOIN staging.mssql_customers sc ON sc.source_key = s.customer_key AND sc.source_system = 'MSSQL_SRC'
                INNER JOIN staging.map_producto mp ON mp.source_code = sp.code AND mp.source_system = 'MSSQL'
                INNER JOIN dwh.DimProduct p ON p.code = mp.sku_oficial
                INNER JOIN dwh.DimCustomer c ON c.email = sc.email
                INNER JOIN dwh.DimTime t ON t.date = s.order_date
                LEFT JOIN dwh.DimExchangeRate ex ON ex.date = s.order_date AND ex.fromCurrency = 'CRC' AND ex.toCurrency = 'USD'
                LEFT JOIN mssql_orders mo ON mo.order_key = s.order_key
                LEFT JOIN dwh.DimOrder o ON ABS(o.totalOrderUSD - mo.total) < 0.01
                WHERE s.quantity > 0 AND s.unit_price > 0
            """)
            count_mssql = cur.rowcount
            conn.commit()
            logger.info(f"   ✓ {count_mssql:,} ventas de MSSQL")
            
            # 7.2 MySQL sales
            logger.info("   • Cargando MySQL sales...")
            cur.execute("""
                WITH mysql_orders AS (
                    SELECT order_key, SUM(quantity * unit_price) as total
                    FROM staging.mysql_sales
                    GROUP BY order_key
                )
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
                        WHEN s.channel = 'WEB' THEN 1
                        WHEN s.channel = 'TIENDA' THEN 2
                        WHEN s.channel = 'APP' THEN 3
                        ELSE 1
                    END as channelId,
                    COALESCE(o.id, 1) as orderId,
                    s.quantity,
                    -- Si viene en CRC, convertir a USD dividiendo por el rate
                    CASE 
                        WHEN s.currency = 'CRC' AND ex.rate IS NOT NULL THEN s.unit_price / ex.rate
                        ELSE s.unit_price
                    END as productUnitPriceUSD,
                    -- Calcular total en USD
                    CASE 
                        WHEN s.currency = 'CRC' AND ex.rate IS NOT NULL THEN (s.quantity * s.unit_price) / ex.rate
                        ELSE s.quantity * s.unit_price
                    END as lineTotalUSD,
                    0.0 as discountPercentage,
                    ex.id as exchangeRateId,
                    GETDATE() as created_at
                FROM staging.mysql_sales s
                INNER JOIN staging.mysql_customers mc ON mc.source_key = s.customer_key AND mc.source_system = 'MySQL'
                INNER JOIN staging.map_producto mp ON mp.source_code = s.sku AND mp.source_system = 'MySQL'
                INNER JOIN dwh.DimProduct p ON p.code = mp.sku_oficial
                INNER JOIN dwh.DimCustomer c ON c.email = mc.correo
                INNER JOIN dwh.DimTime t ON t.date = s.order_date
                LEFT JOIN dwh.DimExchangeRate ex ON ex.date = s.order_date AND ex.fromCurrency = 'CRC' AND ex.toCurrency = 'USD'
                LEFT JOIN mysql_orders mo ON mo.order_key = s.order_key
                LEFT JOIN dwh.DimOrder o ON ABS(o.totalOrderUSD - mo.total) < 0.01
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
                    p.id as productId,
                    t.id as timeId,
                    c.id as customerId,
                    1 as channelId,
                    COALESCE(o.id, 1) as orderId,
                    oi.quantity,
                    -- Si viene en CRC, convertir a USD dividiendo por el rate
                    CASE 
                        WHEN oi.currency = 'CRC' AND ex.rate IS NOT NULL THEN oi.unit_price / ex.rate
                        ELSE oi.unit_price
                    END as productUnitPriceUSD,
                    -- Calcular total en USD
                    CASE 
                        WHEN oi.currency = 'CRC' AND ex.rate IS NOT NULL THEN (oi.quantity * oi.unit_price) / ex.rate
                        ELSE oi.quantity * oi.unit_price
                    END as lineTotalUSD,
                    0.0 as discountPercentage,
                    ex.id as exchangeRateId,
                    GETDATE() as created_at
                FROM staging.mongo_order_items oi
                INNER JOIN staging.mongo_orders mo ON mo.source_key = oi.order_key AND mo.source_system = 'MongoDB'
                INNER JOIN staging.mongo_customers mc ON mc.source_key = mo.customer_key AND mc.source_system = 'MongoDB'
                INNER JOIN dwh.DimCustomer c ON c.email = mc.email
                INNER JOIN dwh.DimTime t ON t.date = oi.order_date
                LEFT JOIN dwh.DimExchangeRate ex ON ex.date = oi.order_date AND ex.fromCurrency = 'CRC' AND ex.toCurrency = 'USD'
                -- Mapear producto desde staging.mongo_products usando product_key
                INNER JOIN staging.mongo_products mp ON mp.source_key = oi.product_key AND mp.source_system = 'MongoDB'
                INNER JOIN staging.map_producto mprod ON mprod.source_code = mp.codigo_mongo AND mprod.source_system = 'MongoDB'
                INNER JOIN dwh.DimProduct p ON p.code = mprod.sku_oficial
                LEFT JOIN dwh.DimOrder o ON ABS(o.totalOrderUSD - mo.total_amount) < 0.01
                WHERE oi.quantity > 0 
                  AND oi.unit_price > 0 
                  AND oi.product_key IS NOT NULL
            """)
            count_mongo = cur.rowcount
            conn.commit()
            logger.info(f"   ✓ {count_mongo:,} ventas de MongoDB")
            
            # 7.4 Neo4j order_items
            logger.info("   • Cargando Neo4j order_items...")
            cur.execute("""
                WITH neo4j_orders AS (
                    SELECT order_key, SUM(quantity * unit_price) as total
                    FROM staging.neo4j_order_items
                    GROUP BY order_key
                )
                INSERT INTO dwh.FactSales (
                    productId, timeId, customerId, channelId, orderId,
                    productCant, productUnitPriceUSD, lineTotalUSD, 
                    discountPercentage, exchangeRateId, created_at
                )
                SELECT 
                    p.id as productId,
                    t.id as timeId,
                    c.id as customerId,
                    1 as channelId,
                    COALESCE(o.id, 1) as orderId,
                    oi.quantity,
                    -- Si viene en CRC, convertir a USD dividiendo por el rate
                    CASE 
                        WHEN oi.currency = 'CRC' AND ex.rate IS NOT NULL THEN oi.unit_price / ex.rate
                        ELSE oi.unit_price
                    END as productUnitPriceUSD,
                    -- Calcular total en USD
                    CASE 
                        WHEN oi.currency = 'CRC' AND ex.rate IS NOT NULL THEN (oi.quantity * oi.unit_price) / ex.rate
                        ELSE oi.quantity * oi.unit_price
                    END as lineTotalUSD,
                    0.0 as discountPercentage,
                    ex.id as exchangeRateId,
                    GETDATE() as created_at
                FROM staging.neo4j_order_items oi
                INNER JOIN staging.neo4j_nodes nc ON nc.node_key = oi.customer_key AND nc.node_label = 'Cliente'
                INNER JOIN dwh.DimCustomer c ON c.email = JSON_VALUE(nc.props_json, '$.email')
                INNER JOIN staging.map_producto mp ON mp.source_code = oi.product_key AND mp.source_system = 'Neo4j'
                INNER JOIN dwh.DimProduct p ON p.code = mp.sku_oficial
                INNER JOIN dwh.DimTime t ON t.date = oi.order_date
                LEFT JOIN dwh.DimExchangeRate ex ON ex.date = oi.order_date AND ex.fromCurrency = 'CRC' AND ex.toCurrency = 'USD'
                LEFT JOIN neo4j_orders no ON no.order_key = oi.order_key
                LEFT JOIN dwh.DimOrder o ON ABS(o.totalOrderUSD - no.total) < 0.01
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
                    p.id as productId,
                    t.id as timeId,
                    c.id as customerId,
                    1 as channelId,
                    COALESCE(o.id, 1) as orderId,
                    oi.quantity,
                    oi.unit_price,
                    oi.subtotal as lineTotalUSD,
                    0.0 as discountPercentage,
                    ex.id as exchangeRateId,
                    GETDATE() as created_at
                FROM staging.supabase_order_items oi
                INNER JOIN staging.supabase_orders so ON so.source_key = oi.order_key AND so.source_system = 'SUPABASE'
                INNER JOIN staging.supabase_users su ON su.source_key = so.user_key AND su.source_system = 'SUPABASE'
                INNER JOIN dwh.DimCustomer c ON c.email = su.email
                INNER JOIN staging.supabase_products sp ON sp.source_key = oi.product_key AND sp.source_system = 'SUPABASE'
                INNER JOIN staging.map_producto mp ON mp.source_code = sp.source_key AND mp.source_system = 'Supabase'
                INNER JOIN dwh.DimProduct p ON p.code = mp.sku_oficial
                INNER JOIN dwh.DimTime t ON t.date = CAST(so.created_at_src AS DATE)
                LEFT JOIN dwh.DimExchangeRate ex ON ex.date = CAST(so.created_at_src AS DATE) AND ex.fromCurrency = 'CRC' AND ex.toCurrency = 'USD'
                LEFT JOIN dwh.DimOrder o ON ABS(o.totalOrderUSD - so.total_amount) < 0.01
                WHERE oi.quantity > 0 AND oi.unit_price > 0
            """)
            count_supabase = cur.rowcount
            conn.commit()
            logger.info(f"   ✓ {count_supabase:,} ventas de Supabase")
            
            total_ventas = count_mssql + count_mysql + count_mongo + count_neo4j + count_supabase
            logger.info(f"   📊 Total: {total_ventas:,} transacciones cargadas")
            
            # Verificación Final
            logger.info("\n" + "="*60)
            logger.info("VERIFICACIÓN FINAL")
            logger.info("="*60)
            
            cur.execute("SELECT COUNT(*) FROM dwh.DimCustomer")
            logger.info(f"   • Clientes:   {cur.fetchone()[0]:>8,}")
            
            cur.execute("SELECT COUNT(*) FROM dwh.DimCategory")
            logger.info(f"   • Categorías: {cur.fetchone()[0]:>8,}")
            
            cur.execute("SELECT COUNT(*) FROM dwh.DimProduct")
            logger.info(f"   • Productos:  {cur.fetchone()[0]:>8,}")
            
            cur.execute("SELECT COUNT(*) FROM dwh.DimTime")
            logger.info(f"   • Fechas:     {cur.fetchone()[0]:>8,}")
            
            cur.execute("SELECT COUNT(*) FROM dwh.DimChannel")
            logger.info(f"   • Canales:    {cur.fetchone()[0]:>8,}")
            
            cur.execute("SELECT COUNT(*) FROM dwh.DimExchangeRate")
            logger.info(f"   • Tipos Camb: {cur.fetchone()[0]:>8,}")
            
            cur.execute("SELECT COUNT(*) FROM dwh.DimOrder")
            logger.info(f"   • Órdenes:    {cur.fetchone()[0]:>8,}")
            
            cur.execute("SELECT COUNT(*), SUM(lineTotalUSD) FROM dwh.FactSales")
            ventas, monto = cur.fetchone()
            logger.info(f"   • Ventas:     {ventas:>8,}")
            if monto:
                logger.info(f"   • Monto USD:  ${monto:>13,.2f}")
            else:
                logger.info(f"   • Monto USD:  $          0.00")
            
            # Validar productos con categoría
            cur.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN categoryId IS NOT NULL THEN 1 ELSE 0 END) as con_categoria,
                    SUM(CASE WHEN categoryId IS NULL THEN 1 ELSE 0 END) as sin_categoria
                FROM dwh.DimProduct
            """)
            prod_total, prod_con_cat, prod_sin_cat = cur.fetchone()
            logger.info(f"\n   📦 Productos con categoría: {prod_con_cat:,} / {prod_total:,}")
            if prod_sin_cat > 0:
                logger.info(f"   ⚠️  Productos SIN categoría:  {prod_sin_cat:,}")
            
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
