-- ============================================================================
-- 05-sp_promote_templates.sql
-- Promociones staging -> dwh con reglas de mapeo, moneda, genero y trazabilidad.
-- ============================================================================

USE MSSQL_DW;
GO

-- ========================= PRODUCTO ==============================
IF OBJECT_ID('dbo.sp_promote_product', 'P') IS NOT NULL
    DROP PROCEDURE dbo.sp_promote_product;
GO

CREATE PROCEDURE dbo.sp_promote_product
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        -- Asegurar mapeo por defecto (si no existe) usando la clave natural de cada fuente
        INSERT INTO staging.map_producto (source_system, source_code, sku_oficial, descripcion)
        SELECT 'MSSQL_SRC', ISNULL(code, source_key), ISNULL(code, source_key), name
        FROM staging.mssql_products mp
        WHERE NOT EXISTS (
            SELECT 1 FROM staging.map_producto m WHERE m.source_system = 'MSSQL_SRC' AND m.source_code = ISNULL(mp.code, mp.source_key)
        ) AND ISNULL(code, source_key) IS NOT NULL;

        INSERT INTO staging.map_producto (source_system, source_code, sku_oficial, descripcion)
        SELECT 'MySQL', ISNULL(sku, codigo_alt), ISNULL(sku, codigo_alt), nombre
        FROM staging.mysql_products mp
        WHERE NOT EXISTS (
            SELECT 1 FROM staging.map_producto m WHERE m.source_system = 'MySQL' AND m.source_code = ISNULL(mp.sku, mp.codigo_alt)
        ) AND ISNULL(sku, codigo_alt) IS NOT NULL;

        ;WITH src_union AS (
            -- Prioridad: MSSQL_SRC > MySQL
            SELECT
                1 AS priority,
                map.sku_oficial AS code,
                ISNULL(mp.name, 'SIN_NOMBRE') AS name,
                ISNULL(mp.category, 'SIN_CATEGORIA') AS categoria
            FROM staging.mssql_products mp
            LEFT JOIN staging.map_producto map ON map.source_system = 'MSSQL_SRC' AND map.source_code = ISNULL(mp.code, mp.source_key)
            UNION ALL
            SELECT
                2 AS priority,
                map.sku_oficial AS code,
                ISNULL(mp.nombre, 'SIN_NOMBRE') AS name,
                ISNULL(mp.categoria, 'SIN_CATEGORIA') AS categoria
            FROM staging.mysql_products mp
            LEFT JOIN staging.map_producto map ON map.source_system = 'MySQL' AND map.source_code = ISNULL(mp.sku, mp.codigo_alt)
            UNION ALL
            SELECT
                3 AS priority,
                m.sku_oficial AS code,
                ISNULL(m.descripcion, 'SIN_NOMBRE') AS name,
                'SIN_CATEGORIA' AS categoria
            FROM staging.map_producto m
            WHERE m.source_system = 'AUTO'
        ),
        ranked AS (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY code ORDER BY priority) AS rn
            FROM src_union
            WHERE code IS NOT NULL
        )
        -- Asegurar categorias
        MERGE dwh.DimCategory AS cat
        USING (SELECT DISTINCT categoria AS name FROM ranked WHERE rn = 1) AS src(name)
        ON cat.name = src.name
        WHEN NOT MATCHED THEN INSERT (name) VALUES (src.name);

        MERGE dwh.DimProduct AS p
        USING (
            SELECT
                r.code,
                r.name,
                c.id AS categoryId
            FROM ranked r
            LEFT JOIN dwh.DimCategory c ON c.name = r.categoria
            WHERE r.rn = 1
        ) AS src(code, name, categoryId)
        ON p.code = src.code
        WHEN MATCHED THEN
            UPDATE SET p.name = src.name, p.categoryId = src.categoryId
        WHEN NOT MATCHED THEN
            INSERT (name, code, categoryId) VALUES (src.name, src.code, src.categoryId);

        PRINT '[OK] DimProduct/DimCategory promovidos usando map_producto';
    END TRY
    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        RAISERROR(@ErrorMessage, 16, 1);
    END CATCH
END;
GO

-- ========================= CLIENTE ==============================
IF OBJECT_ID('dbo.sp_promote_customer', 'P') IS NOT NULL
    DROP PROCEDURE dbo.sp_promote_customer;
GO

CREATE PROCEDURE dbo.sp_promote_customer
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        MERGE dwh.DimCustomer AS c
        USING (
            SELECT 1 AS priority,
                   email,
                   ISNULL(name, 'SIN_NOMBRE') AS name,
                   country,
                   created_at_src AS created_at,
                   CASE
                       WHEN LOWER(ISNULL(gender,'')) IN ('masculino','m','hombre') THEN 'M'
                       WHEN LOWER(ISNULL(gender,'')) IN ('femenino','f','mujer') THEN 'F'
                       WHEN LOWER(ISNULL(gender,'')) IN ('otro','otros','other','o','x') THEN 'O'
                       ELSE 'O'
                   END AS gender_norm
            FROM staging.mssql_customers
            UNION ALL
            SELECT 2,
                   correo AS email,
                   ISNULL(nombre, 'SIN_NOMBRE'),
                   pais,
                   created_at_src,
                   CASE
                       WHEN LOWER(ISNULL(genero,'')) IN ('m','masculino','hombre') THEN 'M'
                       WHEN LOWER(ISNULL(genero,'')) IN ('f','femenino','mujer') THEN 'F'
                       WHEN LOWER(ISNULL(genero,'')) IN ('x','o','otro','otros','other') THEN 'O'
                       ELSE 'O'
                   END
            FROM staging.mysql_customers
            UNION ALL
            SELECT 3,
                   email,
                   ISNULL(name, 'SIN_NOMBRE'),
                   country,
                   created_at_src,
                   'O'
            FROM staging.supabase_users
            UNION ALL
            SELECT 4,
                   email,
                   ISNULL(name, 'SIN_NOMBRE'),
                   country,
                   NULL,
                   'O'
            FROM staging.mongo_customers
        ) AS src(priority, email, name, country, created_at, gender_norm)
        ON c.email = src.email
        WHEN MATCHED THEN
            UPDATE SET c.name = src.name, c.country = src.country, c.gender = src.gender_norm
        WHEN NOT MATCHED THEN
            INSERT (name, email, country, created_at, gender) VALUES (src.name, src.email, src.country, ISNULL(src.created_at, GETDATE()), src.gender_norm);

        PRINT '[OK] DimCustomer promovida (MSSQL > MySQL > Supabase > Mongo)';
    END TRY
    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        RAISERROR(@ErrorMessage, 16, 1);
    END CATCH
END;
GO

-- ========================= CANAL ==============================
IF OBJECT_ID('dbo.sp_promote_channel', 'P') IS NOT NULL
    DROP PROCEDURE dbo.sp_promote_channel;
GO

CREATE PROCEDURE dbo.sp_promote_channel
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        MERGE dwh.DimChannel AS ch
        USING (
            SELECT DISTINCT ISNULL(channel, 'UNKNOWN') AS name FROM staging.mssql_sales
            UNION
            SELECT DISTINCT ISNULL(channel, 'UNKNOWN') FROM staging.mysql_sales
            UNION
            SELECT DISTINCT 'SUPABASE' FROM staging.supabase_orders
            UNION
            SELECT DISTINCT 'Mongo' FROM staging.mongo_orders
        ) AS src(name)
        ON ch.name = src.name
        WHEN NOT MATCHED THEN INSERT (name, channelType) VALUES (src.name, 'Other');

        PRINT '[OK] DimChannel promovida';
    END TRY
    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        RAISERROR(@ErrorMessage, 16, 1);
    END CATCH
END;
GO

-- ========================= ORDENES/VENTAS (FACTS) ==============================
IF OBJECT_ID('dbo.sp_promote_sales', 'P') IS NOT NULL
    DROP PROCEDURE dbo.sp_promote_sales;
GO

CREATE PROCEDURE dbo.sp_promote_sales
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        ;WITH sales_union AS (
            SELECT 'MSSQL_SRC' AS source_system,
                   ISNULL(s.product_key, s.source_key) AS product_code,
                   c.email AS customer_email,
                   ISNULL(s.channel, 'UNKNOWN') AS channel,
                   CAST(s.quantity AS DECIMAL(18,4)) AS quantity,
                   CAST(s.unit_price AS DECIMAL(18,4)) AS unit_price,
                   ISNULL(s.currency, 'USD') AS currency,
                   s.order_date,
                   ISNULL(s.order_key, s.source_key) AS order_key
            FROM staging.mssql_sales s
            LEFT JOIN staging.mssql_customers c ON c.source_key = s.customer_key
            UNION ALL
            SELECT 'MySQL',
                   ISNULL(s.sku, s.source_key),
                   c.correo,
                   ISNULL(s.channel, 'UNKNOWN'),
                   CAST(s.quantity AS DECIMAL(18,4)),
                   CAST(s.unit_price AS DECIMAL(18,4)),
                   ISNULL(s.currency, 'CRC'),
                   s.order_date,
                   ISNULL(s.order_key, s.source_key)
            FROM staging.mysql_sales s
            LEFT JOIN staging.mysql_customers c ON c.source_key = s.customer_key
            UNION ALL
            SELECT 'MongoDB',
                   ISNULL(i.product_key, i.product_desc),
                   mc.email,
                   'Mongo',
                   CAST(i.quantity AS DECIMAL(18,4)),
                   CAST(i.unit_price AS DECIMAL(18,4)),
                   ISNULL(i.currency, 'CRC'),
                   i.order_date,
                   i.order_key
            FROM staging.mongo_order_items i
            LEFT JOIN staging.mongo_orders mo ON mo.source_key = i.order_key
            LEFT JOIN staging.mongo_customers mc ON mc.source_key = mo.customer_key
            UNION ALL
            SELECT 'SUPABASE',
                   oi.product_key,
                   u.email,
                   'SUPABASE',
                   CAST(oi.quantity AS DECIMAL(18,4)),
                   CAST(oi.unit_price AS DECIMAL(18,4)),
                   'USD',
                   o.created_at_src,
                   oi.order_key
            FROM staging.supabase_order_items oi
            LEFT JOIN staging.supabase_orders o ON o.source_key = oi.order_key
            LEFT JOIN staging.supabase_users u ON u.source_key = o.user_key
            UNION ALL
            SELECT 'NEO4J',
                   i.product_key,
                   NULL, -- sin email, se filtrara despues
                   ISNULL(i.currency, 'USD'),
                   CAST(i.quantity AS DECIMAL(18,4)),
                   CAST(i.unit_price AS DECIMAL(18,4)),
                   ISNULL(i.currency, 'USD'),
                   i.order_date,
                   i.order_key
            FROM staging.neo4j_order_items i
        ),
        sales_filtered AS (
            SELECT * FROM sales_union WHERE customer_email IS NOT NULL AND product_code IS NOT NULL
        ),
        sales_rates AS (
            SELECT su.*,
                   ex.id AS exchangeRateId,
                   CASE WHEN UPPER(ISNULL(su.currency,'')) = 'CRC' THEN ISNULL(ex.rate, 1) ELSE 1 END AS rate_to_usd
            FROM sales_filtered su
            LEFT JOIN dwh.DimExchangeRate ex
                ON ex.[date] = su.order_date
               AND ex.fromCurrency = 'CRC'
               AND ex.toCurrency = 'USD'
        ),
        sales_resolved AS (
            SELECT
                su.*,
                mp.sku_oficial AS sku_oficial
            FROM sales_rates su
            LEFT JOIN staging.map_producto mp ON mp.source_code = su.product_code
        )

        -- Asegurar DimTime
        MERGE dwh.DimTime AS dt
        USING (SELECT DISTINCT order_date FROM sales_resolved WHERE order_date IS NOT NULL) AS src(order_date)
        ON dt.date = src.order_date
        WHEN NOT MATCHED THEN
            INSERT (year, month, day, date) VALUES (YEAR(src.order_date), MONTH(src.order_date), DAY(src.order_date), src.order_date);

        -- Asegurar canales (ya se cubre en sp_promote_channel, se deja por seguridad)
        MERGE dwh.DimChannel AS ch
        USING (SELECT DISTINCT channel FROM sales_resolved) AS src(name)
        ON ch.name = src.name
        WHEN NOT MATCHED THEN INSERT (name, channelType) VALUES (src.name, 'Other');

        -- Asegurar productos existentes (si faltan, usar sku_oficial o product_code)
        INSERT INTO staging.map_producto (source_system, source_code, sku_oficial)
        SELECT DISTINCT 'AUTO', product_code, product_code
        FROM sales_resolved
        WHERE sku_oficial IS NULL
          AND NOT EXISTS (SELECT 1 FROM staging.map_producto m WHERE m.source_code = sales_resolved.product_code);

        -- Re-ejecutar productos para garantizar DimProduct
        EXEC dbo.sp_promote_product;

        -- Asegurar clientes en DimCustomer (previamente llamado sp_promote_customer)
        EXEC dbo.sp_promote_customer;

        -- Preparar tabla de pedidos con totales USD
        DECLARE @Orders TABLE(order_key NVARCHAR(200) PRIMARY KEY, totalUSD DECIMAL(18,4));
        INSERT INTO @Orders(order_key, totalUSD)
        SELECT order_key, SUM( (ISNULL(unit_price,0) / NULLIF(rate_to_usd,0)) * ISNULL(quantity,0) )
        FROM sales_resolved
        GROUP BY order_key;

        -- Mapear order_key a DimOrder vía source_tracking
        DECLARE @OrderMap TABLE(order_key NVARCHAR(200) PRIMARY KEY, orderId INT);
        INSERT INTO @OrderMap(order_key, orderId)
        SELECT o.order_key, st.id_destino
        FROM @Orders o
        JOIN staging.source_tracking st ON st.source_system = 'ORDER' AND st.source_key = o.order_key AND st.tabla_destino = 'DimOrder';

        -- Insertar nuevas ordenes que no existan
        INSERT INTO dwh.DimOrder(totalOrderUSD)
        OUTPUT o.order_key, inserted.id INTO @OrderMap(order_key, orderId)
        SELECT o.order_key, o.totalUSD
        FROM @Orders o
        WHERE NOT EXISTS (SELECT 1 FROM @OrderMap m WHERE m.order_key = o.order_key);

        -- Registrar en tracking
        INSERT INTO staging.source_tracking (source_system, source_key, tabla_destino, id_destino)
        SELECT 'ORDER', om.order_key, 'DimOrder', om.orderId
        FROM @OrderMap om
        WHERE NOT EXISTS (
            SELECT 1 FROM staging.source_tracking st WHERE st.source_system = 'ORDER' AND st.source_key = om.order_key AND st.tabla_destino = 'DimOrder'
        );

        -- Insertar facts
        INSERT INTO dwh.FactSales (productId, timeId, orderId, channelId, customerId, productCant, productUnitPriceUSD, lineTotalUSD, discountPercentage, exchangeRateId)
        SELECT
            p.id AS productId,
            dt.id AS timeId,
            om.orderId,
            ch.id AS channelId,
            dc.id AS customerId,
            CAST(sr.quantity AS INT) AS productCant,
            CAST(ISNULL(sr.unit_price,0) / NULLIF(sr.rate_to_usd,1) AS DECIMAL(18,4)) AS productUnitPriceUSD,
            CAST( (ISNULL(sr.unit_price,0) / NULLIF(sr.rate_to_usd,1)) * ISNULL(sr.quantity,0) AS DECIMAL(18,4)) AS lineTotalUSD,
            0 AS discountPercentage,
            sr.exchangeRateId
        FROM sales_resolved sr
        JOIN staging.map_producto mp ON mp.source_code = sr.product_code
        JOIN dwh.DimProduct p ON p.code = mp.sku_oficial
        JOIN dwh.DimChannel ch ON ch.name = sr.channel
        JOIN dwh.DimCustomer dc ON dc.email = sr.customer_email
        LEFT JOIN dwh.DimTime dt ON dt.date = sr.order_date
        JOIN @OrderMap om ON om.order_key = sr.order_key
        WHERE dt.id IS NOT NULL; -- solo fechas válidas

        PRINT '[OK] FactSales promovida';
    END TRY
    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        RAISERROR(@ErrorMessage, 16, 1);
    END CATCH
END;
GO

-- ========================= MAESTRO ==============================
IF OBJECT_ID('dbo.sp_etl_run_all', 'P') IS NOT NULL
    DROP PROCEDURE dbo.sp_etl_run_all;
GO

CREATE PROCEDURE dbo.sp_etl_run_all
AS
BEGIN
    SET NOCOUNT ON;
    EXEC dbo.sp_promote_exchange_rate;
    EXEC dbo.sp_promote_product;
    EXEC dbo.sp_promote_customer;
    EXEC dbo.sp_promote_channel;
    EXEC dbo.sp_promote_sales;
END;
GO

GRANT EXECUTE ON dbo.sp_promote_product TO public;
GRANT EXECUTE ON dbo.sp_promote_customer TO public;
GRANT EXECUTE ON dbo.sp_promote_channel TO public;
GRANT EXECUTE ON dbo.sp_promote_sales TO public;
GRANT EXECUTE ON dbo.sp_etl_run_all TO public;
GO
