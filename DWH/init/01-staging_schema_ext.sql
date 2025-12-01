-- ============================================================================
-- 01-staging_schema_ext.sql
-- Definicion de staging estandar para fuentes: MongoDB, MSSQL (fuente),
-- MySQL, Neo4j, Supabase.
-- Staging: sin FKs, indices/uniques por claves naturales.
-- ============================================================================

USE MSSQL_DW;
GO

-- Crear schema staging si no existe
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'staging')
    EXEC('CREATE SCHEMA staging');
GO

-- ====================== MongoDB =========================
IF OBJECT_ID('staging.mongo_orders', 'U') IS NULL
BEGIN
    CREATE TABLE staging.mongo_orders (
        staging_id     INT IDENTITY(1,1) PRIMARY KEY,
        source_system  NVARCHAR(50) NOT NULL DEFAULT 'MongoDB',
        source_key     NVARCHAR(200) NOT NULL, -- _id de Mongo
        customer_key   NVARCHAR(200) NULL,
        order_date     DATE NULL,
        total_amount   DECIMAL(18,2) NULL,
        currency       NVARCHAR(10) NULL,
        payload_json   NVARCHAR(MAX) NULL,
        created_at     DATETIME DEFAULT GETDATE(),
        fecha_carga    DATETIME DEFAULT GETDATE(),
        estado         NVARCHAR(20) DEFAULT 'ACTIVO',
        CONSTRAINT uq_mongo_orders UNIQUE (source_system, source_key)
    );
    CREATE INDEX ix_mongo_orders_order_date ON staging.mongo_orders(order_date);
END
GO

IF OBJECT_ID('staging.mongo_order_items', 'U') IS NULL
BEGIN
    CREATE TABLE staging.mongo_order_items (
        staging_id     INT IDENTITY(1,1) PRIMARY KEY,
        source_system  NVARCHAR(50) NOT NULL DEFAULT 'MongoDB',
        source_key     NVARCHAR(200) NOT NULL, -- item id synthetic
        order_key      NVARCHAR(200) NOT NULL,
        product_key    NVARCHAR(200) NULL,
        product_desc   NVARCHAR(400) NULL,
        quantity       DECIMAL(18,4) NULL,
        unit_price     DECIMAL(18,4) NULL,
        currency       NVARCHAR(10) NULL,
        order_date     DATE NULL,
        payload_json   NVARCHAR(MAX) NULL,
        created_at     DATETIME DEFAULT GETDATE(),
        fecha_carga    DATETIME DEFAULT GETDATE(),
        estado         NVARCHAR(20) DEFAULT 'ACTIVO',
        CONSTRAINT uq_mongo_order_items UNIQUE (source_system, source_key)
    );
    CREATE INDEX ix_mongo_order_items_order ON staging.mongo_order_items(order_key);
END
GO

IF OBJECT_ID('staging.mongo_customers', 'U') IS NULL
BEGIN
    CREATE TABLE staging.mongo_customers (
        staging_id     INT IDENTITY(1,1) PRIMARY KEY,
        source_system  NVARCHAR(50) NOT NULL DEFAULT 'MongoDB',
        source_key     NVARCHAR(200) NOT NULL, -- _id de Mongo
        name           NVARCHAR(200) NULL,
        email          NVARCHAR(200) NULL,
        genero         NVARCHAR(20) NULL, -- 'Masculino', 'Femenino', 'Otro'
        payload_json   NVARCHAR(MAX) NULL,
        created_at     DATETIME DEFAULT GETDATE(),
        fecha_carga    DATETIME DEFAULT GETDATE(),
        estado         NVARCHAR(20) DEFAULT 'ACTIVO',
        CONSTRAINT uq_mongo_customers UNIQUE (source_system, source_key)
    );
    CREATE INDEX ix_mongo_customers_email ON staging.mongo_customers(email);
END
GO

IF OBJECT_ID('staging.mongo_products', 'U') IS NULL
BEGIN
    CREATE TABLE staging.mongo_products (
        staging_id     INT IDENTITY(1,1) PRIMARY KEY,
        source_system  NVARCHAR(50) NOT NULL DEFAULT 'MongoDB',
        source_key     NVARCHAR(200) NOT NULL, -- _id o codigo_mongo
        codigo_mongo   NVARCHAR(100) NULL,
        nombre         NVARCHAR(200) NULL,
        categoria      NVARCHAR(100) NULL,
        sku_equiv      NVARCHAR(100) NULL, -- equivalencias.sku
        alt_equiv      NVARCHAR(100) NULL, -- equivalencias.alt
        payload_json   NVARCHAR(MAX) NULL,
        created_at     DATETIME DEFAULT GETDATE(),
        fecha_carga    DATETIME DEFAULT GETDATE(),
        estado         NVARCHAR(20) DEFAULT 'ACTIVO',
        CONSTRAINT uq_mongo_products UNIQUE (source_system, source_key)
    );
    CREATE INDEX ix_mongo_products_codigo ON staging.mongo_products(codigo_mongo);
END
GO

-- ====================== MSSQL (fuente transaccional) =========================
IF OBJECT_ID('staging.mssql_customers', 'U') IS NULL
BEGIN
    CREATE TABLE staging.mssql_customers (
        staging_id     INT IDENTITY(1,1) PRIMARY KEY,
        source_system  NVARCHAR(50) NOT NULL DEFAULT 'MSSQL_SRC',
        source_key     NVARCHAR(200) NOT NULL,
        name           NVARCHAR(200) NULL,
        email          NVARCHAR(200) NULL,
        gender         NVARCHAR(20) NULL,
        country        NVARCHAR(100) NULL,
        created_at_src DATE NULL,
        payload_json   NVARCHAR(MAX) NULL,
        created_at     DATETIME DEFAULT GETDATE(),
        fecha_carga    DATETIME DEFAULT GETDATE(),
        estado         NVARCHAR(20) DEFAULT 'ACTIVO',
        CONSTRAINT uq_mssql_customers UNIQUE (source_system, source_key)
    );
    CREATE INDEX ix_mssql_customers_email ON staging.mssql_customers(email);
END
GO

IF OBJECT_ID('staging.mssql_products', 'U') IS NULL
BEGIN
    CREATE TABLE staging.mssql_products (
        staging_id     INT IDENTITY(1,1) PRIMARY KEY,
        source_system  NVARCHAR(50) NOT NULL DEFAULT 'MSSQL_SRC',
        source_key     NVARCHAR(200) NOT NULL,
        code           NVARCHAR(100) NULL,
        name           NVARCHAR(200) NULL,
        category       NVARCHAR(100) NULL,
        price          DECIMAL(18,2) NULL,
        payload_json   NVARCHAR(MAX) NULL,
        created_at     DATETIME DEFAULT GETDATE(),
        fecha_carga    DATETIME DEFAULT GETDATE(),
        estado         NVARCHAR(20) DEFAULT 'ACTIVO',
        CONSTRAINT uq_mssql_products UNIQUE (source_system, source_key)
    );
    CREATE INDEX ix_mssql_products_code ON staging.mssql_products(code);
END
GO

IF OBJECT_ID('staging.mssql_sales', 'U') IS NULL
BEGIN
    CREATE TABLE staging.mssql_sales (
        staging_id     INT IDENTITY(1,1) PRIMARY KEY,
        source_system  NVARCHAR(50) NOT NULL DEFAULT 'MSSQL_SRC',
        source_key     NVARCHAR(200) NOT NULL,
        product_key    NVARCHAR(200) NULL,
        customer_key   NVARCHAR(200) NULL,
        order_key      NVARCHAR(200) NULL,
        channel        NVARCHAR(50) NULL,
        quantity       INT NULL,
        unit_price     DECIMAL(18,2) NULL,
        currency       NVARCHAR(10) NULL,
        order_date     DATE NULL,
        payload_json   NVARCHAR(MAX) NULL,
        created_at     DATETIME DEFAULT GETDATE(),
        fecha_carga    DATETIME DEFAULT GETDATE(),
        estado         NVARCHAR(20) DEFAULT 'ACTIVO',
        CONSTRAINT uq_mssql_sales UNIQUE (source_system, source_key)
    );
    CREATE INDEX ix_mssql_sales_order_date ON staging.mssql_sales(order_date);
END
GO

-- ====================== MySQL =========================
IF OBJECT_ID('staging.mysql_customers', 'U') IS NULL
BEGIN
    CREATE TABLE staging.mysql_customers (
        staging_id     INT IDENTITY(1,1) PRIMARY KEY,
        source_system  NVARCHAR(50) NOT NULL DEFAULT 'MySQL',
        source_key     NVARCHAR(200) NOT NULL,
        nombre         NVARCHAR(200) NULL,
        correo         NVARCHAR(200) NULL,
        genero         NVARCHAR(10) NULL,
        pais           NVARCHAR(100) NULL,
        created_at_src DATE NULL,
        payload_json   NVARCHAR(MAX) NULL,
        created_at     DATETIME DEFAULT GETDATE(),
        fecha_carga    DATETIME DEFAULT GETDATE(),
        estado         NVARCHAR(20) DEFAULT 'ACTIVO',
        CONSTRAINT uq_mysql_customers UNIQUE (source_system, source_key)
    );
    CREATE INDEX ix_mysql_customers_correo ON staging.mysql_customers(correo);
END
GO

IF OBJECT_ID('staging.mysql_products', 'U') IS NULL
BEGIN
    CREATE TABLE staging.mysql_products (
        staging_id     INT IDENTITY(1,1) PRIMARY KEY,
        source_system  NVARCHAR(50) NOT NULL DEFAULT 'MySQL',
        source_key     NVARCHAR(200) NOT NULL,
        sku            NVARCHAR(100) NULL,
        codigo_alt     NVARCHAR(100) NULL,
        nombre         NVARCHAR(200) NULL,
        categoria      NVARCHAR(100) NULL,
        precio         DECIMAL(18,2) NULL,
        payload_json   NVARCHAR(MAX) NULL,
        created_at     DATETIME DEFAULT GETDATE(),
        fecha_carga    DATETIME DEFAULT GETDATE(),
        estado         NVARCHAR(20) DEFAULT 'ACTIVO',
        CONSTRAINT uq_mysql_products UNIQUE (source_system, source_key)
    );
    CREATE INDEX ix_mysql_products_sku ON staging.mysql_products(sku);
END
GO

IF OBJECT_ID('staging.mysql_sales', 'U') IS NULL
BEGIN
    CREATE TABLE staging.mysql_sales (
        staging_id     INT IDENTITY(1,1) PRIMARY KEY,
        source_system  NVARCHAR(50) NOT NULL DEFAULT 'MySQL',
        source_key     NVARCHAR(200) NOT NULL,
        sku            NVARCHAR(100) NULL,
        customer_key   NVARCHAR(200) NULL,
        order_key      NVARCHAR(200) NULL,
        channel        NVARCHAR(50) NULL,
        quantity       INT NULL,
        unit_price     DECIMAL(18,2) NULL,
        currency       NVARCHAR(10) NULL,
        order_date     DATE NULL,
        payload_json   NVARCHAR(MAX) NULL,
        created_at     DATETIME DEFAULT GETDATE(),
        fecha_carga    DATETIME DEFAULT GETDATE(),
        estado         NVARCHAR(20) DEFAULT 'ACTIVO',
        CONSTRAINT uq_mysql_sales UNIQUE (source_system, source_key)
    );
    CREATE INDEX ix_mysql_sales_order_date ON staging.mysql_sales(order_date);
END
GO

-- ====================== Neo4j =========================
IF OBJECT_ID('staging.neo4j_nodes', 'U') IS NULL
BEGIN
    CREATE TABLE staging.neo4j_nodes (
        staging_id    INT IDENTITY(1,1) PRIMARY KEY,
        source_system NVARCHAR(50) NOT NULL DEFAULT 'NEO4J',
        node_label    NVARCHAR(100) NOT NULL,
        node_key      NVARCHAR(200) NOT NULL,
        props_json    NVARCHAR(MAX) NULL,
        created_at    DATETIME DEFAULT GETDATE(),
        fecha_carga   DATETIME DEFAULT GETDATE(),
        estado        NVARCHAR(20) DEFAULT 'ACTIVO',
        CONSTRAINT uq_neo4j_nodes UNIQUE (source_system, node_label, node_key)
    );
    CREATE INDEX ix_neo4j_nodes_label ON staging.neo4j_nodes(node_label);
END
GO

IF OBJECT_ID('staging.neo4j_edges', 'U') IS NULL
BEGIN
    CREATE TABLE staging.neo4j_edges (
        staging_id     INT IDENTITY(1,1) PRIMARY KEY,
        source_system  NVARCHAR(50) NOT NULL DEFAULT 'NEO4J',
        edge_type      NVARCHAR(100) NOT NULL,
        from_label     NVARCHAR(100) NULL,
        from_key       NVARCHAR(200) NOT NULL,
        to_label       NVARCHAR(100) NULL,
        to_key         NVARCHAR(200) NOT NULL,
        props_json     NVARCHAR(MAX) NULL,
        created_at     DATETIME DEFAULT GETDATE(),
        fecha_carga    DATETIME DEFAULT GETDATE(),
        estado         NVARCHAR(20) DEFAULT 'ACTIVO',
        CONSTRAINT uq_neo4j_edges UNIQUE (source_system, edge_type, from_key, to_key)
    );
    CREATE INDEX ix_neo4j_edges_type ON staging.neo4j_edges(edge_type);
END
GO

IF OBJECT_ID('staging.neo4j_order_items', 'U') IS NULL
BEGIN
    CREATE TABLE staging.neo4j_order_items (
        staging_id     INT IDENTITY(1,1) PRIMARY KEY,
        source_system  NVARCHAR(50) NOT NULL DEFAULT 'NEO4J',
        source_key     NVARCHAR(200) NOT NULL,
        order_key      NVARCHAR(200) NOT NULL,
        product_key    NVARCHAR(200) NOT NULL,
        customer_key   NVARCHAR(200) NULL,
        category_key   NVARCHAR(200) NULL,
        quantity       DECIMAL(18,4) NULL,
        unit_price     DECIMAL(18,4) NULL,
        currency       NVARCHAR(10) NULL,
        order_date     DATE NULL,
        payload_json   NVARCHAR(MAX) NULL,
        created_at     DATETIME DEFAULT GETDATE(),
        fecha_carga    DATETIME DEFAULT GETDATE(),
        estado         NVARCHAR(20) DEFAULT 'ACTIVO',
        CONSTRAINT uq_neo4j_order_items UNIQUE (source_system, source_key)
    );
    CREATE INDEX ix_neo4j_order_items_order ON staging.neo4j_order_items(order_key);
END
GO

-- ====================== Supabase =========================
IF OBJECT_ID('staging.supabase_users', 'U') IS NULL
BEGIN
    CREATE TABLE staging.supabase_users (
        staging_id     INT IDENTITY(1,1) PRIMARY KEY,
        source_system  NVARCHAR(50) NOT NULL DEFAULT 'SUPABASE',
        source_key     NVARCHAR(200) NOT NULL, -- uuid
        email          NVARCHAR(200) NULL,
        name           NVARCHAR(200) NULL,
        gender         NVARCHAR(10) NULL,
        country        NVARCHAR(100) NULL,
        created_at_src DATETIME NULL,
        payload_json   NVARCHAR(MAX) NULL,
        created_at     DATETIME DEFAULT GETDATE(),
        fecha_carga    DATETIME DEFAULT GETDATE(),
        estado         NVARCHAR(20) DEFAULT 'ACTIVO',
        CONSTRAINT uq_supabase_users UNIQUE (source_system, source_key)
    );
    CREATE INDEX ix_supabase_users_email ON staging.supabase_users(email);
END
GO

IF OBJECT_ID('staging.supabase_orders', 'U') IS NULL
BEGIN
    CREATE TABLE staging.supabase_orders (
        staging_id      INT IDENTITY(1,1) PRIMARY KEY,
        source_system   NVARCHAR(50) NOT NULL DEFAULT 'SUPABASE',
        source_key      NVARCHAR(200) NOT NULL, -- uuid
        user_key        NVARCHAR(200) NULL,
        total_amount    DECIMAL(18,2) NULL,
        status          NVARCHAR(50) NULL,
        payment_method  NVARCHAR(100) NULL,
        created_at_src  DATETIME NULL,
        updated_at_src  DATETIME NULL,
        payload_json    NVARCHAR(MAX) NULL,
        created_at      DATETIME DEFAULT GETDATE(),
        fecha_carga     DATETIME DEFAULT GETDATE(),
        estado          NVARCHAR(20) DEFAULT 'ACTIVO',
        CONSTRAINT uq_supabase_orders UNIQUE (source_system, source_key)
    );
    CREATE INDEX ix_supabase_orders_user ON staging.supabase_orders(user_key);
END
GO

IF OBJECT_ID('staging.supabase_order_items', 'U') IS NULL
BEGIN
    CREATE TABLE staging.supabase_order_items (
        staging_id     INT IDENTITY(1,1) PRIMARY KEY,
        source_system  NVARCHAR(50) NOT NULL DEFAULT 'SUPABASE',
        source_key     NVARCHAR(200) NOT NULL, -- uuid
        order_key      NVARCHAR(200) NOT NULL,
        product_key    NVARCHAR(200) NOT NULL,
        quantity       INT NULL,
        unit_price     DECIMAL(18,2) NULL,
        subtotal       DECIMAL(18,2) NULL,
        payload_json   NVARCHAR(MAX) NULL,
        created_at     DATETIME DEFAULT GETDATE(),
        fecha_carga    DATETIME DEFAULT GETDATE(),
        estado         NVARCHAR(20) DEFAULT 'ACTIVO',
        CONSTRAINT uq_supabase_order_items UNIQUE (source_system, source_key)
    );
    CREATE INDEX ix_supabase_order_items_order ON staging.supabase_order_items(order_key);
END
GO

IF OBJECT_ID('staging.supabase_products', 'U') IS NULL
BEGIN
    CREATE TABLE staging.supabase_products (
        staging_id     INT IDENTITY(1,1) PRIMARY KEY,
        source_system  NVARCHAR(50) NOT NULL DEFAULT 'SUPABASE',
        source_key     NVARCHAR(200) NOT NULL, -- uuid
        name           NVARCHAR(200) NULL,
        description    NVARCHAR(MAX) NULL,
        category       NVARCHAR(100) NULL,
        price          DECIMAL(18,2) NULL,
        stock          INT NULL,
        supplier_id    NVARCHAR(200) NULL,
        active         BIT NULL,
        created_at_src DATETIME NULL,
        payload_json   NVARCHAR(MAX) NULL,
        created_at     DATETIME DEFAULT GETDATE(),
        fecha_carga    DATETIME DEFAULT GETDATE(),
        estado         NVARCHAR(20) DEFAULT 'ACTIVO',
        CONSTRAINT uq_supabase_products UNIQUE (source_system, source_key)
    );
    CREATE INDEX ix_supabase_products_category ON staging.supabase_products(category);
END
GO
