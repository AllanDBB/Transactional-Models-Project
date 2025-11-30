-- ============================================================================
-- 00-sp_init_schema.sql
-- Stored Procedure para inicializar schema completo del DWH con separación real
-- ============================================================================

-- Crear base de datos si no existe
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = 'MSSQL_DW')
    CREATE DATABASE MSSQL_DW;
GO

USE MSSQL_DW;
GO

-- ============================================================================
-- Crear Schemas Lógicos
-- ============================================================================

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'staging')
    EXEC('CREATE SCHEMA staging');
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'dwh')
    EXEC('CREATE SCHEMA dwh');
GO

-- Eliminar procedimiento si existe
IF OBJECT_ID('dbo.sp_init_schema', 'P') IS NOT NULL
    DROP PROCEDURE dbo.sp_init_schema;
GO

-- ============================================================================
-- Crear procedimiento
-- ============================================================================
CREATE PROCEDURE dbo.sp_init_schema
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY

        -----------------------------------------------------------------------
        -- ELIMINAR TABLAS EN ORDEN CORRECTO (FACT → DIM → STAGING)
        -----------------------------------------------------------------------

        -- FACTS
        IF OBJECT_ID('dwh.FactTargetSales', 'U') IS NOT NULL DROP TABLE dwh.FactTargetSales;
        IF OBJECT_ID('dwh.MetasVentas', 'U') IS NOT NULL DROP TABLE dwh.MetasVentas;
        IF OBJECT_ID('dwh.FactSales', 'U') IS NOT NULL DROP TABLE dwh.FactSales;

        -- DIMENSIONS
        IF OBJECT_ID('dwh.DimOrder', 'U') IS NOT NULL DROP TABLE dwh.DimOrder;
        IF OBJECT_ID('dwh.DimProduct', 'U') IS NOT NULL DROP TABLE dwh.DimProduct;
        IF OBJECT_ID('dwh.DimCustomer', 'U') IS NOT NULL DROP TABLE dwh.DimCustomer;
        IF OBJECT_ID('dwh.DimChannel', 'U') IS NOT NULL DROP TABLE dwh.DimChannel;
        IF OBJECT_ID('dwh.DimCategory', 'U') IS NOT NULL DROP TABLE dwh.DimCategory;
        IF OBJECT_ID('dwh.DimTime', 'U') IS NOT NULL DROP TABLE dwh.DimTime;
        IF OBJECT_ID('dwh.DimExchangeRate', 'U') IS NOT NULL DROP TABLE dwh.DimExchangeRate;

        -- STAGING
        IF OBJECT_ID('staging.source_tracking', 'U') IS NOT NULL DROP TABLE staging.source_tracking;
        IF OBJECT_ID('staging.tipo_cambio', 'U') IS NOT NULL DROP TABLE staging.tipo_cambio;
        IF OBJECT_ID('staging.map_producto', 'U') IS NOT NULL DROP TABLE staging.map_producto;


        -----------------------------------------------------------------------
        -- ============================ STAGING ================================
        -----------------------------------------------------------------------

        CREATE TABLE staging.map_producto (
            map_id INT IDENTITY(1,1) PRIMARY KEY,
            source_system NVARCHAR(50) NOT NULL,
            source_code NVARCHAR(100) NOT NULL,
            sku_oficial NVARCHAR(40) NOT NULL,
            descripcion NVARCHAR(200),
            created_at DATETIME DEFAULT GETDATE(),
            fecha_mapeo DATETIME DEFAULT GETDATE(),
            activo BIT DEFAULT 1,
            CONSTRAINT unique_map UNIQUE (source_system, source_code)
        );
        CREATE INDEX idx_stg_sku ON staging.map_producto(sku_oficial);

        CREATE TABLE staging.tipo_cambio (
            cambio_id INT IDENTITY(1,1) PRIMARY KEY,
            fecha DATE NOT NULL,
            de_moneda CHAR(3) NOT NULL,
            a_moneda CHAR(3) NOT NULL,
            tasa DECIMAL(18,6) NOT NULL,
            fuente NVARCHAR(100) DEFAULT 'BCCR',
            fecha_actualizacion DATETIME DEFAULT GETDATE(),
            CONSTRAINT chk_tasa CHECK (tasa > 0),
            CONSTRAINT chk_monedas CHECK (a_moneda = 'USD'),
            CONSTRAINT unique_cambio UNIQUE (fecha, de_moneda, a_moneda)
        );
        CREATE INDEX idx_stg_fecha_cambio ON staging.tipo_cambio(fecha);

        CREATE TABLE staging.source_tracking (
            tracking_id INT IDENTITY(1,1) PRIMARY KEY,
            source_system NVARCHAR(50) NOT NULL,
            source_key NVARCHAR(100) NOT NULL,
            tabla_destino NVARCHAR(50) NOT NULL,
            id_destino INT NOT NULL,
            created_at DATETIME DEFAULT GETDATE(),
            fecha_carga DATETIME DEFAULT GETDATE(),
            estado NVARCHAR(20) DEFAULT 'ACTIVO',
            CONSTRAINT unique_source_key UNIQUE (source_system, source_key, tabla_destino)
        );
        CREATE INDEX idx_stg_source_tracking ON staging.source_tracking(source_system, source_key);


        -----------------------------------------------------------------------
        -- ============================ DIMENSIONS =============================
        -----------------------------------------------------------------------

        CREATE TABLE dwh.DimTime (
            id INT IDENTITY(1,1) PRIMARY KEY,
            year INT NOT NULL,
            month INT NOT NULL,
            day INT NOT NULL,
            date DATE NOT NULL UNIQUE,
            CONSTRAINT chk_month CHECK (month BETWEEN 1 AND 12),
            CONSTRAINT chk_day CHECK (day BETWEEN 1 AND 31)
        );

        CREATE TABLE dwh.DimCategory (
            id INT IDENTITY(1,1) PRIMARY KEY,
            name VARCHAR(100) NOT NULL UNIQUE
        );

        CREATE TABLE dwh.DimProduct (
            id INT IDENTITY(1,1) PRIMARY KEY,
            name VARCHAR(150) NOT NULL,
            code VARCHAR(50) NOT NULL UNIQUE,
            categoryId INT,
            FOREIGN KEY (categoryId) REFERENCES dwh.DimCategory(id)
        );

        CREATE TABLE dwh.DimCustomer (
            id INT IDENTITY(1,1) PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            email VARCHAR(150) NOT NULL UNIQUE,
            gender VARCHAR(1),
            country VARCHAR(50),
            created_at DATETIME DEFAULT GETDATE(),
            CONSTRAINT chk_email_format CHECK (email LIKE '%@%'),
            CONSTRAINT chk_gender CHECK (gender IN ('M', 'F', 'O'))
        );

        CREATE TABLE dwh.DimChannel (
            id INT IDENTITY(1,1) PRIMARY KEY,
            name VARCHAR(50),
            channelType VARCHAR(50),
            CONSTRAINT chk_channel_type CHECK (channelType IN ('Website', 'Store', 'App', 'Partner', 'Other'))
        );

        CREATE TABLE dwh.DimOrder (
            id INT IDENTITY(1,1) PRIMARY KEY,
            totalOrderUSD DECIMAL(10,2) NOT NULL,
            CONSTRAINT chk_total_order CHECK (totalOrderUSD >= 0)
        );

        CREATE TABLE dwh.DimExchangeRate (
            id INT IDENTITY(1,1) PRIMARY KEY,
            toCurrency VARCHAR(3) NOT NULL,
            fromCurrency VARCHAR(3) NOT NULL,
            date DATE NOT NULL,
            rate DECIMAL(18,6) NOT NULL,
            CONSTRAINT chk_rate_positive CHECK (rate > 0),
            CONSTRAINT unique_exchange_rate UNIQUE (fromCurrency, toCurrency, date)
        );


        -----------------------------------------------------------------------
        -- ================================ FACTS ===============================
        -----------------------------------------------------------------------

        CREATE TABLE dwh.FactSales (
            id INT IDENTITY(1,1) PRIMARY KEY,
            productId INT NOT NULL,
            timeId INT NOT NULL,
            orderId INT NOT NULL,
            channelId INT NOT NULL,
            customerId INT NOT NULL,
            productCant INT NOT NULL,
            productUnitPriceUSD DECIMAL(10,2) NOT NULL,
            lineTotalUSD DECIMAL(10,2) NOT NULL,
            discountPercentage DECIMAL(5,2) DEFAULT 0,
            created_at DATETIME DEFAULT GETDATE(),
            exchangeRateId INT NULL,
            FOREIGN KEY (productId) REFERENCES dwh.DimProduct(id),
            FOREIGN KEY (timeId) REFERENCES dwh.DimTime(id),
            FOREIGN KEY (orderId) REFERENCES dwh.DimOrder(id),
            FOREIGN KEY (channelId) REFERENCES dwh.DimChannel(id),
            FOREIGN KEY (customerId) REFERENCES dwh.DimCustomer(id),
            FOREIGN KEY (exchangeRateId) REFERENCES dwh.DimExchangeRate(id)
        );

        CREATE TABLE dwh.FactTargetSales (
            id INT IDENTITY(1,1) PRIMARY KEY,
            customerId INT,
            productId INT,
            targetUSD DECIMAL(10,2) NOT NULL,
            year INT NOT NULL,
            month INT NOT NULL,
            FOREIGN KEY (customerId) REFERENCES dwh.DimCustomer(id),
            FOREIGN KEY (productId) REFERENCES dwh.DimProduct(id),
            CONSTRAINT unique_target UNIQUE (customerId, productId, year, month)
        );

        CREATE TABLE dwh.MetasVentas (
            MetaID INT IDENTITY(1,1) PRIMARY KEY,
            customerId INT NOT NULL,
            productId INT NOT NULL,
            Anio INT NOT NULL,
            Mes INT NOT NULL,
            MetaUSD DECIMAL(18,2) NOT NULL,
            FOREIGN KEY (customerId) REFERENCES dwh.DimCustomer(id),
            FOREIGN KEY (productId) REFERENCES dwh.DimProduct(id),
            CONSTRAINT unique_meta UNIQUE (customerId, productId, Anio, Mes)
        );


        -----------------------------------------------------------------------
        -- Mensajes de éxito
        -----------------------------------------------------------------------

        PRINT '=========================================================';
        PRINT 'SCHEMA del Data Warehouse INICIALIZADO CORRECTAMENTE';
        PRINT '=========================================================';
        PRINT 'Schemas: staging, dwh';
        PRINT 'Tablas STAGING:   3';
        PRINT 'Tablas DIMENSION: 7';
        PRINT 'Tablas FACT:      3';
        PRINT 'Total tablas:     13';
        PRINT '=========================================================';

    END TRY
    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
        DECLARE @ErrorState INT = ERROR_STATE();

        RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH
END;
GO

GRANT EXECUTE ON dbo.sp_init_schema TO public;
GO

EXEC dbo.sp_init_schema;
