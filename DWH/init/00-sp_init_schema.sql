-- ============================================================================
-- 00-sp_init_schema.sql
-- Stored Procedure para inicializar schema completo del DWH
-- ============================================================================

USE MSSQL_DW;
GO

-- Eliminar procedimiento si existe
IF OBJECT_ID('dbo.sp_init_schema', 'P') IS NOT NULL
    DROP PROCEDURE dbo.sp_init_schema;
GO

-- Crear procedimiento
CREATE PROCEDURE dbo.sp_init_schema
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        
        -- Eliminar tablas existentes (en orden por FKs)
        IF OBJECT_ID('FactTargetSales', 'U') IS NOT NULL DROP TABLE FactTargetSales;
        IF OBJECT_ID('MetasVentas', 'U') IS NOT NULL DROP TABLE MetasVentas;
        IF OBJECT_ID('FactSales', 'U') IS NOT NULL DROP TABLE FactSales;
        IF OBJECT_ID('DimOrder', 'U') IS NOT NULL DROP TABLE DimOrder;
        IF OBJECT_ID('DimProduct', 'U') IS NOT NULL DROP TABLE DimProduct;
        IF OBJECT_ID('DimCustomer', 'U') IS NOT NULL DROP TABLE DimCustomer;
        IF OBJECT_ID('DimChannel', 'U') IS NOT NULL DROP TABLE DimChannel;
        IF OBJECT_ID('DimCategory', 'U') IS NOT NULL DROP TABLE DimCategory;
        IF OBJECT_ID('DimTime', 'U') IS NOT NULL DROP TABLE DimTime;
        IF OBJECT_ID('DimExchangeRate', 'U') IS NOT NULL DROP TABLE DimExchangeRate;
        IF OBJECT_ID('staging_source_tracking', 'U') IS NOT NULL DROP TABLE staging_source_tracking;
        IF OBJECT_ID('staging_tipo_cambio', 'U') IS NOT NULL DROP TABLE staging_tipo_cambio;
        IF OBJECT_ID('staging_map_producto', 'U') IS NOT NULL DROP TABLE staging_map_producto;
        
        
        -- ================================================================
        -- STAGING TABLES
        -- ================================================================
        
        CREATE TABLE staging_map_producto (
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
        CREATE INDEX idx_sku ON staging_map_producto(sku_oficial);
        CREATE INDEX idx_source ON staging_map_producto(source_system, source_code);
        
        CREATE TABLE staging_tipo_cambio (
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
        CREATE INDEX idx_fecha_cambio ON staging_tipo_cambio(fecha, de_moneda);
        CREATE INDEX idx_moneda ON staging_tipo_cambio(de_moneda, a_moneda);
        
        CREATE TABLE staging_source_tracking (
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
        CREATE INDEX idx_source_tracking ON staging_source_tracking(source_system, source_key);
        
        -- ================================================================
        -- DIMENSION TABLES
        -- ================================================================
        
        CREATE TABLE DimTime (
            id INT IDENTITY(1,1) PRIMARY KEY,
            year INT NOT NULL,
            month INT NOT NULL,
            day INT NOT NULL,
            date DATE NOT NULL UNIQUE,
            CONSTRAINT chk_month CHECK (month BETWEEN 1 AND 12),
            CONSTRAINT chk_day CHECK (day BETWEEN 1 AND 31),
        );
        CREATE INDEX idx_dimtime_date ON DimTime(date);
        
        CREATE TABLE DimCategory (
            id INT IDENTITY(1,1) PRIMARY KEY,
            name VARCHAR(100) NOT NULL UNIQUE
        );
        CREATE INDEX idx_dimcategory_name ON DimCategory(name);
        
        CREATE TABLE DimProduct (
            id INT IDENTITY(1,1) PRIMARY KEY,
            name VARCHAR(150) NOT NULL,
            code VARCHAR(50) NOT NULL UNIQUE,
            categoryId INT,
            CONSTRAINT fk_product_category FOREIGN KEY (categoryId) REFERENCES DimCategory(id)
        );
        CREATE INDEX idx_dimproduct_code ON DimProduct(code);
        
        CREATE TABLE DimCustomer (
            id INT IDENTITY(1,1) PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            email VARCHAR(150) NOT NULL UNIQUE,
            gender VARCHAR(1),
            country VARCHAR(50),
            created_at DATETIME DEFAULT GETDATE(),
            CONSTRAINT chk_email_format CHECK (email LIKE '%@%'),
            CONSTRAINT chk_gender CHECK (gender IN ('M', 'F', 'O'))
        );
        CREATE INDEX idx_dimcustomer_email ON DimCustomer(email);
        
        CREATE TABLE DimChannel (
            id INT IDENTITY(1,1) PRIMARY KEY,
            channelType VARCHAR(50),
            CONSTRAINT chk_channel_type CHECK (channelType IN ('Website', 'Store', 'App', 'Partner', 'Other'))
        );
        
        CREATE TABLE DimOrder (
            id INT IDENTITY(1,1) PRIMARY KEY,
            totalOrderUSD DECIMAL(10,2) NOT NULL,
            CONSTRAINT chk_total_order CHECK (totalOrderUSD >= 0)
        );
        
        CREATE TABLE DimExchangeRate (
            id INT IDENTITY(1,1) PRIMARY KEY,
            toCurrency VARCHAR(3) NOT NULL,
            fromCurrency VARCHAR(3) NOT NULL,
            date DATE NOT NULL,
            rate DECIMAL(18,6) NOT NULL,
            CONSTRAINT chk_currencies CHECK (toCurrency IN ('USD')),
            CONSTRAINT chk_from_currencies CHECK (fromCurrency IN ('CRC')),
            CONSTRAINT chk_rate_positive CHECK (rate > 0),
            CONSTRAINT unique_exchange_rate UNIQUE (fromCurrency, toCurrency, date)
        );
        
        -- ================================================================
        -- FACT TABLES
        -- ================================================================
        
        CREATE TABLE FactSales (
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
            CONSTRAINT chk_product_cant CHECK (productCant > 0),
            CONSTRAINT chk_unit_price CHECK (productUnitPriceUSD >= 0),
            CONSTRAINT chk_line_total CHECK (lineTotalUSD >= 0),
            CONSTRAINT chk_discount CHECK (discountPercentage BETWEEN 0 AND 100),
            CONSTRAINT fk_sales_product FOREIGN KEY (productId) REFERENCES DimProduct(id),
            CONSTRAINT fk_sales_time FOREIGN KEY (timeId) REFERENCES DimTime(id),
            CONSTRAINT fk_sales_order FOREIGN KEY (orderId) REFERENCES DimOrder(id),
            CONSTRAINT fk_sales_channel FOREIGN KEY (channelId) REFERENCES DimChannel(id),
            CONSTRAINT fk_sales_customer FOREIGN KEY (customerId) REFERENCES DimCustomer(id),
            CONSTRAINT fk_sales_exchange_rate FOREIGN KEY (exchangeRateId) REFERENCES DimExchangeRate(id)
        );
        CREATE INDEX idx_factsales_product ON FactSales(productId);
        CREATE INDEX idx_factsales_time ON FactSales(timeId);
        CREATE INDEX idx_factsales_customer ON FactSales(customerId);
        CREATE INDEX idx_factsales_channel ON FactSales(channelId);
        CREATE INDEX idx_factsales_order ON FactSales(orderId);
        
        CREATE TABLE FactTargetSales (
            id INT IDENTITY(1,1) PRIMARY KEY,
            customerId INT,
            productId INT,
            targetUSD DECIMAL(10,2) NOT NULL,
            year INT NOT NULL,
            month INT NOT NULL,
            CONSTRAINT chk_target_usd CHECK (targetUSD >= 0),
            CONSTRAINT chk_target_year CHECK (year >= 1950 AND year <= 2200),
            CONSTRAINT chk_target_month CHECK (month BETWEEN 1 AND 12),
            CONSTRAINT fk_target_customer FOREIGN KEY (customerId) REFERENCES DimCustomer(id),
            CONSTRAINT fk_target_product FOREIGN KEY (productId) REFERENCES DimProduct(id),
            CONSTRAINT unique_target UNIQUE (customerId, productId, year, month)
        );
        CREATE INDEX idx_facttarget_customer ON FactTargetSales(customerId);
        CREATE INDEX idx_facttarget_product ON FactTargetSales(productId);
        CREATE INDEX idx_facttarget_period ON FactTargetSales(year, month);
        
        CREATE TABLE MetasVentas (
            MetaID INT IDENTITY(1,1) PRIMARY KEY,
            customerId INT NOT NULL,
            productId INT NOT NULL,
            Anio INT NOT NULL,
            Mes INT NOT NULL,
            MetaUSD DECIMAL(18,2) NOT NULL,
            CONSTRAINT chk_meta_usd CHECK (MetaUSD >= 0),
            CONSTRAINT chk_meta_year CHECK (Anio >= 1950 AND Anio <= 2200),
            CONSTRAINT chk_meta_month CHECK (Mes BETWEEN 1 AND 12),
            CONSTRAINT fk_meta_customer FOREIGN KEY (customerId) REFERENCES DimCustomer(id),
            CONSTRAINT fk_meta_product FOREIGN KEY (productId) REFERENCES DimProduct(id),
            CONSTRAINT unique_meta UNIQUE (customerId, productId, Anio, Mes)
        );
        CREATE INDEX idx_meta_customer ON MetasVentas(customerId);
        CREATE INDEX idx_meta_product ON MetasVentas(productId);
        CREATE INDEX idx_meta_period ON MetasVentas(Anio, Mes);
        
        PRINT '';
        PRINT '========================================';
        PRINT 'SCHEMA DWH INICIALIZADO EXITOSAMENTE';
        PRINT '========================================';
        PRINT 'Tablas de Staging: 3';
        PRINT 'Dimensiones: 7';
        PRINT 'Tablas de Hechos: 3';
        PRINT 'Total: 13 tablas';
        PRINT '========================================';
        
    END TRY
    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
        DECLARE @ErrorState INT = ERROR_STATE();
        
        RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH
END;
GO

-- Otorgar permisos
GRANT EXECUTE ON dbo.sp_init_schema TO public;
GO

GO
