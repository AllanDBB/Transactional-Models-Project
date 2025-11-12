
-- ============================================================================
-- DATABASE AND USER SETUP
-- ============================================================================

-- Create database if not exists
IF DB_ID('MSSQL_DW') IS NULL
BEGIN
    CREATE DATABASE MSSQL_DW;
END
GO

USE MSSQL_DW;
GO

-- Drop existing login and user if exist
IF EXISTS (SELECT * FROM sys.server_principals WHERE name = 'admin')
BEGIN
    DROP LOGIN admin;
END
GO

IF EXISTS (SELECT * FROM sys.database_principals WHERE name = 'admin')
BEGIN
    DROP USER admin;
END
GO

-- Create new login and user
CREATE LOGIN admin WITH PASSWORD = 'admin123', CHECK_POLICY = OFF, CHECK_EXPIRATION = OFF;
CREATE USER admin FOR LOGIN admin;
GO

-- Grant permissions on this database
ALTER ROLE db_datareader ADD MEMBER admin;
ALTER ROLE db_datawriter ADD MEMBER admin;
GRANT CREATE TABLE TO admin;
GRANT ALTER TO admin;
GRANT REFERENCES TO admin;
GRANT EXECUTE TO admin;
GO


-- ============================================================================
-- DIMENSION TABLES
-- ============================================================================

-- Time dimension with exchange rate
CREATE TABLE DimTime (
    id INT PRIMARY KEY,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    date DATE NOT NULL UNIQUE,
    exchangeRateToUSD DECIMAL(10,6) NOT NULL,
    CONSTRAINT chk_month CHECK (month BETWEEN 1 AND 12),
    CONSTRAINT chk_day CHECK (day BETWEEN 1 AND 31),
    CONSTRAINT chk_exchange_rate CHECK (exchangeRateToUSD > 0)
);
CREATE INDEX idx_dimtime_date ON DimTime(date);


-- Category dimension
CREATE TABLE DimCategory (
    id INT IDENTITY(1,1) PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE
);
CREATE INDEX idx_dimcategory_name ON DimCategory(name);


-- Product dimension
CREATE TABLE DimProduct (
    id INT IDENTITY(1,1) PRIMARY KEY,
    name VARCHAR(150) NOT NULL,
    code VARCHAR(50) NOT NULL UNIQUE,
    categoryId INT,
    CONSTRAINT fk_product_category FOREIGN KEY (categoryId) REFERENCES DimCategory(id)
);
CREATE INDEX idx_dimproduct_code ON DimProduct(code);


-- Customer dimension
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


-- Channel dimension
CREATE TABLE DimChannel (
    id INT IDENTITY(1,1) PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    channelType VARCHAR(50),
    CONSTRAINT chk_channel_type CHECK (channelType IN ('Website', 'Store', 'App', 'Partner', 'Other'))
);
CREATE INDEX idx_dimchannel_name ON DimChannel(name);


-- Order dimension
CREATE TABLE DimOrder (
    id INT IDENTITY(1,1) PRIMARY KEY,
    totalOrderUSD DECIMAL(10,2) NOT NULL,
    CONSTRAINT chk_total_order CHECK (totalOrderUSD >= 0)
);

-- EXCHANGE RATE dimension

CREATE TABLE DimExchangeRate (
    id INT IDENTITY(1,1) PRIMARY KEY,
    toCurrency VARCHAR(3) NOT NULL,
    fromCurrency VARCHAR(3) NOT NULL,
    date DATE NOT NULL,
    rate DECIMAL(18,6) NOT NULL,
    created_at DATETIME DEFAULT GETDATE(),
    updated_at DATETIME DEFAULT GETDATE(),
    
    CONSTRAINT chk_currencies CHECK (toCurrency IN ('USD')),
    CONSTRAINT chk_from_currencies CHECK (fromCurrency IN ('CRC')),
    CONSTRAINT chk_rate_positive CHECK (rate > 0),
    CONSTRAINT unique_exchange_rate UNIQUE (fromCurrency, toCurrency, date)
);


-- ============================================================================
-- FACT TABLES
-- ============================================================================

-- Main sales fact table
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


-- Target sales fact table
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

GO