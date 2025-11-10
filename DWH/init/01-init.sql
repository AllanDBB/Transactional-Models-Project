-- Script de inicialización para ClickHouse Data Warehouse

-- Crear base de datos
CREATE DATABASE IF NOT EXISTS dwh;

USE dwh;

-- Tabla de hechos: Ventas
CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id UInt64,
    date_key UInt32,
    product_key UInt32,
    customer_key UInt32,
    quantity UInt32,
    unit_price Decimal(10, 2),
    total_amount Decimal(10, 2),
    discount Decimal(10, 2),
    tax Decimal(10, 2),
    net_amount Decimal(10, 2),
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (date_key, product_key, customer_key)
PARTITION BY toYYYYMM(created_at);

-- Dimensión: Fechas
CREATE TABLE IF NOT EXISTS dim_date (
    date_key UInt32,
    date Date,
    year UInt16,
    quarter UInt8,
    month UInt8,
    month_name String,
    week UInt8,
    day UInt8,
    day_of_week UInt8,
    day_name String,
    is_weekend UInt8,
    is_holiday UInt8
) ENGINE = MergeTree()
ORDER BY date_key;

-- Dimensión: Productos
CREATE TABLE IF NOT EXISTS dim_product (
    product_key UInt32,
    product_id String,
    product_name String,
    category String,
    subcategory String,
    brand String,
    supplier String,
    unit_cost Decimal(10, 2),
    unit_price Decimal(10, 2),
    active UInt8,
    created_at DateTime,
    updated_at DateTime
) ENGINE = MergeTree()
ORDER BY product_key;

-- Dimensión: Clientes
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key UInt32,
    customer_id String,
    customer_name String,
    email String,
    phone String,
    country String,
    city String,
    segment String,
    registration_date Date,
    active UInt8
) ENGINE = MergeTree()
ORDER BY customer_key;

-- Dimensión: Ubicaciones
CREATE TABLE IF NOT EXISTS dim_location (
    location_key UInt32,
    country String,
    state String,
    city String,
    postal_code String,
    region String,
    latitude Decimal(10, 6),
    longitude Decimal(10, 6)
) ENGINE = MergeTree()
ORDER BY location_key;

-- Tabla agregada: Ventas por día
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_sales_daily
ENGINE = SummingMergeTree()
ORDER BY (date_key, product_key)
AS SELECT
    date_key,
    product_key,
    sum(quantity) as total_quantity,
    sum(total_amount) as total_sales,
    sum(net_amount) as total_net_sales,
    count() as transaction_count
FROM fact_sales
GROUP BY date_key, product_key;

-- Tabla agregada: Ventas por mes
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_sales_monthly
ENGINE = SummingMergeTree()
ORDER BY (year_month, product_key)
AS SELECT
    toYYYYMM(created_at) as year_month,
    product_key,
    sum(quantity) as total_quantity,
    sum(total_amount) as total_sales,
    sum(net_amount) as total_net_sales,
    count() as transaction_count
FROM fact_sales
GROUP BY year_month, product_key;

-- Insertar datos de ejemplo en dim_date
INSERT INTO dim_date VALUES
(20240101, '2024-01-01', 2024, 1, 1, 'January', 1, 1, 1, 'Monday', 0, 0),
(20240102, '2024-01-02', 2024, 1, 1, 'January', 1, 2, 2, 'Tuesday', 0, 0),
(20240103, '2024-01-03', 2024, 1, 1, 'January', 1, 3, 3, 'Wednesday', 0, 0);

-- Insertar datos de ejemplo en dim_product
INSERT INTO dim_product VALUES
(1, 'PROD001', 'Laptop Dell XPS 15', 'Electronics', 'Computers', 'Dell', 'TechSupplier', 1000.00, 1299.99, 1, now(), now()),
(2, 'PROD002', 'Python Book', 'Books', 'Programming', 'OReilly', 'BookDistributor', 30.00, 49.99, 1, now(), now()),
(3, 'PROD003', 'Wireless Mouse', 'Electronics', 'Accessories', 'Logitech', 'TechSupplier', 15.00, 29.99, 1, now(), now());

-- Insertar datos de ejemplo en dim_customer
INSERT INTO dim_customer VALUES
(1, 'CUST001', 'John Doe', 'john@example.com', '+1234567890', 'USA', 'New York', 'Premium', '2023-01-15', 1),
(2, 'CUST002', 'Jane Smith', 'jane@example.com', '+0987654321', 'USA', 'Los Angeles', 'Regular', '2023-03-20', 1);

-- Insertar datos de ejemplo en fact_sales
INSERT INTO fact_sales VALUES
(1, 20240101, 1, 1, 1, 1299.99, 1299.99, 0.00, 104.00, 1195.99, '2024-01-01 10:30:00'),
(2, 20240101, 3, 1, 2, 29.99, 59.98, 5.00, 4.40, 50.58, '2024-01-01 11:15:00'),
(3, 20240102, 2, 2, 1, 49.99, 49.99, 0.00, 4.00, 45.99, '2024-01-02 14:20:00');

SELECT 'ClickHouse Data Warehouse initialization completed successfully!' as message;
