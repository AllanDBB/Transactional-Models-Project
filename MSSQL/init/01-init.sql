-- Script de inicialización para MS SQL Server

-- Crear base de datos si no existe
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'transactional_db')
BEGIN
    CREATE DATABASE transactional_db;
END
GO

USE transactional_db;
GO

-- Crear tabla de usuarios
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[users]') AND type in (N'U'))
BEGIN
    CREATE TABLE users (
        id INT IDENTITY(1,1) PRIMARY KEY,
        username NVARCHAR(50) UNIQUE NOT NULL,
        email NVARCHAR(100) UNIQUE NOT NULL,
        password_hash NVARCHAR(255) NOT NULL,
        role NVARCHAR(20) DEFAULT 'customer' CHECK (role IN ('admin', 'customer', 'supplier')),
        created_at DATETIME2 DEFAULT GETDATE(),
        updated_at DATETIME2 DEFAULT GETDATE(),
        active BIT DEFAULT 1
    );
    CREATE INDEX idx_email ON users(email);
    CREATE INDEX idx_username ON users(username);
END
GO

-- Crear tabla de productos
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[products]') AND type in (N'U'))
BEGIN
    CREATE TABLE products (
        id INT IDENTITY(1,1) PRIMARY KEY,
        name NVARCHAR(200) NOT NULL,
        description NVARCHAR(MAX),
        category NVARCHAR(50),
        price DECIMAL(10, 2) NOT NULL,
        stock INT DEFAULT 0,
        supplier_id INT,
        created_at DATETIME2 DEFAULT GETDATE(),
        updated_at DATETIME2 DEFAULT GETDATE(),
        active BIT DEFAULT 1
    );
    CREATE INDEX idx_category ON products(category);
    CREATE INDEX idx_supplier ON products(supplier_id);
END
GO

-- Crear tabla de pedidos
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[orders]') AND type in (N'U'))
BEGIN
    CREATE TABLE orders (
        id INT IDENTITY(1,1) PRIMARY KEY,
        user_id INT NOT NULL FOREIGN KEY REFERENCES users(id),
        total_amount DECIMAL(10, 2) NOT NULL,
        status NVARCHAR(20) DEFAULT 'pending' CHECK (status IN ('pending', 'processing', 'shipped', 'delivered', 'cancelled')),
        payment_method NVARCHAR(50),
        shipping_address NVARCHAR(MAX),
        created_at DATETIME2 DEFAULT GETDATE(),
        updated_at DATETIME2 DEFAULT GETDATE()
    );
    CREATE INDEX idx_user ON orders(user_id);
    CREATE INDEX idx_status ON orders(status);
END
GO

-- Insertar datos de ejemplo
INSERT INTO users (username, email, password_hash, role) VALUES
('admin', 'admin@example.com', 'hash123', 'admin'),
('customer1', 'customer1@example.com', 'hash456', 'customer'),
('supplier1', 'supplier1@example.com', 'hash789', 'supplier');
GO

INSERT INTO products (name, description, category, price, stock, supplier_id) VALUES
('Laptop Dell XPS 15', 'High-performance laptop', 'Electronics', 1299.99, 45, 3),
('Python Book', 'Learn Python', 'Books', 49.99, 120, 3),
('Wireless Mouse', 'Ergonomic mouse', 'Electronics', 29.99, 200, 3);
GO

PRINT 'MS SQL Server initialization completed successfully!';
GO
