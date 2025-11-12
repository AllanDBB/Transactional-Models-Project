-- Script SQL para insertar datos de prueba en la BD transaccional
-- Ejecutar en SalesDB_MSSQL

USE SalesDB_MSSQL;
GO

-- Limpiar datos existentes (optional)
DELETE FROM sales_ms.OrdenDetalle;
DELETE FROM sales_ms.Orden;
DELETE FROM sales_ms.Producto;
DELETE FROM sales_ms.Cliente;
GO

-- Insertar clientes de prueba
INSERT INTO sales_ms.Cliente (Nombre, Email, Genero, Pais, FechaRegistro)
VALUES
    ('Juan Pérez', 'juan.perez@email.com', 'Masculino', 'Costa Rica', '2024-01-15'),
    ('María García', 'maria.garcia@email.com', 'Femenino', 'Costa Rica', '2024-01-20'),
    ('Carlos López', 'carlos.lopez@email.com', 'Masculino', 'Panama', '2024-02-10'),
    ('Ana Martínez', 'ana.martinez@email.com', 'Femenino', 'Colombia', '2024-02-15'),
    ('Roberto Silva', 'roberto.silva@email.com', 'Masculino', 'Costa Rica', '2024-03-01');
GO

-- Insertar productos de prueba
INSERT INTO sales_ms.Producto (SKU, Nombre, Categoria)
VALUES
    ('SKU-001', 'Laptop Dell XPS 13', 'Electrónica'),
    ('SKU-002', 'Mouse Logitech MX', 'Accesorios'),
    ('SKU-003', 'Teclado Mecánico RGB', 'Accesorios'),
    ('SKU-004', 'Monitor LG 27"', 'Electrónica'),
    ('SKU-005', 'Webcam Logitech 4K', 'Accesorios');
GO

-- Insertar órdenes de prueba
INSERT INTO sales_ms.Orden (ClienteId, Fecha, Canal, Moneda, Total)
VALUES
    (1, '2024-03-15 10:30:00', 'WEB', 'USD', 1500.00),
    (2, '2024-03-16 14:45:00', 'TIENDA', 'USD', 450.00),
    (1, '2024-03-17 09:15:00', 'APP', 'USD', 250.00),
    (3, '2024-03-18 11:20:00', 'WEB', 'USD', 2000.00),
    (2, '2024-03-19 15:30:00', 'WEB', 'USD', 800.00);
GO

-- Insertar detalles de órdenes
INSERT INTO sales_ms.OrdenDetalle (OrdenId, ProductoId, Cantidad, PrecioUnit, DescuentoPct)
VALUES
    -- Orden 1: Laptop + Mouse
    (1, 1, 1, 1200.00, 5.0),
    (1, 2, 1, 80.00, 0),
    -- Orden 2: Teclado + Webcam
    (2, 3, 1, 150.00, 10.0),
    (2, 5, 1, 300.00, 0),
    -- Orden 3: Mouse + Teclado
    (3, 2, 2, 80.00, 0),
    (3, 3, 1, 90.00, 0),
    -- Orden 4: Monitor
    (4, 4, 2, 800.00, 10.0),
    (4, 1, 1, 1200.00, 0),
    -- Orden 5: Varios
    (5, 2, 1, 80.00, 5.0),
    (5, 5, 2, 300.00, 0);
GO

-- Verificar datos insertados
SELECT COUNT(*) as 'Total Clientes' FROM sales_ms.Cliente;
SELECT COUNT(*) as 'Total Productos' FROM sales_ms.Producto;
SELECT COUNT(*) as 'Total Órdenes' FROM sales_ms.Orden;
SELECT COUNT(*) as 'Total Detalles' FROM sales_ms.OrdenDetalle;
GO

-- Ver datos de muestra
SELECT 'CLIENTES' as Tipo, * FROM sales_ms.Cliente;
SELECT 'PRODUCTOS' as Tipo, * FROM sales_ms.Producto;
SELECT 'ÓRDENES' as Tipo, * FROM sales_ms.Orden;
SELECT 'DETALLES' as Tipo, * FROM sales_ms.OrdenDetalle;
GO
