-- ============================================================================
-- 02-insert_test_data.sql
-- Insertar datos de prueba en BD Transaccional MSSQL
-- ============================================================================
-- - Clientes: 600
-- - Productos: 5000
-- - Órdenes: 5000
-- - Detalles: ~15000-25000 (2-5 items por orden)
-- - Fechas: 2024-01-01 a 2025-11-15
-- - Monedas: USD (todos, BD MSSQL es en USD)
-- - Canales: WEB, TIENDA, APP
--
-- ¿Qué son los DETALLES?
-- Cada ORDEN puede tener múltiples productos (líneas de detalle).
-- Ejemplo: Orden #1 tiene 3 detalles:
--   - Detalle 1: 2 Laptops × $1200 = $2400
--   - Detalle 2: 1 Mouse × $25 = $25
--   - Detalle 3: 3 Teclados × $80 = $240
--   Total Orden: $2665
-- ============================================================================

USE SalesDB_MSSQL;
GO

-- Deshabilitar mensajes de filas afectadas
SET NOCOUNT ON;
GO


-- Limpiar datos existentes (mantener estructura)
DELETE FROM sales_ms.OrdenDetalle;
DELETE FROM sales_ms.Orden;
DELETE FROM sales_ms.Producto;
DELETE FROM sales_ms.Cliente;

-- Resetear identidades
DBCC CHECKIDENT ('sales_ms.Cliente', RESEED, 0) WITH NO_INFOMSGS;
DBCC CHECKIDENT ('sales_ms.Producto', RESEED, 0) WITH NO_INFOMSGS;
DBCC CHECKIDENT ('sales_ms.Orden', RESEED, 0) WITH NO_INFOMSGS;
DBCC CHECKIDENT ('sales_ms.OrdenDetalle', RESEED, 0) WITH NO_INFOMSGS;


GO

-- Tabla auxiliar con nombres y países
DECLARE @Clientes TABLE (
    Nombre NVARCHAR(120),
    Pais NVARCHAR(60)
);

-- Insertar 600 clientes con variedad de nombres y países
INSERT INTO @Clientes VALUES 
('Juan García', 'Costa Rica'), ('María López', 'Costa Rica'), ('Carlos Rodríguez', 'Costa Rica'), 
('Ana Martínez', 'Costa Rica'), ('Pedro Sánchez', 'Costa Rica'), ('Laura Hernández', 'Costa Rica'),
('Miguel Flores', 'Costa Rica'), ('Isabel Mora', 'Costa Rica'), ('Diego Ramírez', 'Costa Rica'),
('Elena Vega', 'Costa Rica'), ('Antonio Díaz', 'Costa Rica'), ('Rosa Jiménez', 'Costa Rica'),
('Fernando Acosta', 'Costa Rica'), ('Sofía Campos', 'Costa Rica'), ('Raúl Ortiz', 'Costa Rica'),
('Gabriela Brenes', 'Costa Rica'), ('Andrés Castro', 'Costa Rica'), ('Patricia Solís', 'Costa Rica'),
('Ricardo Medina', 'Costa Rica'), ('Cristina Arias', 'Costa Rica'), ('Julio Delgado', 'Costa Rica'),
('Sandra Vargas', 'Costa Rica'), ('Hector Zamora', 'Costa Rica'), ('Verónica Segura', 'Costa Rica'),
('Eduardo Navarro', 'Costa Rica'), ('Margarita Rojas', 'Costa Rica'), ('Sergio Quirós', 'Costa Rica'),
('Beatriz Reyes', 'Costa Rica'), ('Javier Montoya', 'Costa Rica'), ('Dolores Aguilar', 'Costa Rica'),
('Samuel Vindas', 'Costa Rica'), ('Catalina Chaves', 'Costa Rica'), ('Vicente Gómez', 'Costa Rica'),
('Marta Durán', 'Costa Rica'), ('Bonifacio Araya', 'Costa Rica'), ('Lorena Espinoza', 'Costa Rica'),
('Octavio Paniagua', 'Costa Rica'), ('Valentina Soto', 'Costa Rica'), ('Gustavo Benavides', 'Costa Rica'),
('Adriana Salazar', 'Costa Rica'), ('Leopoldo Cordero', 'Costa Rica'), ('Pilar González', 'Costa Rica');

-- Generar 600 clientes usando números y combinaciones
;WITH NumberSequence AS (
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS Num
    FROM (
        SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
        UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
    ) t1,
    (SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
     UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10) t2,
    (SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
     UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10) t3,
    (SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
     UNION ALL SELECT 6) t4
)
INSERT INTO sales_ms.Cliente (Nombre, Email, Genero, Pais, FechaRegistro)
SELECT 
    'Cliente_' + FORMAT(Num, '000'),
    'cliente' + FORMAT(Num, '000') + '@example.com',
    CASE WHEN Num % 2 = 0 THEN 'Masculino' ELSE 'Femenino' END,
    CASE WHEN Num % 3 = 0 THEN 'Panamá' WHEN Num % 3 = 1 THEN 'Nicaragua' ELSE 'Costa Rica' END,
    DATEADD(DAY, -(ABS(CHECKSUM(Num)) % 365), CAST(GETDATE() AS DATE))
FROM NumberSequence
WHERE Num <= 600;


GO

-- Insertar 5000 productos con categorías variadas
;WITH ProductoNumbers AS (
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS Num
    FROM (
        SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
        UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
    ) t1,
    (SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
     UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10) t2,
    (SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
     UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10) t3,
    (SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
     UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10) t4,
    (SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5) t5
)
INSERT INTO sales_ms.Producto (SKU, Nombre, Categoria)
SELECT 
    'SKU-' + FORMAT(Num, '00000'),
    CASE 
        WHEN Num % 5 = 0 THEN 'Laptop Pro ' + CAST(Num AS NVARCHAR(10))
        WHEN Num % 5 = 1 THEN 'Monitor 27" ' + CAST(Num AS NVARCHAR(10))
        WHEN Num % 5 = 2 THEN 'Teclado Mecánico ' + CAST(Num AS NVARCHAR(10))
        WHEN Num % 5 = 3 THEN 'Mouse Inalámbrico ' + CAST(Num AS NVARCHAR(10))
        ELSE 'Webcam HD ' + CAST(Num AS NVARCHAR(10))
    END,
    CASE 
        WHEN Num % 5 = 0 THEN 'Computadoras'
        WHEN Num % 5 = 1 THEN 'Periféricos de Visualización'
        WHEN Num % 5 = 2 THEN 'Periféricos de Entrada'
        WHEN Num % 5 = 3 THEN 'Accesorios'
        ELSE 'Accesorios'
    END
FROM ProductoNumbers
WHERE Num <= 5000;

GO

-- Variables para control
DECLARE @OrdenActual INT = 1;
DECLARE @MaxOrdenes INT = 5000;
DECLARE @ClienteId INT;
DECLARE @ProductoId INT;
DECLARE @Cantidad INT;
DECLARE @PrecioUnit DECIMAL(18,2);
DECLARE @Total DECIMAL(18,2);
DECLARE @Fecha DATETIME2;
DECLARE @Canal NVARCHAR(20);
DECLARE @DetallesCount INT = 0;
DECLARE @MaxClientes INT;
DECLARE @MaxProductos INT;
DECLARE @DetallesPorOrden INT;

-- Obtener máximo de clientes y productos
SELECT @MaxClientes = MAX(ClienteId) FROM sales_ms.Cliente;
SELECT @MaxProductos = MAX(ProductoId) FROM sales_ms.Producto;

-- Crear tabla temporal para órdenes
DECLARE @OrdenesTmp TABLE (
    ClienteId INT,
    Fecha DATETIME2,
    Canal NVARCHAR(20),
    Total DECIMAL(18,2)
);

-- Generar 5000 órdenes (10 × 10 × 10 × 5 = 5000)
;WITH OrderNumbers AS (
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS Num
    FROM (
        SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
        UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
    ) t1,
    (SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
     UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10) t2,
    (SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
     UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10) t3,
    (SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5) t4
)
INSERT INTO @OrdenesTmp
SELECT 
    (Num % @MaxClientes) + 1 AS ClienteId,
    DATEADD(DAY, -(ABS(CHECKSUM(Num)) % 680), CAST(GETDATE() AS DATE)) AS Fecha,
    CASE WHEN (Num % 3) = 0 THEN 'WEB' WHEN (Num % 3) = 1 THEN 'TIENDA' ELSE 'APP' END AS Canal,
    CAST(ABS(CHECKSUM(Num)) % 5000 * 0.01 AS DECIMAL(18,2)) + 10 AS Total
FROM OrderNumbers
WHERE Num <= 5000;

-- Insertar órdenes
INSERT INTO sales_ms.Orden (ClienteId, Fecha, Canal, Moneda, Total)
SELECT ClienteId, Fecha, Canal, 'USD', Total
FROM @OrdenesTmp;

-- Insertar detalles de órdenes (2-5 items por orden)
DECLARE @OrdenId INT = 1;
DECLARE @MaxOrdenId INT;

SELECT @MaxOrdenId = MAX(OrdenId) FROM sales_ms.Orden;

WHILE @OrdenId <= @MaxOrdenId
BEGIN
    -- Cantidad aleatoria de detalles (2-5 items por orden)
    SET @DetallesPorOrden = (ABS(CHECKSUM(@OrdenId)) % 4) + 2;
    
    DECLARE @DetailNum INT = 1;
    DECLARE @TotalOrden DECIMAL(18,2) = 0;
    
    WHILE @DetailNum <= @DetallesPorOrden
    BEGIN
        SET @ProductoId = (ABS(CHECKSUM(@OrdenId + @DetailNum)) % @MaxProductos) + 1;
        SET @Cantidad = (ABS(CHECKSUM(@OrdenId * @DetailNum)) % 5) + 1;
        SET @PrecioUnit = CAST((ABS(CHECKSUM(@ProductoId)) % 20000) * 0.01 + 5 AS DECIMAL(18,2));
        
        INSERT INTO sales_ms.OrdenDetalle (OrdenId, ProductoId, Cantidad, PrecioUnit, DescuentoPct)
        VALUES (@OrdenId, @ProductoId, @Cantidad, @PrecioUnit, CASE WHEN @DetailNum % 5 = 0 THEN 10 ELSE NULL END);
        
        SET @TotalOrden = @TotalOrden + (@Cantidad * @PrecioUnit);
        SET @DetallesCount = @DetallesCount + 1;
        SET @DetailNum = @DetailNum + 1;
    END;
    
    -- Actualizar total de la orden
    UPDATE sales_ms.Orden 
    SET Total = @TotalOrden 
    WHERE OrdenId = @OrdenId;
    
    SET @OrdenId = @OrdenId + 1;
END;

GO

PRINT '';
PRINT '========================================';
PRINT 'RESUMEN FINAL';
PRINT '========================================';

DECLARE @TotalClientes INT, @TotalProductos INT, @TotalOrdenes INT, @TotalDetalles INT;
SELECT @TotalClientes = COUNT(*) FROM sales_ms.Cliente;
SELECT @TotalProductos = COUNT(*) FROM sales_ms.Producto;
SELECT @TotalOrdenes = COUNT(*) FROM sales_ms.Orden;
SELECT @TotalDetalles = COUNT(*) FROM sales_ms.OrdenDetalle;

PRINT 'Clientes:  ' + CAST(@TotalClientes AS NVARCHAR(10));
PRINT 'Productos: ' + CAST(@TotalProductos AS NVARCHAR(10));
PRINT 'Órdenes:   ' + CAST(@TotalOrdenes AS NVARCHAR(10));
PRINT 'Detalles:  ' + CAST(@TotalDetalles AS NVARCHAR(10));
PRINT '========================================';

SET NOCOUNT OFF;
