-- ============================================================================
-- 04-sp_generar_datos.sql
-- Stored Procedure para generar datos de prueba en BD Transaccional MSSQL
-- ============================================================================

USE SalesDB_MSSQL;
GO

-- Eliminar procedimiento si existe
IF OBJECT_ID('sales_ms.sp_generar_datos', 'P') IS NOT NULL
    DROP PROCEDURE sales_ms.sp_generar_datos;
GO

-- Crear procedimiento
CREATE PROCEDURE sales_ms.sp_generar_datos
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        -- Tabla con nombres reales
        DECLARE @NombresBase TABLE (
            RowNum INT IDENTITY(1,1),
            Nombre NVARCHAR(120),
            Genero NVARCHAR(12)
        );

        INSERT INTO @NombresBase (Nombre, Genero) VALUES 
        ('Juan García', 'Masculino'), ('María López', 'Femenino'), ('Carlos Rodríguez', 'Masculino'), 
        ('Ana Martínez', 'Femenino'), ('Pedro Sánchez', 'Masculino'), ('Laura Hernández', 'Femenino'),
        ('Miguel Flores', 'Masculino'), ('Isabel Mora', 'Femenino'), ('Diego Ramírez', 'Masculino'),
        ('Elena Vega', 'Femenino'), ('Antonio Díaz', 'Masculino'), ('Rosa Jiménez', 'Femenino'),
        ('Fernando Acosta', 'Masculino'), ('Sofía Campos', 'Femenino'), ('Raúl Ortiz', 'Masculino'),
        ('Gabriela Brenes', 'Femenino'), ('Andrés Castro', 'Masculino'), ('Patricia Solís', 'Femenino'),
        ('Ricardo Medina', 'Masculino'), ('Cristina Arias', 'Femenino'), ('Julio Delgado', 'Masculino'),
        ('Sandra Vargas', 'Femenino'), ('Hector Zamora', 'Masculino'), ('Verónica Segura', 'Femenino'),
        ('Eduardo Navarro', 'Masculino'), ('Margarita Rojas', 'Femenino'), ('Sergio Quirós', 'Masculino'),
        ('Beatriz Reyes', 'Femenino'), ('Javier Montoya', 'Masculino'), ('Dolores Aguilar', 'Femenino'),
        ('Samuel Vindas', 'Masculino'), ('Catalina Chaves', 'Femenino'), ('Vicente Gómez', 'Masculino'),
        ('Marta Durán', 'Femenino'), ('Bonifacio Araya', 'Masculino'), ('Lorena Espinoza', 'Femenino'),
        ('Octavio Paniagua', 'Masculino'), ('Valentina Soto', 'Femenino'), ('Gustavo Benavides', 'Masculino'),
        ('Adriana Salazar', 'Femenino'), ('Leopoldo Cordero', 'Masculino'), ('Pilar González', 'Femenino');

        -- Generar 600 clientes reutilizando los 42 nombres (42 × 15 = 630, usamos 600)
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
            N.Nombre,
            LOWER(REPLACE(N.Nombre, ' ', '.')) + CAST(Num AS NVARCHAR(10)) + '@example.com',
            N.Genero,
            CASE WHEN Num % 3 = 0 THEN 'Panamá' WHEN Num % 3 = 1 THEN 'Nicaragua' ELSE 'Costa Rica' END,
            DATEADD(DAY, -(ABS(CHECKSUM(Num)) % 365), CAST(GETDATE() AS DATE))
        FROM NumberSequence
        CROSS APPLY (
            SELECT Nombre, Genero 
            FROM @NombresBase 
            WHERE RowNum = ((Num - 1) % 42) + 1
        ) N
        WHERE Num <= 600;

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
            FORMAT(Num, '00000'),
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

        -- Resumen final
        PRINT '';
        PRINT '========================================';
        PRINT 'DATOS GENERADOS EXITOSAMENTE';
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
GRANT EXECUTE ON sales_ms.sp_generar_datos TO public;
GO

PRINT '[OK] Stored Procedure sales_ms.sp_generar_datos creado exitosamente';
GO
