-- ============================================================================
-- 00-sp_init_schema.sql
-- Stored Procedure para inicializar schema de BD Transaccional MSSQL
-- ============================================================================

USE SalesDB_MSSQL;
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
        PRINT '========================================';
        PRINT 'INICIALIZANDO SCHEMA BD TRANSACCIONAL';
        PRINT '========================================';
        
        -- Verificar si el schema ya existe
        IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'sales_ms')
        BEGIN
            EXEC('CREATE SCHEMA sales_ms');
            PRINT '[OK] Schema sales_ms creado';
        END
        ELSE
        BEGIN
            PRINT '[INFO] Schema sales_ms ya existe';
        END
        
        -- Eliminar tablas si existen (en orden inverso por FKs)
        IF OBJECT_ID('sales_ms.OrdenDetalle', 'U') IS NOT NULL
        BEGIN
            DROP TABLE sales_ms.OrdenDetalle;
            PRINT '[OK] Tabla OrdenDetalle eliminada';
        END
        
        IF OBJECT_ID('sales_ms.Orden', 'U') IS NOT NULL
        BEGIN
            DROP TABLE sales_ms.Orden;
            PRINT '[OK] Tabla Orden eliminada';
        END
        
        IF OBJECT_ID('sales_ms.Producto', 'U') IS NOT NULL
        BEGIN
            DROP TABLE sales_ms.Producto;
            PRINT '[OK] Tabla Producto eliminada';
        END
        
        IF OBJECT_ID('sales_ms.Cliente', 'U') IS NOT NULL
        BEGIN
            DROP TABLE sales_ms.Cliente;
            PRINT '[OK] Tabla Cliente eliminada';
        END
        
        -- Crear tabla Cliente
        CREATE TABLE sales_ms.Cliente (
            ClienteId INT IDENTITY PRIMARY KEY,
            Nombre NVARCHAR(120) NOT NULL,
            Email NVARCHAR(150) UNIQUE,
            Genero NVARCHAR(12) CHECK (Genero IN ('Masculino','Femenino')),
            Pais NVARCHAR(60) NOT NULL,
            FechaRegistro DATE NOT NULL DEFAULT (GETDATE())
        );
        PRINT '[OK] Tabla Cliente creada';
        
        -- Crear tabla Producto
        CREATE TABLE sales_ms.Producto (
            ProductoId INT IDENTITY PRIMARY KEY,
            SKU NVARCHAR(40) UNIQUE NOT NULL,
            Nombre NVARCHAR(150) NOT NULL,
            Categoria NVARCHAR(80) NOT NULL
        );
        PRINT '[OK] Tabla Producto creada';
        
        -- Crear tabla Orden
        CREATE TABLE sales_ms.Orden (
            OrdenId INT IDENTITY PRIMARY KEY,
            ClienteId INT NOT NULL FOREIGN KEY REFERENCES sales_ms.Cliente(ClienteId),
            Fecha DATETIME2 NOT NULL DEFAULT (SYSDATETIME()),
            Canal NVARCHAR(20) NOT NULL CHECK (Canal IN ('WEB','TIENDA','APP')),
            Moneda CHAR(3) NOT NULL DEFAULT 'USD',
            Total DECIMAL(18,2) NOT NULL
        );
        PRINT '[OK] Tabla Orden creada';
        
        -- Crear tabla OrdenDetalle
        CREATE TABLE sales_ms.OrdenDetalle (
            OrdenDetalleId INT IDENTITY PRIMARY KEY,
            OrdenId INT NOT NULL FOREIGN KEY REFERENCES sales_ms.Orden(OrdenId),
            ProductoId INT NOT NULL FOREIGN KEY REFERENCES sales_ms.Producto(ProductoId),
            Cantidad INT NOT NULL CHECK (Cantidad > 0),
            PrecioUnit DECIMAL(18,2) NOT NULL,
            DescuentoPct DECIMAL(5,2) NULL
        );
        PRINT '[OK] Tabla OrdenDetalle creada';
        
        -- Crear índices
        CREATE INDEX IX_Orden_Fecha ON sales_ms.Orden(Fecha);
        CREATE INDEX IX_Detalle_Prod ON sales_ms.OrdenDetalle(ProductoId);
        PRINT '[OK] Índices creados';

        -- Recrear stored procedures del schema
        -- sp_limpiar_bd
        IF OBJECT_ID('sales_ms.sp_limpiar_bd', 'P') IS NOT NULL
            EXEC('DROP PROCEDURE sales_ms.sp_limpiar_bd');

        EXEC('
        CREATE PROCEDURE sales_ms.sp_limpiar_bd
        AS
        BEGIN
            SET NOCOUNT ON;
            BEGIN TRY
                BEGIN TRANSACTION;
                DELETE FROM sales_ms.OrdenDetalle;
                DELETE FROM sales_ms.Orden;
                DELETE FROM sales_ms.Producto;
                DELETE FROM sales_ms.Cliente;
                DBCC CHECKIDENT (''sales_ms.Cliente'', RESEED, 0);
                DBCC CHECKIDENT (''sales_ms.Producto'', RESEED, 0);
                DBCC CHECKIDENT (''sales_ms.Orden'', RESEED, 0);
                DBCC CHECKIDENT (''sales_ms.OrdenDetalle'', RESEED, 0);
                COMMIT;
                PRINT ''[OK] Base de datos limpiada exitosamente'';
            END TRY
            BEGIN CATCH
                ROLLBACK;
                THROW;
            END CATCH
        END
        ');
        PRINT '[OK] Stored procedure sales_ms.sp_limpiar_bd recreado';

        -- sp_generar_datos
        IF OBJECT_ID('sales_ms.sp_generar_datos', 'P') IS NOT NULL
            EXEC('DROP PROCEDURE sales_ms.sp_generar_datos');

        EXEC('
        CREATE PROCEDURE sales_ms.sp_generar_datos
        AS
        BEGIN
            SET NOCOUNT ON;
            DECLARE @i INT;
            BEGIN TRY
                BEGIN TRANSACTION;

                -- Generar Clientes (600)
                SET @i = 1;
                WHILE @i <= 600
                BEGIN
                    INSERT INTO sales_ms.Cliente (Nombre, Email, Genero, Pais, FechaRegistro)
                    VALUES (
                        ''Cliente'' + CAST(@i AS NVARCHAR),
                        ''cliente'' + CAST(@i AS NVARCHAR) + ''@mail.com'',
                        CASE WHEN @i % 2 = 0 THEN ''Masculino'' ELSE ''Femenino'' END,
                        CASE WHEN @i % 3 = 0 THEN ''Costa Rica'' WHEN @i % 3 = 1 THEN ''USA'' ELSE ''Mexico'' END,
                        DATEADD(DAY, -(@i % 365), GETDATE())
                    );
                    SET @i = @i + 1;
                END;

                -- Generar Productos (5000)
                SET @i = 1;
                WHILE @i <= 5000
                BEGIN
                    INSERT INTO sales_ms.Producto (SKU, Nombre, Categoria)
                    VALUES (
                        ''SKU'' + RIGHT(''00000'' + CAST(@i AS NVARCHAR), 5),
                        ''Producto '' + CAST(@i AS NVARCHAR),
                        CASE WHEN @i % 4 = 0 THEN ''Electronica'' WHEN @i % 4 = 1 THEN ''Ropa'' WHEN @i % 4 = 2 THEN ''Hogar'' ELSE ''Deportes'' END
                    );
                    SET @i = @i + 1;
                END;

                -- Generar Ordenes (5000)
                SET @i = 1;
                WHILE @i <= 5000
                BEGIN
                    INSERT INTO sales_ms.Orden (ClienteId, Fecha, Canal, Moneda, Total)
                    VALUES (
                        ((@i % 600) + 1),
                        DATEADD(MINUTE, -@i, GETDATE()),
                        CASE WHEN @i % 3 = 0 THEN ''WEB'' WHEN @i % 3 = 1 THEN ''TIENDA'' ELSE ''APP'' END,
                        ''USD'',
                        ROUND(RAND(CHECKSUM(NEWID())) * 1000 + 100, 2)
                    );
                    SET @i = @i + 1;
                END;

                -- Generar OrdenDetalles (17500 - promedio 3.5 por orden)
                SET @i = 1;
                DECLARE @ordenId INT = 1;
                DECLARE @detallesPorOrden INT;
                WHILE @ordenId <= 5000
                BEGIN
                    SET @detallesPorOrden = 3 + (@ordenId % 2);
                    DECLARE @j INT = 1;
                    WHILE @j <= @detallesPorOrden
                    BEGIN
                        INSERT INTO sales_ms.OrdenDetalle (OrdenId, ProductoId, Cantidad, PrecioUnit, DescuentoPct)
                        VALUES (
                            @ordenId,
                            ((@i % 5000) + 1),
                            ((@i % 5) + 1),
                            ROUND(RAND(CHECKSUM(NEWID())) * 200 + 10, 2),
                            CASE WHEN @i % 5 = 0 THEN 10.00 ELSE NULL END
                        );
                        SET @i = @i + 1;
                        SET @j = @j + 1;
                    END;
                    SET @ordenId = @ordenId + 1;
                END;

                COMMIT;
                PRINT ''[OK] Datos generados: 600 clientes, 5000 productos, 5000 ordenes, 17500 detalles'';
            END TRY
            BEGIN CATCH
                ROLLBACK;
                THROW;
            END CATCH
        END
        ');
        PRINT '[OK] Stored procedure sales_ms.sp_generar_datos recreado';

        PRINT '';
        PRINT '========================================';
        PRINT 'SCHEMA INICIALIZADO EXITOSAMENTE';
        PRINT '========================================';
        PRINT 'Tablas creadas:';
        PRINT '  - sales_ms.Cliente';
        PRINT '  - sales_ms.Producto';
        PRINT '  - sales_ms.Orden';
        PRINT '  - sales_ms.OrdenDetalle';
        PRINT 'Stored Procedures recreados:';
        PRINT '  - sales_ms.sp_limpiar_bd';
        PRINT '  - sales_ms.sp_generar_datos';
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

PRINT '[OK] Stored Procedure dbo.sp_init_schema creado exitosamente';
GO
